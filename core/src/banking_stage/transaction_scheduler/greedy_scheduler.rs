use {
    super::{
        in_flight_tracker::InFlightTracker,
        scheduler_error::SchedulerError,
        thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadId, ThreadSet},
        transaction_state::SanitizedTransactionTTL,
        transaction_state_container::TransactionStateContainer,
    },
    crate::banking_stage::{
        consumer::TARGET_NUM_TRANSACTIONS_PER_BATCH,
        read_write_account_set::ReadWriteAccountSet,
        scheduler_messages::{
            ConsumeWork, FinishedConsumeWork, MaxAge, TransactionBatchId, TransactionId,
        },
        transaction_scheduler::transaction_state::TransactionState,
    },
    crossbeam_channel::{Receiver, Sender, TryRecvError},
    itertools::izip,
    solana_sdk::{saturating_add_assign, transaction::SanitizedTransaction},
};

pub(crate) struct GreedyScheduler {
    in_flight_tracker: InFlightTracker,
    working_account_set: ReadWriteAccountSet,
    account_locks: ThreadAwareAccountLocks,
    consume_work_senders: Vec<Sender<ConsumeWork>>,
    finished_consume_work_receiver: Receiver<FinishedConsumeWork>,
}

impl GreedyScheduler {
    pub(crate) fn new(
        consume_work_senders: Vec<Sender<ConsumeWork>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork>,
    ) -> Self {
        let num_threads = consume_work_senders.len();
        Self {
            in_flight_tracker: InFlightTracker::new(num_threads),
            working_account_set: ReadWriteAccountSet::default(),
            account_locks: ThreadAwareAccountLocks::new(num_threads),
            consume_work_senders,
            finished_consume_work_receiver,
        }
    }

    pub(crate) fn schedule(
        &mut self,
        container: &mut TransactionStateContainer,
        _pre_graph_filter: impl Fn(&[&SanitizedTransaction], &mut [bool]),
        pre_lock_filter: impl Fn(&SanitizedTransaction) -> bool,
    ) -> Result<SchedulingSummary, SchedulerError> {
        let num_threads = self.consume_work_senders.len();
        let max_cu_per_thread = 48_000_000 / num_threads as u64;

        let mut schedulable_threads = ThreadSet::any(num_threads);
        for thread_id in 0..num_threads {
            if self.in_flight_tracker.cus_in_flight_per_thread()[thread_id] >= max_cu_per_thread {
                schedulable_threads.remove(thread_id);
            }
        }
        if schedulable_threads.is_empty() {
            return Ok(SchedulingSummary {
                num_scheduled: 0,
                num_unschedulable: 0,
                num_filtered_out: 0,
                filter_time_us: 0,
            });
        }

        // Track metrics on filter.
        let num_filtered_out: usize = 0;
        let total_filter_time_us: u64 = 0;
        let mut num_scheduled: usize = 0;
        let mut num_sent: usize = 0;
        let mut hit_unschedulable = false;

        let mut batches = Batches::new(num_threads);
        'outer: while !hit_unschedulable
            && num_scheduled < 100_000
            && !schedulable_threads.is_empty()
        {
            loop {
                let Some(id) = container.pop() else {
                    break 'outer;
                };

                // Should always be in the container, during initial testing phase panic.
                // Later, we can replace with a continue in case this does happen.
                let Some(transaction_state) = container.get_mut_transaction_state(&id.id) else {
                    panic!("transaction state must exist")
                };

                // If there is a conflict with any of the transactions in the current batches,
                // we should immediately send out the batches, so this transaction may be scheduled.
                if !self
                    .working_account_set
                    .check_locks(transaction_state.transaction_ttl().transaction.message())
                {
                    num_sent += self.send_batches(&mut batches)?;
                    self.working_account_set.clear();
                }

                // Now check if the transaction can be actually be scheduled.
                match try_schedule_transaction(
                    transaction_state,
                    &pre_lock_filter,
                    &mut self.account_locks,
                    schedulable_threads,
                    |thread_set| {
                        Self::select_thread(
                            thread_set,
                            &batches.total_cus,
                            self.in_flight_tracker.cus_in_flight_per_thread(),
                            &batches.transactions,
                            self.in_flight_tracker.num_in_flight_per_thread(),
                        )
                    },
                ) {
                    Err(TransactionSchedulingError::Filtered) => {
                        container.remove_by_id(&id.id);
                    }
                    Err(TransactionSchedulingError::UnschedulableConflicts) => {
                        // Push popped ID back into the queue.
                        container.push_id_into_queue(id);
                        hit_unschedulable = true;
                        break;
                    }
                    Ok(TransactionSchedulingInfo {
                        thread_id,
                        transaction,
                        max_age,
                        cost,
                    }) => {
                        assert!(self.working_account_set.take_locks(transaction.message()));
                        saturating_add_assign!(num_scheduled, 1);
                        batches.transactions[thread_id].push(transaction);
                        batches.ids[thread_id].push(id.id);
                        batches.max_ages[thread_id].push(max_age);
                        saturating_add_assign!(batches.total_cus[thread_id], cost);

                        // If target batch size is reached, send only this batch.
                        if batches.ids[thread_id].len() >= TARGET_NUM_TRANSACTIONS_PER_BATCH {
                            saturating_add_assign!(
                                num_sent,
                                self.send_batch(&mut batches, thread_id)?
                            );
                        }

                        // if the thread is at max_cu_per_thread, remove it from the schedulable threads
                        // if there are no more schedulable threads, stop scheduling.
                        if self.in_flight_tracker.cus_in_flight_per_thread()[thread_id]
                            + batches.total_cus[thread_id]
                            >= max_cu_per_thread
                        {
                            schedulable_threads.remove(thread_id);
                            if schedulable_threads.is_empty() {
                                break;
                            }
                        }

                        if num_scheduled >= 100_000 {
                            break;
                        }
                    }
                }
            }
        }

        self.working_account_set.clear();
        self.send_batches(&mut batches)?;

        Ok(SchedulingSummary {
            num_scheduled,
            num_unschedulable: usize::from(hit_unschedulable),
            num_filtered_out,
            filter_time_us: total_filter_time_us,
        })
    }

    /// Receive completed batches of transactions without blocking.
    /// Returns (num_transactions, num_retryable_transactions) on success.
    pub fn receive_completed(
        &mut self,
        container: &mut TransactionStateContainer,
    ) -> Result<(usize, usize), SchedulerError> {
        let mut total_num_transactions: usize = 0;
        let mut total_num_retryable: usize = 0;
        loop {
            let (num_transactions, num_retryable) = self.try_receive_completed(container)?;
            if num_transactions == 0 {
                break;
            }
            saturating_add_assign!(total_num_transactions, num_transactions);
            saturating_add_assign!(total_num_retryable, num_retryable);
        }
        Ok((total_num_transactions, total_num_retryable))
    }

    /// Receive completed batches of transactions.
    /// Returns `Ok((num_transactions, num_retryable))` if a batch was received, `Ok((0, 0))` if no batch was received.
    fn try_receive_completed(
        &mut self,
        container: &mut TransactionStateContainer,
    ) -> Result<(usize, usize), SchedulerError> {
        match self.finished_consume_work_receiver.try_recv() {
            Ok(FinishedConsumeWork {
                work:
                    ConsumeWork {
                        batch_id,
                        ids,
                        transactions,
                        max_ages,
                    },
                retryable_indexes,
            }) => {
                let num_transactions = ids.len();
                let num_retryable = retryable_indexes.len();

                // Free the locks
                self.complete_batch(batch_id, &transactions);

                // Retryable transactions should be inserted back into the container
                let mut retryable_iter = retryable_indexes.into_iter().peekable();
                for (index, (id, transaction, max_age)) in
                    izip!(ids, transactions, max_ages).enumerate()
                {
                    if let Some(retryable_index) = retryable_iter.peek() {
                        if *retryable_index == index {
                            container.retry_transaction(
                                id,
                                SanitizedTransactionTTL {
                                    transaction,
                                    max_age,
                                },
                            );
                            retryable_iter.next();
                            continue;
                        }
                    }
                    container.remove_by_id(&id);
                }

                Ok((num_transactions, num_retryable))
            }
            Err(TryRecvError::Empty) => Ok((0, 0)),
            Err(TryRecvError::Disconnected) => Err(SchedulerError::DisconnectedRecvChannel(
                "finished consume work",
            )),
        }
    }

    /// Mark a given `TransactionBatchId` as completed.
    /// This will update the internal tracking, including account locks.
    fn complete_batch(
        &mut self,
        batch_id: TransactionBatchId,
        transactions: &[SanitizedTransaction],
    ) {
        let thread_id = self.in_flight_tracker.complete_batch(batch_id);
        for transaction in transactions {
            let message = transaction.message();
            let account_keys = message.account_keys();
            let write_account_locks = account_keys
                .iter()
                .enumerate()
                .filter_map(|(index, key)| message.is_writable(index).then_some(key));
            let read_account_locks = account_keys
                .iter()
                .enumerate()
                .filter_map(|(index, key)| (!message.is_writable(index)).then_some(key));
            self.account_locks
                .unlock_accounts(write_account_locks, read_account_locks, thread_id);
        }
    }

    /// Send all batches of transactions to the worker threads.
    /// Returns the number of transactions sent.
    fn send_batches(&mut self, batches: &mut Batches) -> Result<usize, SchedulerError> {
        (0..self.consume_work_senders.len())
            .map(|thread_index| self.send_batch(batches, thread_index))
            .sum()
    }

    /// Send a batch of transactions to the given thread's `ConsumeWork` channel.
    /// Returns the number of transactions sent.
    fn send_batch(
        &mut self,
        batches: &mut Batches,
        thread_index: usize,
    ) -> Result<usize, SchedulerError> {
        if batches.ids[thread_index].is_empty() {
            return Ok(0);
        }

        let (ids, transactions, max_ages, total_cus) = batches.take_batch(thread_index);

        let batch_id = self
            .in_flight_tracker
            .track_batch(ids.len(), total_cus, thread_index);

        let num_scheduled = ids.len();
        let work = ConsumeWork {
            batch_id,
            ids,
            transactions,
            max_ages,
        };
        self.consume_work_senders[thread_index]
            .send(work)
            .map_err(|_| SchedulerError::DisconnectedSendChannel("consume work sender"))?;

        Ok(num_scheduled)
    }

    /// Given the schedulable `thread_set`, select the thread with the least amount
    /// of work queued up.
    /// Currently, "work" is just defined as the number of transactions.
    ///
    /// If the `chain_thread` is available, this thread will be selected, regardless of
    /// load-balancing.
    ///
    /// Panics if the `thread_set` is empty. This should never happen, see comment
    /// on `ThreadAwareAccountLocks::try_lock_accounts`.
    fn select_thread(
        thread_set: ThreadSet,
        batch_cus_per_thread: &[u64],
        in_flight_cus_per_thread: &[u64],
        batches_per_thread: &[Vec<SanitizedTransaction>],
        in_flight_per_thread: &[usize],
    ) -> ThreadId {
        thread_set
            .contained_threads_iter()
            .map(|thread_id| {
                (
                    thread_id,
                    batch_cus_per_thread[thread_id] + in_flight_cus_per_thread[thread_id],
                    batches_per_thread[thread_id].len() + in_flight_per_thread[thread_id],
                )
            })
            .min_by(|a, b| a.1.cmp(&b.1).then_with(|| a.2.cmp(&b.2)))
            .map(|(thread_id, _, _)| thread_id)
            .unwrap()
    }
}

/// Metrics from scheduling transactions.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SchedulingSummary {
    /// Number of transactions scheduled.
    pub num_scheduled: usize,
    /// Number of transactions that were not scheduled due to conflicts.
    pub num_unschedulable: usize,
    /// Number of transactions that were dropped due to filter.
    pub num_filtered_out: usize,
    /// Time spent filtering transactions
    pub filter_time_us: u64,
}

struct Batches {
    ids: Vec<Vec<TransactionId>>,
    transactions: Vec<Vec<SanitizedTransaction>>,
    max_ages: Vec<Vec<MaxAge>>,
    total_cus: Vec<u64>,
}

impl Batches {
    fn new(num_threads: usize) -> Self {
        Self {
            ids: vec![Vec::with_capacity(TARGET_NUM_TRANSACTIONS_PER_BATCH); num_threads],
            transactions: vec![Vec::with_capacity(TARGET_NUM_TRANSACTIONS_PER_BATCH); num_threads],
            max_ages: vec![Vec::with_capacity(TARGET_NUM_TRANSACTIONS_PER_BATCH); num_threads],
            total_cus: vec![0; num_threads],
        }
    }

    fn take_batch(
        &mut self,
        thread_id: ThreadId,
    ) -> (
        Vec<TransactionId>,
        Vec<SanitizedTransaction>,
        Vec<MaxAge>,
        u64,
    ) {
        (
            core::mem::replace(
                &mut self.ids[thread_id],
                Vec::with_capacity(TARGET_NUM_TRANSACTIONS_PER_BATCH),
            ),
            core::mem::replace(
                &mut self.transactions[thread_id],
                Vec::with_capacity(TARGET_NUM_TRANSACTIONS_PER_BATCH),
            ),
            core::mem::replace(
                &mut self.max_ages[thread_id],
                Vec::with_capacity(TARGET_NUM_TRANSACTIONS_PER_BATCH),
            ),
            core::mem::replace(&mut self.total_cus[thread_id], 0),
        )
    }
}

/// A transaction has been scheduled to a thread.
struct TransactionSchedulingInfo {
    thread_id: ThreadId,
    transaction: SanitizedTransaction,
    max_age: MaxAge,
    cost: u64,
}

/// Error type for reasons a transaction could not be scheduled.
enum TransactionSchedulingError {
    /// Transaction was filtered out before locking.
    Filtered,
    /// Transaction cannot be scheduled due to conflicts, or
    /// higher priority conflicting transactions are unschedulable.
    UnschedulableConflicts,
}

fn try_schedule_transaction(
    transaction_state: &mut TransactionState,
    pre_lock_filter: impl Fn(&SanitizedTransaction) -> bool,
    account_locks: &mut ThreadAwareAccountLocks,
    schedulable_threads: ThreadSet,
    thread_selector: impl Fn(ThreadSet) -> ThreadId,
) -> Result<TransactionSchedulingInfo, TransactionSchedulingError> {
    let transaction = &transaction_state.transaction_ttl().transaction;
    if !pre_lock_filter(transaction) {
        return Err(TransactionSchedulingError::Filtered);
    }

    // Schedule the transaction if it can be.
    let account_keys = transaction.message().account_keys();
    let write_account_locks = account_keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| transaction.message().is_writable(index).then_some(key));
    let read_account_locks = account_keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| (!transaction.message().is_writable(index)).then_some(key));

    let Some(thread_id) = account_locks.try_lock_accounts(
        write_account_locks,
        read_account_locks,
        schedulable_threads,
        thread_selector,
    ) else {
        return Err(TransactionSchedulingError::UnschedulableConflicts);
    };

    let sanitized_transaction_ttl = transaction_state.transition_to_pending();
    let cost = transaction_state.cost();

    Ok(TransactionSchedulingInfo {
        thread_id,
        transaction: sanitized_transaction_ttl.transaction,
        max_age: sanitized_transaction_ttl.max_age,
        cost,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_stage::{
            consumer::TARGET_NUM_TRANSACTIONS_PER_BATCH,
            immutable_deserialized_packet::ImmutableDeserializedPacket,
        },
        crossbeam_channel::{unbounded, Receiver},
        itertools::Itertools,
        solana_sdk::{
            compute_budget::ComputeBudgetInstruction, hash::Hash, message::Message, packet::Packet,
            pubkey::Pubkey, signature::Keypair, signer::Signer, system_instruction,
            transaction::Transaction,
        },
        std::{borrow::Borrow, sync::Arc},
    };

    macro_rules! txid {
        ($value:expr) => {
            TransactionId::new($value)
        };
    }

    macro_rules! txids {
        ([$($element:expr),*]) => {
            vec![ $(txid!($element)),* ]
        };
    }

    fn create_test_frame(
        num_threads: usize,
    ) -> (
        GreedyScheduler,
        Vec<Receiver<ConsumeWork>>,
        Sender<FinishedConsumeWork>,
    ) {
        let (consume_work_senders, consume_work_receivers) =
            (0..num_threads).map(|_| unbounded()).unzip();
        let (finished_consume_work_sender, finished_consume_work_receiver) = unbounded();
        let scheduler = GreedyScheduler::new(consume_work_senders, finished_consume_work_receiver);
        (
            scheduler,
            consume_work_receivers,
            finished_consume_work_sender,
        )
    }

    fn prioritized_tranfers(
        from_keypair: &Keypair,
        to_pubkeys: impl IntoIterator<Item = impl Borrow<Pubkey>>,
        lamports: u64,
        priority: u64,
    ) -> SanitizedTransaction {
        let to_pubkeys_lamports = to_pubkeys
            .into_iter()
            .map(|pubkey| *pubkey.borrow())
            .zip(std::iter::repeat(lamports))
            .collect_vec();
        let mut ixs =
            system_instruction::transfer_many(&from_keypair.pubkey(), &to_pubkeys_lamports);
        let prioritization = ComputeBudgetInstruction::set_compute_unit_price(priority);
        ixs.push(prioritization);
        let message = Message::new(&ixs, Some(&from_keypair.pubkey()));
        let tx = Transaction::new(&[from_keypair], message, Hash::default());
        SanitizedTransaction::from_transaction_for_tests(tx)
    }

    fn create_container(
        tx_infos: impl IntoIterator<
            Item = (
                impl Borrow<Keypair>,
                impl IntoIterator<Item = impl Borrow<Pubkey>>,
                u64,
                u64,
            ),
        >,
    ) -> TransactionStateContainer {
        let mut container = TransactionStateContainer::with_capacity(10 * 1024);
        for (index, (from_keypair, to_pubkeys, lamports, compute_unit_price)) in
            tx_infos.into_iter().enumerate()
        {
            let id = TransactionId::new(index as u64);
            let transaction = prioritized_tranfers(
                from_keypair.borrow(),
                to_pubkeys,
                lamports,
                compute_unit_price,
            );
            let packet = Arc::new(
                ImmutableDeserializedPacket::new(
                    Packet::from_data(None, transaction.to_versioned_transaction()).unwrap(),
                )
                .unwrap(),
            );
            let transaction_ttl = SanitizedTransactionTTL {
                transaction,
                max_age: MaxAge::MAX,
            };
            const TEST_TRANSACTION_COST: u64 = 5000;
            container.insert_new_transaction(
                id,
                transaction_ttl,
                packet,
                compute_unit_price,
                TEST_TRANSACTION_COST,
            );
        }

        container
    }

    fn collect_work(
        receiver: &Receiver<ConsumeWork>,
    ) -> (Vec<ConsumeWork>, Vec<Vec<TransactionId>>) {
        receiver
            .try_iter()
            .map(|work| {
                let ids = work.ids.clone();
                (work, ids)
            })
            .unzip()
    }

    fn test_pre_graph_filter(_txs: &[&SanitizedTransaction], results: &mut [bool]) {
        results.fill(true);
    }

    fn test_pre_lock_filter(_tx: &SanitizedTransaction) -> bool {
        true
    }

    #[test]
    fn test_schedule_disconnected_channel() {
        let (mut scheduler, work_receivers, _finished_work_sender) = create_test_frame(1);
        let mut container = create_container([(&Keypair::new(), &[Pubkey::new_unique()], 1, 1)]);

        drop(work_receivers); // explicitly drop receivers
        assert_matches!(
            scheduler.schedule(&mut container, test_pre_graph_filter, test_pre_lock_filter),
            Err(SchedulerError::DisconnectedSendChannel(_))
        );
    }

    #[test]
    fn test_schedule_single_threaded_no_conflicts() {
        let (mut scheduler, work_receivers, _finished_work_sender) = create_test_frame(1);
        let mut container = create_container([
            (&Keypair::new(), &[Pubkey::new_unique()], 1, 1),
            (&Keypair::new(), &[Pubkey::new_unique()], 2, 2),
        ]);

        let scheduling_summary = scheduler
            .schedule(&mut container, test_pre_graph_filter, test_pre_lock_filter)
            .unwrap();
        assert_eq!(scheduling_summary.num_scheduled, 2);
        assert_eq!(scheduling_summary.num_unschedulable, 0);
        assert_eq!(collect_work(&work_receivers[0]).1, vec![txids!([1, 0])]);
    }

    #[test]
    fn test_schedule_single_threaded_conflict() {
        let (mut scheduler, work_receivers, _finished_work_sender) = create_test_frame(1);
        let pubkey = Pubkey::new_unique();
        let mut container = create_container([
            (&Keypair::new(), &[pubkey], 1, 1),
            (&Keypair::new(), &[pubkey], 1, 2),
        ]);

        let scheduling_summary = scheduler
            .schedule(&mut container, test_pre_graph_filter, test_pre_lock_filter)
            .unwrap();
        assert_eq!(scheduling_summary.num_scheduled, 2);
        assert_eq!(scheduling_summary.num_unschedulable, 0);
        assert_eq!(
            collect_work(&work_receivers[0]).1,
            vec![txids!([1]), txids!([0])]
        );
    }

    #[test]
    fn test_schedule_consume_single_threaded_multi_batch() {
        let (mut scheduler, work_receivers, _finished_work_sender) = create_test_frame(1);
        let mut container = create_container(
            (0..4 * TARGET_NUM_TRANSACTIONS_PER_BATCH)
                .map(|i| (Keypair::new(), [Pubkey::new_unique()], i as u64, 1)),
        );

        // expect 4 full batches to be scheduled
        let scheduling_summary = scheduler
            .schedule(&mut container, test_pre_graph_filter, test_pre_lock_filter)
            .unwrap();
        assert_eq!(
            scheduling_summary.num_scheduled,
            4 * TARGET_NUM_TRANSACTIONS_PER_BATCH
        );
        assert_eq!(scheduling_summary.num_unschedulable, 0);

        let thread0_work_counts: Vec<_> = work_receivers[0]
            .try_iter()
            .map(|work| work.ids.len())
            .collect();
        assert_eq!(thread0_work_counts, [TARGET_NUM_TRANSACTIONS_PER_BATCH; 4]);
    }

    #[test]
    fn test_schedule_simple_thread_selection() {
        let (mut scheduler, work_receivers, _finished_work_sender) = create_test_frame(2);
        let mut container =
            create_container((0..4).map(|i| (Keypair::new(), [Pubkey::new_unique()], 1, i)));

        let scheduling_summary = scheduler
            .schedule(&mut container, test_pre_graph_filter, test_pre_lock_filter)
            .unwrap();
        assert_eq!(scheduling_summary.num_scheduled, 4);
        assert_eq!(scheduling_summary.num_unschedulable, 0);
        assert_eq!(collect_work(&work_receivers[0]).1, [txids!([3, 1])]);
        assert_eq!(collect_work(&work_receivers[1]).1, [txids!([2, 0])]);
    }

    #[test]
    fn test_schedule_pre_lock_filter() {
        let (mut scheduler, work_receivers, _finished_work_sender) = create_test_frame(1);
        let pubkey = Pubkey::new_unique();
        let keypair = Keypair::new();
        let mut container = create_container([
            (&Keypair::new(), &[pubkey], 1, 1),
            (&keypair, &[pubkey], 1, 2),
            (&Keypair::new(), &[pubkey], 1, 3),
        ]);

        // 2nd transaction should be filtered out and dropped before locking.
        let pre_lock_filter =
            |tx: &SanitizedTransaction| tx.message().fee_payer() != &keypair.pubkey();
        let scheduling_summary = scheduler
            .schedule(&mut container, test_pre_graph_filter, pre_lock_filter)
            .unwrap();
        assert_eq!(scheduling_summary.num_scheduled, 2);
        assert_eq!(scheduling_summary.num_unschedulable, 0);
        assert_eq!(
            collect_work(&work_receivers[0]).1,
            vec![txids!([2]), txids!([0])]
        );
    }
}
