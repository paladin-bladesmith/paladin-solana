use {
    crate::{
        banking_stage::{
            decision_maker::BufferedPacketsDecision,
            unified_schedule::unified_state_container::StateContainer,
        },
        bundle_stage::bundle_packet_deserializer::{
            BundlePacketDeserializer, ReceiveBundleResults,
        },
        packet_bundle::PacketBundle,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_measure::{measure::Measure, measure_us},
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    solana_time_utils::timestamp,
    std::time::{Duration, Instant},
};

const DEFAULT_BATCH_BUNDLE_TIMEOUT: Duration = Duration::from_millis(10);

pub struct BundleReceiver {
    bundle_packet_deserializer: BundlePacketDeserializer,
}

impl BundleReceiver {
    pub fn new(
        bundle_packet_receiver: Receiver<Vec<PacketBundle>>,
        max_packets_per_bundle: Option<usize>,
    ) -> Self {
        Self {
            bundle_packet_deserializer: BundlePacketDeserializer::new(
                bundle_packet_receiver,
                max_packets_per_bundle,
            ),
        }
    }

    /// Receive incoming packets, push into unprocessed buffer with packet indexes
    pub fn receive_and_buffer_bundles<S, Tx>(
        &mut self,
        conatiner: &mut S,
        batch_bundle_results: &mut ReceiveBundleResults,
        batch_bundle_timer: &mut Option<Instant>,
        decision: &BufferedPacketsDecision,
    ) -> Result<(), RecvTimeoutError>
    where
        S: StateContainer<Tx>,
        Tx: TransactionWithMeta,
    {
        let (result, _recv_time_us) = measure_us!({
            let mut recv_and_buffer_measure = Measure::start("recv_and_buffer");

            // If timer has passed, buffer current bundles and reset timer
            if let Some(timer) = batch_bundle_timer {
                if timer.elapsed() >= DEFAULT_BATCH_BUNDLE_TIMEOUT {
                    // Take the batch, and reset to default
                    let batch_bundles = std::mem::take(batch_bundle_results);

                    // Buffer bundles
                    self.buffer_bundles(batch_bundles, conatiner, decision);

                    // Reset timer
                    *batch_bundle_timer = None;
                }
            }

            let recv_timeout = Self::get_receive_timeout(conatiner, batch_bundle_timer);

            self.bundle_packet_deserializer
                .receive_bundles(recv_timeout, conatiner.buffer_size())
                // Add to batch if Ok, otherwise we keep the Err
                .map(|receive_bundle_results| {
                    // If batch is empty, start timer because its the first bundle we receive
                    if batch_bundle_results.is_empty() {
                        *batch_bundle_timer = Some(Instant::now());
                    }

                    batch_bundle_results.extend(receive_bundle_results);

                    recv_and_buffer_measure.stop();
                    // bundle_stage_metrics.increment_receive_and_buffer_bundles_elapsed_us(
                    //     recv_and_buffer_measure.as_us(),
                    // );
                })
        });

        // bundle_stage_leader_metrics
        //     .leader_slot_metrics_tracker()
        //     .increment_receive_and_buffer_packets_us(recv_time_us);

        result
    }

    fn get_receive_timeout<S, Tx>(
        bundle_storage: &S,
        batch_bundle_timer: &Option<Instant>,
    ) -> Duration
    where
        S: StateContainer<Tx>,
        Tx: TransactionWithMeta,
    {
        // Gossip thread will almost always not wait because the transaction storage will most likely not be empty
        if !bundle_storage.is_empty() {
            // If there are buffered packets, run the equivalent of try_recv to try reading more
            // packets. This prevents starving BankingStage::consume_buffered_packets due to
            // buffered_packet_batches containing transactions that exceed the cost model for
            // the current bank.
            Duration::from_millis(0)
        } else if let Some(timer) = batch_bundle_timer {
            DEFAULT_BATCH_BUNDLE_TIMEOUT.saturating_sub(timer.elapsed())
        } else {
            // BundleStage should pick up a working_bank as fast as possible
            Duration::from_millis(100)
        }
    }

    fn buffer_bundles<S>(
        &self,
        ReceiveBundleResults {
            deserialized_bundles,
            num_dropped_bundles: _,
        }: ReceiveBundleResults,
        _conatiner: &mut S,
        _decision: &BufferedPacketsDecision,
    ) {
        let bundle_count = deserialized_bundles.len();
        let packet_count: usize = deserialized_bundles.iter().map(|b| b.len()).sum();

        // bundle_stage_stats.increment_num_bundles_received(bundle_count as u64);
        // bundle_stage_stats.increment_num_packets_received(packet_count as u64);
        // bundle_stage_stats.increment_num_bundles_dropped(num_dropped_bundles.0 as u64);

        debug!(
            "@{:?} bundles: {} txs: {}",
            timestamp(),
            bundle_count,
            packet_count,
        );
    }

    // fn push_unprocessed(
    //     bundle_storage: &mut BundleStorage,
    //     deserialized_bundles: Vec<ImmutableDeserializedBundle>,
    //     bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    //     bundle_stage_stats: &mut BundleStageLoopMetrics,
    // ) {
    //     if !deserialized_bundles.is_empty() {
    //         // bundles get pushed onto the back of the unprocessed bundle queue
    //         let insert_bundles_summary =
    //             bundle_storage.insert_unprocessed_bundles(deserialized_bundles);

    //         bundle_stage_stats.increment_newly_buffered_bundles_count(
    //             insert_bundles_summary.num_bundles_inserted as u64,
    //         );
    //         bundle_stage_stats
    //             .increment_num_bundles_dropped(insert_bundles_summary.num_bundles_dropped as u64);

    //         bundle_stage_leader_metrics
    //             .leader_slot_metrics_tracker()
    //             .increment_newly_buffered_packets_count(
    //                 insert_bundles_summary.num_packets_inserted as u64,
    //             );
    //     }
    // }
}

/// This tests functionality of BundlePacketReceiver and the internals of BundleStorage because
/// they're tightly intertwined
/// TODO: Update tests for unified scheduler
#[cfg(any())] // Disabled - tests need to be updated for unified scheduler
#[allow(dead_code, unused_variables, unused_imports)]
mod tests {
    use {
        crate::{
            bundle_stage::{
                bundle_packet_deserializer::ReceiveBundleResults,
                bundle_packet_receiver::{BundleReceiver, DEFAULT_BATCH_BUNDLE_TIMEOUT},
                bundle_stage_leader_metrics::BundleStageLeaderMetrics,
                bundle_storage::BundleStorage,
                BundleStageLoopMetrics,
            },
            immutable_deserialized_bundle::ImmutableDeserializedBundle,
            packet_bundle::PacketBundle,
        },
        crossbeam_channel::{unbounded, RecvTimeoutError},
        rand::{thread_rng, RngCore},
        solana_bundle::{
            bundle_execution::LoadAndExecuteBundleError, BundleExecutionError, SanitizedBundle,
            TipError,
        },
        solana_bundle_sdk::derive_bundle_id,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_ledger::genesis_utils::create_genesis_config,
        solana_perf::packet::{BytesPacket, PacketBatch},
        solana_poh::poh_recorder::PohRecorderError,
        solana_runtime::{bank::Bank, genesis_utils::GenesisConfigInfo},
        solana_signer::Signer,
        solana_system_transaction::transfer,
        solana_transaction::versioned::VersionedTransaction,
        std::{
            collections::HashSet,
            sync::Arc,
            thread::sleep,
            time::{Duration, Instant},
        },
    };

    /// Makes `num_bundles` random bundles with `num_packets_per_bundle` packets per bundle.
    fn make_random_bundles(
        mint_keypair: &Keypair,
        num_bundles: usize,
        num_packets_per_bundle: usize,
        hash: Hash,
    ) -> Vec<PacketBundle> {
        let mut rng = thread_rng();

        (0..num_bundles)
            .map(|_| {
                let transfers: Vec<_> = (0..num_packets_per_bundle)
                    .map(|_| {
                        VersionedTransaction::from(transfer(
                            mint_keypair,
                            &mint_keypair.pubkey(),
                            rng.next_u64(),
                            hash,
                        ))
                    })
                    .collect();
                let bundle_id = derive_bundle_id(&transfers).unwrap();

                PacketBundle {
                    batch: PacketBatch::from(
                        transfers
                            .iter()
                            .map(|tx| BytesPacket::from_data(None, tx).unwrap())
                            .collect::<Vec<_>>(),
                    ),
                    bundle_id,
                }
            })
            .collect()
    }

    fn assert_bundles_same(
        packet_bundles: &[PacketBundle],
        bundles_to_process: &[(ImmutableDeserializedBundle, SanitizedBundle)],
    ) {
        assert_eq!(packet_bundles.len(), bundles_to_process.len());
        packet_bundles
            .iter()
            .zip(bundles_to_process.iter())
            .for_each(|(packet_bundle, (_, sanitized_bundle))| {
                assert_eq!(packet_bundle.bundle_id, sanitized_bundle.bundle_id);
                assert_eq!(
                    packet_bundle.batch.len(),
                    sanitized_bundle.transactions.len()
                );
            });
    }

    #[test]
    fn test_receive_bundles() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut bundle_storage = BundleStorage::default();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        let bundles = make_random_bundles(&mint_keypair, 10, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let mut batch_bundle_results = ReceiveBundleResults::default();
        let mut batch_bundle_timer: Option<Instant> = None;

        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());
        // confirm bundles were batched
        assert_eq!(
            batch_bundle_results.deserialized_bundles.len(),
            bundles.len()
        );
        assert_eq!(batch_bundle_results.num_dropped_bundles.0, 0);
        // Confirm timer was set
        assert!(batch_bundle_timer.is_some());

        // Advance time by default batch timeout
        sleep(DEFAULT_BATCH_BUNDLE_TIMEOUT);

        // Call again to buffer batch
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        // No new bundles, so we should timeout
        assert_eq!(result.unwrap_err(), RecvTimeoutError::Timeout);
        // Confirm batch results is empty
        assert!(batch_bundle_results.is_empty());
        // Assert timer reset
        assert!(batch_bundle_timer.is_none());

        assert_eq!(bundle_storage.unprocessed_bundles_len(), 10);
        assert_eq!(bundle_storage.unprocessed_packets_len(), 20);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_packets_len(), 0);
        assert_eq!(bundle_storage.max_receive_size(), 990);

        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles, bundles_to_process);
                (0..bundles_to_process.len()).map(|_| Ok(())).collect()
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.unprocessed_packets_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_packets_len(), 0);
        assert_eq!(bundle_storage.max_receive_size(), 1000);
    }

    #[test]
    fn test_receive_more_bundles_than_capacity() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut bundle_storage = BundleStorage::default();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 more than capacity
        let bundles = make_random_bundles(
            &mint_keypair,
            BundleStorage::BUNDLE_STORAGE_CAPACITY + 5,
            2,
            genesis_config.hash(),
        );

        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let mut batch_bundle_results = ReceiveBundleResults::default();
        let mut batch_bundle_timer: Option<Instant> = None;

        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        // Buffer batch (timeout error)
        sleep(DEFAULT_BATCH_BUNDLE_TIMEOUT);
        bundle_receiver
            .receive_and_buffer_bundles(
                &mut bundle_storage,
                &mut batch_bundle_results,
                &mut batch_bundle_timer,
                &mut bundle_stage_stats,
                &mut bundle_stage_leader_metrics,
            )
            .unwrap_err();

        // 1005 bundles were sent, but the capacity is 1000
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 1000);
        assert_eq!(bundle_storage.unprocessed_packets_len(), 2000);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_packets_len(), 0);

        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                // make sure the first 1000 bundles are the ones to process
                assert_bundles_same(
                    &bundles[0..BundleStorage::BUNDLE_STORAGE_CAPACITY],
                    bundles_to_process,
                );
                (0..bundles_to_process.len()).map(|_| Ok(())).collect()
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_poh_record_error_rebuffered() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut bundle_storage = BundleStorage::default();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 5, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let mut batch_bundle_results = ReceiveBundleResults::default();
        let mut batch_bundle_timer: Option<Instant> = None;

        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        // Buffer batch (timeout error)
        sleep(DEFAULT_BATCH_BUNDLE_TIMEOUT);
        bundle_receiver
            .receive_and_buffer_bundles(
                &mut bundle_storage,
                &mut batch_bundle_results,
                &mut batch_bundle_timer,
                &mut bundle_stage_stats,
                &mut bundle_stage_leader_metrics,
            )
            .unwrap_err();

        let poh_max_height_reached_index = 3;

        // make sure poh end of slot reached + the correct bundles are buffered for the next time.
        // bundles at index 3 + 4 are rebuffered
        assert!(bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles, bundles_to_process);

                let mut results = vec![Ok(()); bundles_to_process.len()];

                (poh_max_height_reached_index..bundles_to_process.len()).for_each(|index| {
                    results[index] = Err(BundleExecutionError::PohRecordError(
                        PohRecorderError::MaxHeightReached,
                    ));
                });
                results
            }
        ));

        assert_eq!(bundle_storage.unprocessed_bundles_len(), 2);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles[poh_max_height_reached_index..], bundles_to_process);
                vec![Ok(()); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_bank_processing_done_rebuffered() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut bundle_storage = BundleStorage::default();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 5, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let mut batch_bundle_results = ReceiveBundleResults::default();
        let mut batch_bundle_timer: Option<Instant> = None;

        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        // Buffer batch (timeout error)
        sleep(DEFAULT_BATCH_BUNDLE_TIMEOUT);
        bundle_receiver
            .receive_and_buffer_bundles(
                &mut bundle_storage,
                &mut batch_bundle_results,
                &mut batch_bundle_timer,
                &mut bundle_stage_stats,
                &mut bundle_stage_leader_metrics,
            )
            .unwrap_err();

        let bank_processing_done_index = 3;

        // bundles at index 3 + 4 are rebuffered
        assert!(bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles, bundles_to_process);

                let mut results = vec![Ok(()); bundles_to_process.len()];

                (bank_processing_done_index..bundles_to_process.len()).for_each(|index| {
                    results[index] = Err(BundleExecutionError::BankProcessingTimeLimitReached);
                });
                results
            }
        ));

        // 0, 1, 2 processed; 3, 4 buffered
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 2);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles[bank_processing_done_index..], bundles_to_process);
                vec![Ok(()); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_bank_execution_error_dropped() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut bundle_storage = BundleStorage::default();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 5, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let mut batch_bundle_results = ReceiveBundleResults::default();
        let mut batch_bundle_timer: Option<Instant> = None;

        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        // Buffer batch (timeout error)
        sleep(DEFAULT_BATCH_BUNDLE_TIMEOUT);
        bundle_receiver
            .receive_and_buffer_bundles(
                &mut bundle_storage,
                &mut batch_bundle_results,
                &mut batch_bundle_timer,
                &mut bundle_stage_stats,
                &mut bundle_stage_leader_metrics,
            )
            .unwrap_err();

        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles, bundles_to_process);
                vec![
                    Err(BundleExecutionError::TransactionFailure(
                        LoadAndExecuteBundleError::ProcessingTimeExceeded(Duration::from_secs(1)),
                    ));
                    bundles_to_process.len()
                ]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_tip_error_dropped() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut bundle_storage = BundleStorage::default();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 5, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let mut batch_bundle_results = ReceiveBundleResults::default();
        let mut batch_bundle_timer: Option<Instant> = None;

        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        // Buffer batch (timeout error)
        sleep(DEFAULT_BATCH_BUNDLE_TIMEOUT);
        bundle_receiver
            .receive_and_buffer_bundles(
                &mut bundle_storage,
                &mut batch_bundle_results,
                &mut batch_bundle_timer,
                &mut bundle_stage_stats,
                &mut bundle_stage_leader_metrics,
            )
            .unwrap_err();

        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles, bundles_to_process);
                vec![
                    Err(BundleExecutionError::TipError(TipError::LockError));
                    bundles_to_process.len()
                ]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_lock_error_dropped() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut bundle_storage = BundleStorage::default();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 5, 2, genesis_config.hash());
        sender.send(bundles).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let mut batch_bundle_results = ReceiveBundleResults::default();
        let mut batch_bundle_timer: Option<Instant> = None;

        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        // Buffer batch (timeout error)
        sleep(DEFAULT_BATCH_BUNDLE_TIMEOUT);
        bundle_receiver
            .receive_and_buffer_bundles(
                &mut bundle_storage,
                &mut batch_bundle_results,
                &mut batch_bundle_timer,
                &mut bundle_stage_stats,
                &mut bundle_stage_leader_metrics,
            )
            .unwrap_err();

        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                vec![Err(BundleExecutionError::LockError); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_cost_model_exceeded_set_aside_and_requeued() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut bundle_storage = BundleStorage::default();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 5, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let mut batch_bundle_results = ReceiveBundleResults::default();
        let mut batch_bundle_timer: Option<Instant> = None;

        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        // Buffer batch (timeout error)
        sleep(DEFAULT_BATCH_BUNDLE_TIMEOUT);
        bundle_receiver
            .receive_and_buffer_bundles(
                &mut bundle_storage,
                &mut batch_bundle_results,
                &mut batch_bundle_timer,
                &mut bundle_stage_stats,
                &mut bundle_stage_leader_metrics,
            )
            .unwrap_err();

        // buffered bundles are moved to cost model side deque
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles, bundles_to_process);
                vec![Err(BundleExecutionError::ExceedsCostModel); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 5);

        // double check there's no bundles to process
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert!(bundles_to_process.is_empty());
                vec![Ok(()); bundles_to_process.len()]
            }
        ));

        // create a new bank w/ new slot number, cost model buffered packets should move back onto queue
        // in the same order they were originally
        let bank = bank_forks.read().unwrap().working_bank();
        let new_bank = Arc::new(Bank::new_from_parent(
            bank.clone(),
            bank.collector_id(),
            bank.slot() + 1,
        ));
        assert!(!bundle_storage.process_bundles(
            new_bank,
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                // make sure same order as original
                assert_bundles_same(&bundles, bundles_to_process);
                vec![Ok(()); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_cost_model_exceeded_buffer_capacity() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut bundle_storage = BundleStorage::default();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 500 bundles across the queue
        let bundles0 = make_random_bundles(
            &mint_keypair,
            BundleStorage::BUNDLE_STORAGE_CAPACITY / 2,
            2,
            genesis_config.hash(),
        );
        sender.send(bundles0.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);

        // receive and buffer bundles to the cost model reserve to test the capacity/dropped bundles there
        let mut batch_bundle_results = ReceiveBundleResults::default();
        let mut batch_bundle_timer: Option<Instant> = None;

        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        // Buffer batch (timeout error)
        sleep(DEFAULT_BATCH_BUNDLE_TIMEOUT);
        bundle_receiver
            .receive_and_buffer_bundles(
                &mut bundle_storage,
                &mut batch_bundle_results,
                &mut batch_bundle_timer,
                &mut bundle_stage_stats,
                &mut bundle_stage_leader_metrics,
            )
            .unwrap_err();

        // buffered bundles are moved to cost model side deque
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles0, bundles_to_process);
                vec![Err(BundleExecutionError::ExceedsCostModel); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 500);

        let bundles1 = make_random_bundles(
            &mint_keypair,
            BundleStorage::BUNDLE_STORAGE_CAPACITY / 2,
            2,
            genesis_config.hash(),
        );
        sender.send(bundles1.clone()).unwrap();
        // should get 500 more bundles, cost model buffered length should be 1000
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        // Buffer batch (timeout error)
        sleep(DEFAULT_BATCH_BUNDLE_TIMEOUT);
        bundle_receiver
            .receive_and_buffer_bundles(
                &mut bundle_storage,
                &mut batch_bundle_results,
                &mut batch_bundle_timer,
                &mut bundle_stage_stats,
                &mut bundle_stage_leader_metrics,
            )
            .unwrap_err();

        // buffered bundles are moved to cost model side deque
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles1, bundles_to_process);
                vec![Err(BundleExecutionError::ExceedsCostModel); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 1000); // full now

        // send 10 bundles to go over capacity
        let bundles2 = make_random_bundles(&mint_keypair, 10, 2, genesis_config.hash());
        sender.send(bundles2.clone()).unwrap();

        // this set will get dropped from cost model buffered bundles
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        // Buffer batch (timeout error)
        sleep(DEFAULT_BATCH_BUNDLE_TIMEOUT);
        bundle_receiver
            .receive_and_buffer_bundles(
                &mut bundle_storage,
                &mut batch_bundle_results,
                &mut batch_bundle_timer,
                &mut bundle_stage_stats,
                &mut bundle_stage_leader_metrics,
            )
            .unwrap_err();

        // buffered bundles are moved to cost model side deque, but its at capacity so stays the same size
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles2, bundles_to_process);
                vec![Err(BundleExecutionError::ExceedsCostModel); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 1000);

        // create new bank then call process_bundles again, expect to see [bundles1,bundles2]
        let bank = bank_forks.read().unwrap().working_bank();
        let new_bank = Arc::new(Bank::new_from_parent(
            bank.clone(),
            bank.collector_id(),
            bank.slot() + 1,
        ));
        assert!(!bundle_storage.process_bundles(
            new_bank,
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                // make sure same order as original
                let expected_bundles: Vec<_> =
                    bundles0.iter().chain(bundles1.iter()).cloned().collect();
                assert_bundles_same(&expected_bundles, bundles_to_process);
                vec![Ok(()); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
    }

    #[test]
    fn test_batching_bundles() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut bundle_storage = BundleStorage::default();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 2 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 2, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);

        let mut batch_bundle_results = ReceiveBundleResults::default();
        let mut batch_bundle_timer: Option<Instant> = None;

        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());
        assert_eq!(
            batch_bundle_results.deserialized_bundles.len(),
            bundles.len()
        );

        let bundles2 = make_random_bundles(&mint_keypair, 2, 2, genesis_config.hash());
        sender.send(bundles2.clone()).unwrap();

        // Some time should have passed, but not 10ms
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut bundle_storage,
            &mut batch_bundle_results,
            &mut batch_bundle_timer,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());
        assert_eq!(
            batch_bundle_results.deserialized_bundles.len(),
            bundles.len() + bundles2.len()
        );

        // Fast forward 10ms to buffer the batch
        sleep(DEFAULT_BATCH_BUNDLE_TIMEOUT);
        let err = bundle_receiver
            .receive_and_buffer_bundles(
                &mut bundle_storage,
                &mut batch_bundle_results,
                &mut batch_bundle_timer,
                &mut bundle_stage_stats,
                &mut bundle_stage_leader_metrics,
            )
            .unwrap_err();

        // No new bundles, so we should timeout
        assert_eq!(err, RecvTimeoutError::Timeout);
        // Confirm batch results is empty
        assert!(batch_bundle_results.is_empty());
        // Assert timer reset
        assert!(batch_bundle_timer.is_none());

        assert_eq!(bundle_storage.unprocessed_bundles_len(), 4);
        assert_eq!(bundle_storage.unprocessed_packets_len(), 8);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_packets_len(), 0);
        assert_eq!(bundle_storage.max_receive_size(), 996);

        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            &HashSet::default(),
            |bundles_to_process, _stats| {
                let merged_bundles = bundles
                    .iter()
                    .chain(bundles2.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                assert_bundles_same(&merged_bundles, bundles_to_process);
                (0..bundles_to_process.len()).map(|_| Ok(())).collect()
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.unprocessed_packets_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_packets_len(), 0);
        assert_eq!(bundle_storage.max_receive_size(), 1000);
    }
}
