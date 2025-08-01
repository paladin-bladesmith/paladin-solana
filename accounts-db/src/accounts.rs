use {
    crate::{
        account_locks::{validate_account_locks, AccountLocks},
        account_storage::stored_account_info::StoredAccountInfo,
        accounts_db::{
            AccountStorageEntry, AccountsAddRootTiming, AccountsDb, LoadHint, LoadedAccount,
            ScanAccountStorageData, ScanStorageResult, VerifyAccountsHashAndLamportsConfig,
        },
        accounts_index::{IndexKey, ScanConfig, ScanError, ScanOrder, ScanResult},
        ancestors::Ancestors,
        is_loadable::IsLoadable as _,
        storable_accounts::StorableAccounts,
    },
    log::*,
    solana_account::{AccountSharedData, ReadableAccount},
    solana_address_lookup_table_interface::{
        self as address_lookup_table, error::AddressLookupError, state::AddressLookupTable,
    },
    solana_clock::{BankId, Slot},
    solana_message::v0::LoadedAddresses,
    solana_pubkey::Pubkey,
    solana_slot_hashes::SlotHashes,
    solana_svm_transaction::{
        message_address_table_lookup::SVMMessageAddressTableLookup, svm_message::SVMMessage,
    },
    solana_transaction::sanitized::SanitizedTransaction,
    solana_transaction_context::TransactionAccount,
    solana_transaction_error::{TransactionError, TransactionResult as Result},
    std::{
        cmp::Reverse,
        collections::{BinaryHeap, HashMap, HashSet},
        ops::RangeBounds,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
    },
};

pub type PubkeyAccountSlot = (Pubkey, AccountSharedData, Slot);

pub struct TransactionAccountLocksIterator<'a, T: SVMMessage> {
    transaction: &'a T,
}

impl<'a, T: SVMMessage> TransactionAccountLocksIterator<'a, T> {
    pub fn new(transaction: &'a T) -> Self {
        Self { transaction }
    }

    pub(crate) fn accounts_with_is_writable(
        &self,
    ) -> impl Iterator<Item = (&'a Pubkey, bool)> + Clone {
        self.transaction
            .account_keys()
            .iter()
            .enumerate()
            .map(|(index, key)| (key, self.transaction.is_writable(index)))
    }
}

/// This structure handles synchronization for db
#[derive(Debug)]
pub struct Accounts {
    /// Single global AccountsDb
    pub accounts_db: Arc<AccountsDb>,

    /// set of read-only and writable accounts which are currently
    /// being processed by banking/replay threads
    pub(crate) account_locks: Mutex<AccountLocks>,
}

pub enum AccountAddressFilter {
    Exclude, // exclude all addresses matching the filter
    Include, // only include addresses matching the filter
}

impl Accounts {
    pub fn new(accounts_db: Arc<AccountsDb>) -> Self {
        Self {
            accounts_db,
            account_locks: Mutex::new(AccountLocks::default()),
        }
    }

    /// Return loaded addresses and the deactivation slot.
    /// If the table hasn't been deactivated, the deactivation slot is `u64::MAX`.
    pub fn load_lookup_table_addresses(
        &self,
        ancestors: &Ancestors,
        address_table_lookup: SVMMessageAddressTableLookup,
        slot_hashes: &SlotHashes,
    ) -> std::result::Result<(LoadedAddresses, Slot), AddressLookupError> {
        let mut loaded_addresses = LoadedAddresses::default();
        self.load_lookup_table_addresses_into(
            ancestors,
            address_table_lookup,
            slot_hashes,
            &mut loaded_addresses,
        )
        .map(|deactivation_slot| (loaded_addresses, deactivation_slot))
    }

    /// Fill `loaded_addresses` and return the deactivation slot.
    /// If no tables are de-activating, the deactivation slot is `u64::MAX`.
    pub fn load_lookup_table_addresses_into(
        &self,
        ancestors: &Ancestors,
        address_table_lookup: SVMMessageAddressTableLookup,
        slot_hashes: &SlotHashes,
        loaded_addresses: &mut LoadedAddresses,
    ) -> std::result::Result<Slot, AddressLookupError> {
        let table_account = self
            .accounts_db
            .load_with_fixed_root(ancestors, address_table_lookup.account_key)
            .map(|(account, _rent)| account)
            .ok_or(AddressLookupError::LookupTableAccountNotFound)?;

        if table_account.owner() == &address_lookup_table::program::id() {
            let current_slot = ancestors.max_slot();
            let lookup_table = AddressLookupTable::deserialize(table_account.data())
                .map_err(|_ix_err| AddressLookupError::InvalidAccountData)?;

            // Load iterators for addresses.
            let writable_addresses = lookup_table.lookup_iter(
                current_slot,
                address_table_lookup.writable_indexes,
                slot_hashes,
            )?;
            let readonly_addresses = lookup_table.lookup_iter(
                current_slot,
                address_table_lookup.readonly_indexes,
                slot_hashes,
            )?;

            // Reserve space in vectors to avoid reallocations.
            // If `loaded_addresses` is pre-allocated, this only does a simple
            // bounds check.
            loaded_addresses
                .writable
                .reserve(address_table_lookup.writable_indexes.len());
            loaded_addresses
                .readonly
                .reserve(address_table_lookup.readonly_indexes.len());

            // Append to the loaded addresses.
            // Check if **any** of the addresses are not available.
            for address in writable_addresses {
                loaded_addresses
                    .writable
                    .push(address.ok_or(AddressLookupError::InvalidLookupIndex)?);
            }
            for address in readonly_addresses {
                loaded_addresses
                    .readonly
                    .push(address.ok_or(AddressLookupError::InvalidLookupIndex)?);
            }

            Ok(lookup_table.meta.deactivation_slot)
        } else {
            Err(AddressLookupError::InvalidAccountOwner)
        }
    }
    /// Slow because lock is held for 1 operation instead of many
    /// This always returns None for zero-lamport accounts.
    fn load_slow(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
        load_hint: LoadHint,
    ) -> Option<(AccountSharedData, Slot)> {
        self.accounts_db.load(ancestors, pubkey, load_hint)
    }

    pub fn load_with_fixed_root(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
    ) -> Option<(AccountSharedData, Slot)> {
        self.load_slow(ancestors, pubkey, LoadHint::FixedMaxRoot)
    }

    /// same as `load_with_fixed_root` except:
    /// if the account is not already in the read cache, it is NOT put in the read cache on successful load
    pub fn load_with_fixed_root_do_not_populate_read_cache(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
    ) -> Option<(AccountSharedData, Slot)> {
        self.load_slow(
            ancestors,
            pubkey,
            LoadHint::FixedMaxRootDoNotPopulateReadCache,
        )
    }

    pub fn load_without_fixed_root(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
    ) -> Option<(AccountSharedData, Slot)> {
        self.load_slow(ancestors, pubkey, LoadHint::Unspecified)
    }

    /// scans underlying accounts_db for this delta (slot) with a map function
    ///   from LoadedAccount to B
    /// returns only the latest/current version of B for this slot
    pub fn scan_slot<F, B>(&self, slot: Slot, func: F) -> Vec<B>
    where
        F: Fn(&LoadedAccount) -> Option<B> + Send + Sync,
        B: Sync + Send + Default + std::cmp::Eq,
    {
        let scan_result = self.accounts_db.scan_account_storage(
            slot,
            |loaded_account: &LoadedAccount| {
                // Cache only has one version per key, don't need to worry about versioning
                func(loaded_account)
            },
            |accum: &mut HashMap<Pubkey, B>, stored_account, data| {
                // SAFETY: We called scan_account_storage() with
                // ScanAccountStorageData::DataRefForStorage, so `data` must be Some.
                let data = data.unwrap();
                let loaded_account =
                    LoadedAccount::Stored(StoredAccountInfo::new_from(stored_account, data));
                let loaded_account_pubkey = *loaded_account.pubkey();
                if let Some(val) = func(&loaded_account) {
                    accum.insert(loaded_account_pubkey, val);
                }
            },
            ScanAccountStorageData::DataRefForStorage,
        );

        match scan_result {
            ScanStorageResult::Cached(cached_result) => cached_result,
            ScanStorageResult::Stored(stored_result) => stored_result.into_values().collect(),
        }
    }

    /// Returns all the accounts from `slot`
    ///
    /// If `program_id` is `Some`, filter the results to those whose owner matches `program_id`
    pub fn load_by_program_slot(
        &self,
        slot: Slot,
        program_id: Option<&Pubkey>,
    ) -> Vec<TransactionAccount> {
        self.scan_slot(slot, |stored_account| {
            program_id
                .map(|program_id| program_id == stored_account.owner())
                .unwrap_or(true)
                .then(|| (*stored_account.pubkey(), stored_account.take_account()))
        })
    }

    pub fn load_largest_accounts(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        num: usize,
        filter_by_address: &HashSet<Pubkey>,
        filter: AccountAddressFilter,
        sort_results: bool,
    ) -> ScanResult<Vec<(Pubkey, u64)>> {
        if num == 0 {
            return Ok(vec![]);
        }
        let scan_order = if sort_results {
            ScanOrder::Sorted
        } else {
            ScanOrder::Unsorted
        };
        let mut account_balances = BinaryHeap::new();
        self.accounts_db.scan_accounts(
            ancestors,
            bank_id,
            |option| {
                if let Some((pubkey, account, _slot)) = option {
                    if account.lamports() == 0 {
                        return;
                    }
                    let contains_address = filter_by_address.contains(pubkey);
                    let collect = match filter {
                        AccountAddressFilter::Exclude => !contains_address,
                        AccountAddressFilter::Include => contains_address,
                    };
                    if !collect {
                        return;
                    }
                    if account_balances.len() == num {
                        let Reverse(entry) = account_balances
                            .peek()
                            .expect("BinaryHeap::peek should succeed when len > 0");
                        if *entry >= (account.lamports(), *pubkey) {
                            return;
                        }
                        account_balances.pop();
                    }
                    account_balances.push(Reverse((account.lamports(), *pubkey)));
                }
            },
            &ScanConfig::new(scan_order),
        )?;
        Ok(account_balances
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse((balance, pubkey))| (pubkey, balance))
            .collect())
    }

    /// Only called from startup or test code.
    #[must_use]
    pub fn verify_accounts_hash_and_lamports(
        &self,
        snapshot_storages_and_slots: (&[Arc<AccountStorageEntry>], &[Slot]),
        slot: Slot,
        total_lamports: u64,
        base: Option<(Slot, /*capitalization*/ u64)>,
        config: VerifyAccountsHashAndLamportsConfig,
    ) -> bool {
        if let Err(err) = self.accounts_db.verify_accounts_hash_and_lamports(
            snapshot_storages_and_slots,
            slot,
            total_lamports,
            base,
            config,
        ) {
            warn!("verify_accounts_hash failed: {err:?}, slot: {slot}");
            false
        } else {
            true
        }
    }

    fn load_while_filtering<F: Fn(&AccountSharedData) -> bool>(
        collector: &mut Vec<TransactionAccount>,
        some_account_tuple: Option<(&Pubkey, AccountSharedData, Slot)>,
        filter: F,
    ) {
        if let Some(mapped_account_tuple) = some_account_tuple
            .filter(|(_, account, _)| account.is_loadable() && filter(account))
            .map(|(pubkey, account, _slot)| (*pubkey, account))
        {
            collector.push(mapped_account_tuple)
        }
    }

    fn load_with_slot(
        collector: &mut Vec<PubkeyAccountSlot>,
        some_account_tuple: Option<(&Pubkey, AccountSharedData, Slot)>,
    ) {
        if let Some(mapped_account_tuple) = some_account_tuple
            .filter(|(_, account, _)| account.is_loadable())
            .map(|(pubkey, account, slot)| (*pubkey, account, slot))
        {
            collector.push(mapped_account_tuple)
        }
    }

    pub fn load_by_program(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        program_id: &Pubkey,
        config: &ScanConfig,
    ) -> ScanResult<Vec<TransactionAccount>> {
        let mut collector = Vec::new();
        self.accounts_db
            .scan_accounts(
                ancestors,
                bank_id,
                |some_account_tuple| {
                    Self::load_while_filtering(&mut collector, some_account_tuple, |account| {
                        account.owner() == program_id
                    })
                },
                config,
            )
            .map(|_| collector)
    }

    pub fn load_by_program_with_filter<F: Fn(&AccountSharedData) -> bool>(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        program_id: &Pubkey,
        filter: F,
        config: &ScanConfig,
    ) -> ScanResult<Vec<TransactionAccount>> {
        let mut collector = Vec::new();
        self.accounts_db
            .scan_accounts(
                ancestors,
                bank_id,
                |some_account_tuple| {
                    Self::load_while_filtering(&mut collector, some_account_tuple, |account| {
                        account.owner() == program_id && filter(account)
                    })
                },
                config,
            )
            .map(|_| collector)
    }

    fn calc_scan_result_size(account: &AccountSharedData) -> usize {
        account.data().len()
            + std::mem::size_of::<AccountSharedData>()
            + std::mem::size_of::<Pubkey>()
    }

    /// Accumulate size of (pubkey + account) into sum.
    /// Return true iff sum > 'byte_limit_for_scan'
    fn accumulate_and_check_scan_result_size(
        sum: &AtomicUsize,
        account: &AccountSharedData,
        byte_limit_for_scan: &Option<usize>,
    ) -> bool {
        if let Some(byte_limit_for_scan) = byte_limit_for_scan.as_ref() {
            let added = Self::calc_scan_result_size(account);
            sum.fetch_add(added, Ordering::Relaxed)
                .saturating_add(added)
                > *byte_limit_for_scan
        } else {
            false
        }
    }

    fn maybe_abort_scan(
        result: ScanResult<Vec<TransactionAccount>>,
        config: &ScanConfig,
    ) -> ScanResult<Vec<TransactionAccount>> {
        if config.is_aborted() {
            ScanResult::Err(ScanError::Aborted(
                "The accumulated scan results exceeded the limit".to_string(),
            ))
        } else {
            result
        }
    }

    pub fn load_by_index_key_with_filter<F: Fn(&AccountSharedData) -> bool>(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        index_key: &IndexKey,
        filter: F,
        config: &ScanConfig,
        byte_limit_for_scan: Option<usize>,
    ) -> ScanResult<Vec<TransactionAccount>> {
        let sum = AtomicUsize::default();
        let config = config.recreate_with_abort();
        let mut collector = Vec::new();
        let result = self
            .accounts_db
            .index_scan_accounts(
                ancestors,
                bank_id,
                *index_key,
                |some_account_tuple| {
                    Self::load_while_filtering(&mut collector, some_account_tuple, |account| {
                        let use_account = filter(account);
                        if use_account
                            && Self::accumulate_and_check_scan_result_size(
                                &sum,
                                account,
                                &byte_limit_for_scan,
                            )
                        {
                            // total size of results exceeds size limit, so abort scan
                            config.abort();
                        }
                        use_account
                    });
                },
                &config,
            )
            .map(|_| collector);
        Self::maybe_abort_scan(result, &config)
    }

    pub fn account_indexes_include_key(&self, key: &Pubkey) -> bool {
        self.accounts_db.account_indexes.include_key(key)
    }

    pub fn load_all(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        sort_results: bool,
    ) -> ScanResult<Vec<PubkeyAccountSlot>> {
        let scan_order = if sort_results {
            ScanOrder::Sorted
        } else {
            ScanOrder::Unsorted
        };
        let mut collector = Vec::new();
        self.accounts_db
            .scan_accounts(
                ancestors,
                bank_id,
                |some_account_tuple| {
                    if let Some((pubkey, account, slot)) =
                        some_account_tuple.filter(|(_, account, _)| account.is_loadable())
                    {
                        collector.push((*pubkey, account, slot))
                    }
                },
                &ScanConfig::new(scan_order),
            )
            .map(|_| collector)
    }

    pub fn scan_all<F>(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        scan_func: F,
        sort_results: bool,
    ) -> ScanResult<()>
    where
        F: FnMut(Option<(&Pubkey, AccountSharedData, Slot)>),
    {
        let scan_order = if sort_results {
            ScanOrder::Sorted
        } else {
            ScanOrder::Unsorted
        };
        self.accounts_db
            .scan_accounts(ancestors, bank_id, scan_func, &ScanConfig::new(scan_order))
    }

    pub fn hold_range_in_memory<R>(
        &self,
        range: &R,
        start_holding: bool,
        thread_pool: &rayon::ThreadPool,
    ) where
        R: RangeBounds<Pubkey> + std::fmt::Debug + Sync,
    {
        self.accounts_db
            .accounts_index
            .hold_range_in_memory(range, start_holding, thread_pool)
    }

    pub fn load_to_collect_rent_eagerly<R: RangeBounds<Pubkey> + std::fmt::Debug>(
        &self,
        ancestors: &Ancestors,
        range: R,
    ) -> Vec<PubkeyAccountSlot> {
        let mut collector = Vec::new();
        self.accounts_db.range_scan_accounts(
            "", // disable logging of this. We now parallelize it and this results in multiple parallel logs
            ancestors,
            range,
            &ScanConfig::default(),
            |option| Self::load_with_slot(&mut collector, option),
        );
        collector
    }

    /// Slow because lock is held for 1 operation instead of many.
    /// WARNING: This noncached version is only to be used for tests/benchmarking
    /// as bypassing the cache in general is not supported
    #[cfg(feature = "dev-context-only-utils")]
    pub fn store_slow_uncached(&self, slot: Slot, pubkey: &Pubkey, account: &AccountSharedData) {
        self.accounts_db.store_uncached(slot, &[(pubkey, account)]);
    }

    /// This function will prevent multiple threads from modifying the same account state at the
    /// same time, possibly excluding transactions based on prior results
    #[must_use]
    pub fn lock_accounts<'a>(
        &self,
        txs: impl Iterator<Item = &'a (impl SVMMessage + 'a)>,
        results: impl Iterator<Item = Result<()>>,
        tx_account_lock_limit: usize,
        relax_intrabatch_account_locks: bool,
        is_read_prelocked_callback: &impl Fn(&Pubkey) -> bool,
        is_write_prelocked_callback: &impl Fn(&Pubkey) -> bool,
    ) -> Vec<Result<()>> {
        // Validate the account locks, then get keys and is_writable if successful validation.
        // We collect to fully evaluate before taking the account_locks mutex.
        let validated_batch_keys = txs
            .zip(results)
            .map(|(tx, result)| {
                result
                    .and_then(|_| validate_account_locks(tx.account_keys(), tx_account_lock_limit))
                    .map(|_| TransactionAccountLocksIterator::new(tx).accounts_with_is_writable())
            })
            .collect::<Vec<_>>();

        let account_locks = &mut self.account_locks.lock().unwrap();

        if relax_intrabatch_account_locks {
            account_locks.try_lock_transaction_batch(
                validated_batch_keys,
                is_read_prelocked_callback,
                is_write_prelocked_callback,
            )
        } else {
            validated_batch_keys
                .into_iter()
                .map(|result_validated_tx_keys| match result_validated_tx_keys {
                    Ok(validated_tx_keys) => account_locks.try_lock_accounts(
                        validated_tx_keys,
                        is_read_prelocked_callback,
                        is_write_prelocked_callback,
                    ),
                    Err(e) => Err(e),
                })
                .collect()
        }
    }

    /// Once accounts are unlocked, new transactions that modify that state can enter the pipeline
    pub fn unlock_accounts<'a, Tx: SVMMessage + 'a>(
        &self,
        txs_and_results: impl Iterator<Item = (&'a Tx, &'a Result<()>)> + Clone,
    ) {
        if !txs_and_results.clone().any(|(_, res)| res.is_ok()) {
            return;
        }

        let mut account_locks = self.account_locks.lock().unwrap();
        debug!("bank unlock accounts");
        for (tx, res) in txs_and_results {
            if res.is_ok() {
                let tx_account_locks = TransactionAccountLocksIterator::new(tx);
                account_locks.unlock_accounts(tx_account_locks.accounts_with_is_writable());
            }
        }
    }

    /// Store the accounts into the DB
    pub fn store_cached<'a>(
        &self,
        accounts: impl StorableAccounts<'a>,
        transactions: Option<&'a [&'a SanitizedTransaction]>,
    ) {
        self.accounts_db
            .store_cached_inline_update_index(accounts, transactions);
    }

    pub fn store_accounts_cached<'a>(&self, accounts: impl StorableAccounts<'a>) {
        self.accounts_db.store_cached(accounts, None)
    }

    /// Add a slot to root.  Root slots cannot be purged
    pub fn add_root(&self, slot: Slot) -> AccountsAddRootTiming {
        self.accounts_db.add_root(slot)
    }

    pub fn lock_accounts_sequential_with_results<Tx: SVMMessage>(
        &self,
        txs: &[Tx],
        tx_account_lock_limit: usize,
    ) -> Vec<Result<()>> {
        let tx_account_locks_results: Vec<_> = txs
            .iter()
            .map(|tx| {
                validate_account_locks(tx.account_keys(), tx_account_lock_limit)
                    .map(|_| TransactionAccountLocksIterator::new(tx))
            })
            .collect();
        self.lock_accounts_sequential_inner(tx_account_locks_results)
    }

    #[must_use]
    fn lock_accounts_sequential_inner<Tx: SVMMessage>(
        &self,
        tx_account_locks_results: Vec<Result<TransactionAccountLocksIterator<Tx>>>,
    ) -> Vec<Result<()>> {
        let mut l_account_locks = self.account_locks.lock().unwrap();
        Self::lock_accounts_sequential(&mut l_account_locks, tx_account_locks_results)
    }

    pub fn lock_accounts_sequential<Tx: SVMMessage>(
        account_locks: &mut AccountLocks,
        tx_account_locks_results: Vec<Result<TransactionAccountLocksIterator<Tx>>>,
    ) -> Vec<Result<()>> {
        let mut account_in_use_set = false;
        tx_account_locks_results
            .into_iter()
            .map(|tx_account_locks_result| match tx_account_locks_result {
                Ok(tx_account_locks) => match account_in_use_set {
                    true => Err(TransactionError::AccountInUse),
                    false => {
                        let locked = account_locks.try_lock_accounts(
                            tx_account_locks.accounts_with_is_writable(),
                            &|_| false,
                            &|_| false,
                        );
                        if matches!(locked, Err(TransactionError::AccountInUse)) {
                            account_in_use_set = true;
                        }
                        locked
                    }
                },
                Err(err) => Err(err),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        agave_reserved_account_keys::ReservedAccountKeys,
        solana_account::{AccountSharedData, WritableAccount},
        solana_address_lookup_table_interface::state::LookupTableMeta,
        solana_hash::Hash,
        solana_instruction::{AccountMeta, Instruction},
        solana_keypair::Keypair,
        solana_message::{
            compiled_instruction::CompiledInstruction, v0::MessageAddressTableLookup,
            LegacyMessage, Message, MessageHeader, SanitizedMessage,
        },
        solana_sdk_ids::native_loader,
        solana_signature::Signature,
        solana_signer::{signers::Signers, Signer},
        solana_transaction::{sanitized::MAX_TX_ACCOUNT_LOCKS, Transaction},
        solana_transaction_error::TransactionError,
        std::{
            borrow::Cow,
            iter,
            sync::atomic::{AtomicBool, AtomicU64, Ordering},
            thread, time,
        },
        test_case::test_case,
    };

    fn new_sanitized_tx<T: Signers>(
        from_keypairs: &T,
        message: Message,
        recent_blockhash: Hash,
    ) -> SanitizedTransaction {
        SanitizedTransaction::from_transaction_for_tests(Transaction::new(
            from_keypairs,
            message,
            recent_blockhash,
        ))
    }

    fn sanitized_tx_from_metas(accounts: Vec<AccountMeta>) -> SanitizedTransaction {
        let instruction = Instruction {
            accounts,
            program_id: Pubkey::default(),
            data: vec![],
        };

        let message = Message::new(&[instruction], None);

        let sanitized_message = SanitizedMessage::Legacy(LegacyMessage::new(
            message,
            &ReservedAccountKeys::empty_key_set(),
        ));

        SanitizedTransaction::new_for_tests(sanitized_message, vec![Signature::new_unique()], false)
    }

    #[test]
    fn test_hold_range_in_memory() {
        let accounts_db = AccountsDb::default_for_tests();
        let accts = Accounts::new(Arc::new(accounts_db));
        let range = Pubkey::from([0; 32])..=Pubkey::from([0xff; 32]);
        accts.hold_range_in_memory(&range, true, &test_thread_pool());
        accts.hold_range_in_memory(&range, false, &test_thread_pool());
        accts.hold_range_in_memory(&range, true, &test_thread_pool());
        accts.hold_range_in_memory(&range, true, &test_thread_pool());
        accts.hold_range_in_memory(&range, false, &test_thread_pool());
        accts.hold_range_in_memory(&range, false, &test_thread_pool());
    }

    #[test]
    fn test_hold_range_in_memory2() {
        let accounts_db = AccountsDb::default_for_tests();
        let accts = Accounts::new(Arc::new(accounts_db));
        let range = Pubkey::from([0; 32])..=Pubkey::from([0xff; 32]);
        let idx = &accts.accounts_db.accounts_index;
        let bins = idx.account_maps.len();
        // use bins * 2 to get the first half of the range within bin 0
        let bins_2 = bins * 2;
        let binner = crate::pubkey_bins::PubkeyBinCalculator24::new(bins_2);
        let range2 = binner.lowest_pubkey_from_bin(0)..binner.lowest_pubkey_from_bin(1);
        let range2_inclusive = range2.start..=range2.end;
        assert_eq!(0, idx.bin_calculator.bin_from_pubkey(&range2.start));
        assert_eq!(0, idx.bin_calculator.bin_from_pubkey(&range2.end));
        accts.hold_range_in_memory(&range, true, &test_thread_pool());
        idx.account_maps.iter().for_each(|map| {
            assert_eq!(
                map.cache_ranges_held.read().unwrap().to_vec(),
                vec![range.clone()]
            );
        });
        accts.hold_range_in_memory(&range2, true, &test_thread_pool());
        idx.account_maps.iter().enumerate().for_each(|(bin, map)| {
            let expected = if bin == 0 {
                vec![range.clone(), range2_inclusive.clone()]
            } else {
                vec![range.clone()]
            };
            assert_eq!(
                map.cache_ranges_held.read().unwrap().to_vec(),
                expected,
                "bin: {bin}"
            );
        });
        accts.hold_range_in_memory(&range, false, &test_thread_pool());
        accts.hold_range_in_memory(&range2, false, &test_thread_pool());
    }

    fn test_thread_pool() -> rayon::ThreadPool {
        crate::accounts_db::make_min_priority_thread_pool()
    }

    #[test]
    fn test_load_lookup_table_addresses_account_not_found() {
        let ancestors = vec![(0, 0)].into_iter().collect();
        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));

        let invalid_table_key = Pubkey::new_unique();
        let address_table_lookup = MessageAddressTableLookup {
            account_key: invalid_table_key,
            writable_indexes: vec![],
            readonly_indexes: vec![],
        };

        assert_eq!(
            accounts.load_lookup_table_addresses(
                &ancestors,
                SVMMessageAddressTableLookup::from(&address_table_lookup),
                &SlotHashes::default(),
            ),
            Err(AddressLookupError::LookupTableAccountNotFound),
        );
    }

    #[test]
    fn test_load_lookup_table_addresses_invalid_account_owner() {
        let ancestors = vec![(0, 0)].into_iter().collect();
        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));

        let invalid_table_key = Pubkey::new_unique();
        let mut invalid_table_account = AccountSharedData::default();
        invalid_table_account.set_lamports(1);
        accounts.store_slow_uncached(0, &invalid_table_key, &invalid_table_account);

        let address_table_lookup = MessageAddressTableLookup {
            account_key: invalid_table_key,
            writable_indexes: vec![],
            readonly_indexes: vec![],
        };

        assert_eq!(
            accounts.load_lookup_table_addresses(
                &ancestors,
                SVMMessageAddressTableLookup::from(&address_table_lookup),
                &SlotHashes::default(),
            ),
            Err(AddressLookupError::InvalidAccountOwner),
        );
    }

    #[test]
    fn test_load_lookup_table_addresses_invalid_account_data() {
        let ancestors = vec![(0, 0)].into_iter().collect();
        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));

        let invalid_table_key = Pubkey::new_unique();
        let invalid_table_account =
            AccountSharedData::new(1, 0, &address_lookup_table::program::id());
        accounts.store_slow_uncached(0, &invalid_table_key, &invalid_table_account);

        let address_table_lookup = MessageAddressTableLookup {
            account_key: invalid_table_key,
            writable_indexes: vec![],
            readonly_indexes: vec![],
        };

        assert_eq!(
            accounts.load_lookup_table_addresses(
                &ancestors,
                SVMMessageAddressTableLookup::from(&address_table_lookup),
                &SlotHashes::default(),
            ),
            Err(AddressLookupError::InvalidAccountData),
        );
    }

    #[test]
    fn test_load_lookup_table_addresses() {
        let ancestors = vec![(1, 1), (0, 0)].into_iter().collect();
        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));

        let table_key = Pubkey::new_unique();
        let table_addresses = vec![Pubkey::new_unique(), Pubkey::new_unique()];
        let table_account = {
            let table_state = AddressLookupTable {
                meta: LookupTableMeta::default(),
                addresses: Cow::Owned(table_addresses.clone()),
            };
            AccountSharedData::create(
                1,
                table_state.serialize_for_tests().unwrap(),
                address_lookup_table::program::id(),
                false,
                0,
            )
        };
        accounts.store_slow_uncached(0, &table_key, &table_account);

        let address_table_lookup = MessageAddressTableLookup {
            account_key: table_key,
            writable_indexes: vec![0],
            readonly_indexes: vec![1],
        };

        assert_eq!(
            accounts.load_lookup_table_addresses(
                &ancestors,
                SVMMessageAddressTableLookup::from(&address_table_lookup),
                &SlotHashes::default(),
            ),
            Ok((
                LoadedAddresses {
                    writable: vec![table_addresses[0]],
                    readonly: vec![table_addresses[1]],
                },
                u64::MAX
            )),
        );
    }

    #[test]
    fn test_load_by_program_slot() {
        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));

        // Load accounts owned by various programs into AccountsDb
        let pubkey0 = solana_pubkey::new_rand();
        let account0 = AccountSharedData::new(1, 0, &Pubkey::from([2; 32]));
        accounts.store_slow_uncached(0, &pubkey0, &account0);
        let pubkey1 = solana_pubkey::new_rand();
        let account1 = AccountSharedData::new(1, 0, &Pubkey::from([2; 32]));
        accounts.store_slow_uncached(0, &pubkey1, &account1);
        let pubkey2 = solana_pubkey::new_rand();
        let account2 = AccountSharedData::new(1, 0, &Pubkey::from([3; 32]));
        accounts.store_slow_uncached(0, &pubkey2, &account2);

        let loaded = accounts.load_by_program_slot(0, Some(&Pubkey::from([2; 32])));
        assert_eq!(loaded.len(), 2);
        let loaded = accounts.load_by_program_slot(0, Some(&Pubkey::from([3; 32])));
        assert_eq!(loaded, vec![(pubkey2, account2)]);
        let loaded = accounts.load_by_program_slot(0, Some(&Pubkey::from([4; 32])));
        assert_eq!(loaded, vec![]);
    }

    #[test_case(false; "old")]
    #[test_case(true; "simd83")]
    fn test_lock_accounts_with_duplicates(relax_intrabatch_account_locks: bool) {
        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));

        let keypair = Keypair::new();
        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys: vec![keypair.pubkey(), keypair.pubkey()],
            ..Message::default()
        };

        let tx = new_sanitized_tx(&[&keypair], message, Hash::default());
        let results = accounts.lock_accounts(
            [tx].iter(),
            [Ok(())].into_iter(),
            MAX_TX_ACCOUNT_LOCKS,
            relax_intrabatch_account_locks,
            &|_| false,
            &|_| false,
        );
        assert_eq!(results[0], Err(TransactionError::AccountLoadedTwice));
    }

    #[test_case(false; "old")]
    #[test_case(true; "simd83")]
    fn test_lock_accounts_with_too_many_accounts(relax_intrabatch_account_locks: bool) {
        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));

        let keypair = Keypair::new();

        // Allow up to MAX_TX_ACCOUNT_LOCKS
        {
            let num_account_keys = MAX_TX_ACCOUNT_LOCKS;
            let mut account_keys: Vec<_> = (0..num_account_keys)
                .map(|_| Pubkey::new_unique())
                .collect();
            account_keys[0] = keypair.pubkey();
            let message = Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    ..MessageHeader::default()
                },
                account_keys,
                ..Message::default()
            };

            let txs = vec![new_sanitized_tx(&[&keypair], message, Hash::default())];
            let results = accounts.lock_accounts(
                txs.iter(),
                vec![Ok(()); txs.len()].into_iter(),
                MAX_TX_ACCOUNT_LOCKS,
                relax_intrabatch_account_locks,
                &|_| false,
                &|_| false,
            );
            assert_eq!(results, vec![Ok(())]);
            accounts.unlock_accounts(txs.iter().zip(&results));
        }

        // Disallow over MAX_TX_ACCOUNT_LOCKS
        {
            let num_account_keys = MAX_TX_ACCOUNT_LOCKS + 1;
            let mut account_keys: Vec<_> = (0..num_account_keys)
                .map(|_| Pubkey::new_unique())
                .collect();
            account_keys[0] = keypair.pubkey();
            let message = Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    ..MessageHeader::default()
                },
                account_keys,
                ..Message::default()
            };

            let txs = vec![new_sanitized_tx(&[&keypair], message, Hash::default())];
            let results = accounts.lock_accounts(
                txs.iter(),
                vec![Ok(()); txs.len()].into_iter(),
                MAX_TX_ACCOUNT_LOCKS,
                relax_intrabatch_account_locks,
                &|_| false,
                &|_| false,
            );
            assert_eq!(results[0], Err(TransactionError::TooManyAccountLocks));
        }
    }

    #[test_case(false; "old")]
    #[test_case(true; "simd83")]
    fn test_accounts_locks(relax_intrabatch_account_locks: bool) {
        let keypair0 = Keypair::new();
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let keypair3 = Keypair::new();

        let account0 = AccountSharedData::new(1, 0, &Pubkey::default());
        let account1 = AccountSharedData::new(2, 0, &Pubkey::default());
        let account2 = AccountSharedData::new(3, 0, &Pubkey::default());
        let account3 = AccountSharedData::new(4, 0, &Pubkey::default());

        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));
        accounts.store_for_tests(0, &keypair0.pubkey(), &account0);
        accounts.store_for_tests(0, &keypair1.pubkey(), &account1);
        accounts.store_for_tests(0, &keypair2.pubkey(), &account2);
        accounts.store_for_tests(0, &keypair3.pubkey(), &account3);

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair0.pubkey(), keypair1.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let tx = new_sanitized_tx(&[&keypair0], message, Hash::default());
        let results0 = accounts.lock_accounts(
            [tx.clone()].iter(),
            [Ok(())].into_iter(),
            MAX_TX_ACCOUNT_LOCKS,
            relax_intrabatch_account_locks,
            &|_| false,
            &|_| false,
        );

        assert_eq!(results0, vec![Ok(())]);
        assert!(accounts
            .account_locks
            .lock()
            .unwrap()
            .is_locked_readonly(&keypair1.pubkey()));

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair2.pubkey(), keypair1.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let tx0 = new_sanitized_tx(&[&keypair2], message, Hash::default());
        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair1.pubkey(), keypair3.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let tx1 = new_sanitized_tx(&[&keypair1], message, Hash::default());
        let txs = vec![tx0, tx1];
        let results1 = accounts.lock_accounts(
            txs.iter(),
            vec![Ok(()); txs.len()].into_iter(),
            MAX_TX_ACCOUNT_LOCKS,
            relax_intrabatch_account_locks,
            &|_| false,
            &|_| false,
        );
        assert_eq!(
            results1,
            vec![
                Ok(()), // Read-only account (keypair1) can be referenced multiple times
                Err(TransactionError::AccountInUse), // Read-only account (keypair1) cannot also be locked as writable
            ],
        );
        assert!(accounts
            .account_locks
            .lock()
            .unwrap()
            .is_locked_readonly(&keypair1.pubkey()));

        accounts.unlock_accounts(iter::once(&tx).zip(&results0));
        accounts.unlock_accounts(txs.iter().zip(&results1));
        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair1.pubkey(), keypair3.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let tx = new_sanitized_tx(&[&keypair1], message, Hash::default());
        let results2 = accounts.lock_accounts(
            [tx].iter(),
            [Ok(())].into_iter(),
            MAX_TX_ACCOUNT_LOCKS,
            relax_intrabatch_account_locks,
            &|_| false,
            &|_| false,
        );
        assert_eq!(
            results2,
            vec![Ok(())] // Now keypair1 account can be locked as writable
        );

        // Check that read-only lock with zero references is deleted
        assert!(!accounts
            .account_locks
            .lock()
            .unwrap()
            .is_locked_readonly(&keypair1.pubkey()));
    }

    #[test_case(false; "old")]
    #[test_case(true; "simd83")]
    fn test_accounts_locks_multithreaded(relax_intrabatch_account_locks: bool) {
        let counter = Arc::new(AtomicU64::new(0));
        let exit = Arc::new(AtomicBool::new(false));

        let keypair0 = Keypair::new();
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();

        let account0 = AccountSharedData::new(1, 0, &Pubkey::default());
        let account1 = AccountSharedData::new(2, 0, &Pubkey::default());
        let account2 = AccountSharedData::new(3, 0, &Pubkey::default());

        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));
        accounts.store_for_tests(0, &keypair0.pubkey(), &account0);
        accounts.store_for_tests(0, &keypair1.pubkey(), &account1);
        accounts.store_for_tests(0, &keypair2.pubkey(), &account2);

        let accounts_arc = Arc::new(accounts);

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let readonly_message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair0.pubkey(), keypair1.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let readonly_tx = new_sanitized_tx(&[&keypair0], readonly_message, Hash::default());

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let writable_message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair1.pubkey(), keypair2.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let writable_tx = new_sanitized_tx(&[&keypair1], writable_message, Hash::default());

        let counter_clone = counter.clone();
        let accounts_clone = accounts_arc.clone();
        let exit_clone = exit.clone();
        thread::spawn(move || loop {
            let txs = vec![writable_tx.clone()];
            let results = accounts_clone.clone().lock_accounts(
                txs.iter(),
                vec![Ok(()); txs.len()].into_iter(),
                MAX_TX_ACCOUNT_LOCKS,
                relax_intrabatch_account_locks,
                &|_| false,
                &|_| false,
            );
            for result in results.iter() {
                if result.is_ok() {
                    counter_clone.clone().fetch_add(1, Ordering::Release);
                }
            }
            accounts_clone.unlock_accounts(txs.iter().zip(&results));
            if exit_clone.clone().load(Ordering::Relaxed) {
                break;
            }
        });
        let counter_clone = counter;
        for _ in 0..5 {
            let txs = vec![readonly_tx.clone()];
            let results = accounts_arc.clone().lock_accounts(
                txs.iter(),
                vec![Ok(()); txs.len()].into_iter(),
                MAX_TX_ACCOUNT_LOCKS,
                relax_intrabatch_account_locks,
                &|_| false,
                &|_| false,
            );
            if results[0].is_ok() {
                let counter_value = counter_clone.clone().load(Ordering::Acquire);
                thread::sleep(time::Duration::from_millis(50));
                assert_eq!(counter_value, counter_clone.clone().load(Ordering::Acquire));
            }
            accounts_arc.unlock_accounts(txs.iter().zip(&results));
            thread::sleep(time::Duration::from_millis(50));
        }
        exit.store(true, Ordering::Relaxed);
    }

    #[test_case(false; "old")]
    #[test_case(true; "simd83")]
    fn test_demote_program_write_locks(relax_intrabatch_account_locks: bool) {
        let keypair0 = Keypair::new();
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let keypair3 = Keypair::new();

        let account0 = AccountSharedData::new(1, 0, &Pubkey::default());
        let account1 = AccountSharedData::new(2, 0, &Pubkey::default());
        let account2 = AccountSharedData::new(3, 0, &Pubkey::default());
        let account3 = AccountSharedData::new(4, 0, &Pubkey::default());

        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));
        accounts.store_for_tests(0, &keypair0.pubkey(), &account0);
        accounts.store_for_tests(0, &keypair1.pubkey(), &account1);
        accounts.store_for_tests(0, &keypair2.pubkey(), &account2);
        accounts.store_for_tests(0, &keypair3.pubkey(), &account3);

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            0, // All accounts marked as writable
            vec![keypair0.pubkey(), keypair1.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let tx = new_sanitized_tx(&[&keypair0], message, Hash::default());
        let results0 = accounts.lock_accounts(
            [tx].iter(),
            [Ok(())].into_iter(),
            MAX_TX_ACCOUNT_LOCKS,
            relax_intrabatch_account_locks,
            &|_| false,
            &|_| false,
        );

        assert!(results0[0].is_ok());
        // Instruction program-id account demoted to readonly
        assert!(accounts
            .account_locks
            .lock()
            .unwrap()
            .is_locked_readonly(&native_loader::id()));
        // Non-program accounts remain writable
        assert!(accounts
            .account_locks
            .lock()
            .unwrap()
            .is_locked_write(&keypair0.pubkey()));
        assert!(accounts
            .account_locks
            .lock()
            .unwrap()
            .is_locked_write(&keypair1.pubkey()));
    }

    impl Accounts {
        /// callers used to call store_uncached. But, this is not allowed anymore.
        pub fn store_for_tests(&self, slot: Slot, pubkey: &Pubkey, account: &AccountSharedData) {
            self.accounts_db.store_for_tests(slot, &[(pubkey, account)])
        }

        /// useful to adapt tests written prior to introduction of the write cache
        /// to use the write cache
        pub fn add_root_and_flush_write_cache(&self, slot: Slot) {
            self.add_root(slot);
            self.accounts_db.flush_accounts_cache_slot_for_tests(slot);
        }
    }

    #[test_case(false; "old")]
    #[test_case(true; "simd83")]
    fn test_accounts_locks_with_results(relax_intrabatch_account_locks: bool) {
        let keypair0 = Keypair::new();
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let keypair3 = Keypair::new();

        let account0 = AccountSharedData::new(1, 0, &Pubkey::default());
        let account1 = AccountSharedData::new(2, 0, &Pubkey::default());
        let account2 = AccountSharedData::new(3, 0, &Pubkey::default());
        let account3 = AccountSharedData::new(4, 0, &Pubkey::default());

        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));
        accounts.store_for_tests(0, &keypair0.pubkey(), &account0);
        accounts.store_for_tests(0, &keypair1.pubkey(), &account1);
        accounts.store_for_tests(0, &keypair2.pubkey(), &account2);
        accounts.store_for_tests(0, &keypair3.pubkey(), &account3);

        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair1.pubkey(), keypair0.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let tx0 = new_sanitized_tx(&[&keypair1], message, Hash::default());
        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair2.pubkey(), keypair0.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let tx1 = new_sanitized_tx(&[&keypair2], message, Hash::default());
        let instructions = vec![CompiledInstruction::new(2, &(), vec![0, 1])];
        let message = Message::new_with_compiled_instructions(
            1,
            0,
            2,
            vec![keypair3.pubkey(), keypair0.pubkey(), native_loader::id()],
            Hash::default(),
            instructions,
        );
        let tx2 = new_sanitized_tx(&[&keypair3], message, Hash::default());
        let txs = vec![tx0, tx1, tx2];

        let qos_results = vec![
            Ok(()),
            Err(TransactionError::WouldExceedMaxBlockCostLimit),
            Ok(()),
        ];

        let results = accounts.lock_accounts(
            txs.iter(),
            qos_results.into_iter(),
            MAX_TX_ACCOUNT_LOCKS,
            relax_intrabatch_account_locks,
            &|_| false,
            &|_| false,
        );

        assert_eq!(
            results,
            vec![
                Ok(()), // Read-only account (keypair0) can be referenced multiple times
                Err(TransactionError::WouldExceedMaxBlockCostLimit), // is not locked due to !qos_results[1].is_ok()
                Ok(()), // Read-only account (keypair0) can be referenced multiple times
            ],
        );

        // verify that keypair0 read-only locked
        assert!(accounts
            .account_locks
            .lock()
            .unwrap()
            .is_locked_readonly(&keypair0.pubkey()));
        // verify that keypair2 (for tx1) is not write-locked
        assert!(!accounts
            .account_locks
            .lock()
            .unwrap()
            .is_locked_write(&keypair2.pubkey()));
    }

    #[test_case(false; "old")]
    #[test_case(true; "simd83")]
    fn test_accounts_locks_intrabatch_conflicts(relax_intrabatch_account_locks: bool) {
        let pubkey = Pubkey::new_unique();
        let account_data = AccountSharedData::new(1, 0, &Pubkey::default());
        let accounts_db = Arc::new(AccountsDb::new_single_for_tests());
        accounts_db.store_for_tests(
            0,
            &[
                (&Pubkey::default(), &account_data),
                (&pubkey, &account_data),
            ],
        );

        let r_tx = sanitized_tx_from_metas(vec![AccountMeta {
            pubkey,
            is_writable: false,
            is_signer: false,
        }]);

        let w_tx = sanitized_tx_from_metas(vec![AccountMeta {
            pubkey,
            is_writable: true,
            is_signer: false,
        }]);

        // one w tx alone always works
        let accounts = Accounts::new(accounts_db.clone());
        let results = accounts.lock_accounts(
            [w_tx.clone()].iter(),
            [Ok(())].into_iter(),
            MAX_TX_ACCOUNT_LOCKS,
            relax_intrabatch_account_locks,
            &|_| false,
            &|_| false,
        );

        assert_eq!(results, vec![Ok(())]);

        // wr conflict cross-batch always fails
        let results = accounts.lock_accounts(
            [r_tx.clone()].iter(),
            [Ok(())].into_iter(),
            MAX_TX_ACCOUNT_LOCKS,
            relax_intrabatch_account_locks,
            &|_| false,
            &|_| false,
        );

        assert_eq!(results, vec![Err(TransactionError::AccountInUse)]);

        // ww conflict cross-batch always fails
        let results = accounts.lock_accounts(
            [w_tx.clone()].iter(),
            [Ok(())].into_iter(),
            MAX_TX_ACCOUNT_LOCKS,
            relax_intrabatch_account_locks,
            &|_| false,
            &|_| false,
        );

        assert_eq!(results, vec![Err(TransactionError::AccountInUse)]);

        // wr conflict in-batch succeeds or fails based on feature
        let accounts = Accounts::new(accounts_db.clone());
        let results = accounts.lock_accounts(
            [w_tx.clone(), r_tx.clone()].iter(),
            [Ok(()), Ok(())].into_iter(),
            MAX_TX_ACCOUNT_LOCKS,
            relax_intrabatch_account_locks,
            &|_| false,
            &|_| false,
        );

        if relax_intrabatch_account_locks {
            assert_eq!(results, vec![Ok(()), Ok(())]);
        } else {
            assert_eq!(results, vec![Ok(()), Err(TransactionError::AccountInUse)]);
        }

        // ww conflict in-batch succeeds or fails based on feature
        let accounts = Accounts::new(accounts_db.clone());
        let results = accounts.lock_accounts(
            [w_tx.clone(), r_tx.clone()].iter(),
            [Ok(()), Ok(())].into_iter(),
            MAX_TX_ACCOUNT_LOCKS,
            relax_intrabatch_account_locks,
            &|_| false,
            &|_| false,
        );

        if relax_intrabatch_account_locks {
            assert_eq!(results, vec![Ok(()), Ok(())]);
        } else {
            assert_eq!(results, vec![Ok(()), Err(TransactionError::AccountInUse)]);
        }
    }

    #[test]
    fn huge_clean() {
        solana_logger::setup();
        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));
        let mut old_pubkey = Pubkey::default();
        let zero_account = AccountSharedData::new(0, 0, AccountSharedData::default().owner());
        info!("storing..");
        for i in 0..2_000 {
            let pubkey = solana_pubkey::new_rand();
            let account = AccountSharedData::new(i + 1, 0, AccountSharedData::default().owner());
            accounts.store_for_tests(i, &pubkey, &account);
            accounts.store_for_tests(i, &old_pubkey, &zero_account);
            old_pubkey = pubkey;
            accounts.add_root_and_flush_write_cache(i);

            if i % 1_000 == 0 {
                info!("  store {}", i);
            }
        }
        info!("done..cleaning..");
        accounts.accounts_db.clean_accounts_for_tests();
    }

    #[test]
    fn test_load_largest_accounts() {
        let accounts_db = AccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(accounts_db));

        /* This test assumes pubkey0 < pubkey1 < pubkey2.
         * But the keys created with new_unique() does not guarantee this
         * order because of the endianness.  new_unique() calls add 1 at each
         * key generaration as the little endian integer.  A pubkey stores its
         * value in a 32-byte array bytes, and its eq-partial trait considers
         * the lower-address bytes more significant, which is the big-endian
         * order.
         * So, sort first to ensure the order assumption holds.
         */
        let mut keys = vec![];
        for _idx in 0..3 {
            keys.push(Pubkey::new_unique());
        }
        keys.sort();
        let pubkey2 = keys.pop().unwrap();
        let pubkey1 = keys.pop().unwrap();
        let pubkey0 = keys.pop().unwrap();
        let account0 = AccountSharedData::new(42, 0, &Pubkey::default());
        accounts.store_for_tests(0, &pubkey0, &account0);
        let account1 = AccountSharedData::new(42, 0, &Pubkey::default());
        accounts.store_for_tests(0, &pubkey1, &account1);
        let account2 = AccountSharedData::new(41, 0, &Pubkey::default());
        accounts.store_for_tests(0, &pubkey2, &account2);

        let ancestors = vec![(0, 0)].into_iter().collect();
        let all_pubkeys: HashSet<_> = vec![pubkey0, pubkey1, pubkey2].into_iter().collect();

        // num == 0 should always return empty set
        let bank_id = 0;
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    0,
                    &HashSet::new(),
                    AccountAddressFilter::Exclude,
                    false
                )
                .unwrap(),
            vec![]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    0,
                    &all_pubkeys,
                    AccountAddressFilter::Include,
                    false
                )
                .unwrap(),
            vec![]
        );

        // list should be sorted by balance, then pubkey, descending
        assert!(pubkey1 > pubkey0);
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    1,
                    &HashSet::new(),
                    AccountAddressFilter::Exclude,
                    false
                )
                .unwrap(),
            vec![(pubkey1, 42)]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    2,
                    &HashSet::new(),
                    AccountAddressFilter::Exclude,
                    false
                )
                .unwrap(),
            vec![(pubkey1, 42), (pubkey0, 42)]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    3,
                    &HashSet::new(),
                    AccountAddressFilter::Exclude,
                    false
                )
                .unwrap(),
            vec![(pubkey1, 42), (pubkey0, 42), (pubkey2, 41)]
        );

        // larger num should not affect results
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    6,
                    &HashSet::new(),
                    AccountAddressFilter::Exclude,
                    false
                )
                .unwrap(),
            vec![(pubkey1, 42), (pubkey0, 42), (pubkey2, 41)]
        );

        // AccountAddressFilter::Exclude should exclude entry
        let exclude1: HashSet<_> = vec![pubkey1].into_iter().collect();
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    1,
                    &exclude1,
                    AccountAddressFilter::Exclude,
                    false
                )
                .unwrap(),
            vec![(pubkey0, 42)]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    2,
                    &exclude1,
                    AccountAddressFilter::Exclude,
                    false
                )
                .unwrap(),
            vec![(pubkey0, 42), (pubkey2, 41)]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    3,
                    &exclude1,
                    AccountAddressFilter::Exclude,
                    false
                )
                .unwrap(),
            vec![(pubkey0, 42), (pubkey2, 41)]
        );

        // AccountAddressFilter::Include should limit entries
        let include1_2: HashSet<_> = vec![pubkey1, pubkey2].into_iter().collect();
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    1,
                    &include1_2,
                    AccountAddressFilter::Include,
                    false
                )
                .unwrap(),
            vec![(pubkey1, 42)]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    2,
                    &include1_2,
                    AccountAddressFilter::Include,
                    false
                )
                .unwrap(),
            vec![(pubkey1, 42), (pubkey2, 41)]
        );
        assert_eq!(
            accounts
                .load_largest_accounts(
                    &ancestors,
                    bank_id,
                    3,
                    &include1_2,
                    AccountAddressFilter::Include,
                    false
                )
                .unwrap(),
            vec![(pubkey1, 42), (pubkey2, 41)]
        );
    }

    fn zero_len_account_size() -> usize {
        std::mem::size_of::<AccountSharedData>() + std::mem::size_of::<Pubkey>()
    }

    #[test]
    fn test_calc_scan_result_size() {
        for len in 0..3 {
            assert_eq!(
                Accounts::calc_scan_result_size(&AccountSharedData::new(
                    0,
                    len,
                    &Pubkey::default()
                )),
                zero_len_account_size() + len
            );
        }
    }

    #[test]
    fn test_maybe_abort_scan() {
        assert!(Accounts::maybe_abort_scan(ScanResult::Ok(vec![]), &ScanConfig::default()).is_ok());
        assert!(Accounts::maybe_abort_scan(
            ScanResult::Ok(vec![]),
            &ScanConfig::new(ScanOrder::Sorted)
        )
        .is_ok());
        let config = ScanConfig::new(ScanOrder::Sorted).recreate_with_abort();
        assert!(Accounts::maybe_abort_scan(ScanResult::Ok(vec![]), &config).is_ok());
        config.abort();
        assert!(Accounts::maybe_abort_scan(ScanResult::Ok(vec![]), &config).is_err());
    }

    #[test]
    fn test_accumulate_and_check_scan_result_size() {
        for (account, byte_limit_for_scan, result) in [
            (AccountSharedData::default(), zero_len_account_size(), false),
            (
                AccountSharedData::new(0, 1, &Pubkey::default()),
                zero_len_account_size(),
                true,
            ),
            (
                AccountSharedData::new(0, 2, &Pubkey::default()),
                zero_len_account_size() + 3,
                false,
            ),
        ] {
            let sum = AtomicUsize::default();
            assert_eq!(
                result,
                Accounts::accumulate_and_check_scan_result_size(
                    &sum,
                    &account,
                    &Some(byte_limit_for_scan)
                )
            );
            // calling a second time should accumulate above the threshold
            assert!(Accounts::accumulate_and_check_scan_result_size(
                &sum,
                &account,
                &Some(byte_limit_for_scan)
            ));
            assert!(!Accounts::accumulate_and_check_scan_result_size(
                &sum, &account, &None
            ));
        }
    }
}
