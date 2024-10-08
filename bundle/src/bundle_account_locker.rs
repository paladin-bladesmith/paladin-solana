//! Handles pre-locking bundle accounts so that accounts bundles touch can be reserved ahead
// of time for execution. Also, ensures that ALL accounts mentioned across a bundle are locked
// to avoid race conditions between BundleStage and BankingStage.
//
// For instance, imagine a bundle with three transactions and the set of accounts for each transaction
// is: {{A, B}, {B, C}, {C, D}}. We need to lock A, B, and C even though only one is executed at a time.
// Imagine BundleStage is in the middle of processing {C, D} and we didn't have a lock on accounts {A, B, C}.
// In this situation, there's a chance that BankingStage can process a transaction containing A or B
// and commit the results before the bundle completes. By the time the bundle commits the new account
// state for {A, B, C}, A and B would be incorrect and the entries containing the bundle would be
// replayed improperly and that leader would have produced an invalid block.
use {
    hashbrown::{
        hash_map::{Entry, EntryRef},
        HashMap,
    },
    log::warn,
    solana_runtime::bank::Bank,
    solana_sdk::{bundle::SanitizedBundle, pubkey::Pubkey, transaction::TransactionAccountLocks},
    std::{
        collections::HashSet,
        sync::{Arc, Mutex, MutexGuard},
    },
    thiserror::Error,
};

#[derive(Clone, Error, Debug)]
pub enum BundleAccountLockerError {
    #[error("locking error")]
    LockingError,
}

#[derive(Clone, Error, Debug)]
#[error("Failed to get exlcusivity")]
pub struct ExclusivityError;

pub type BundleAccountLockerResult<T> = Result<T, BundleAccountLockerError>;

pub struct LockedBundle<'a, 'b> {
    bundle_account_locker: &'a BundleAccountLocker,
    sanitized_bundle: &'b SanitizedBundle,
    bank: Arc<Bank>,
    has_exclusivity: bool,
}

impl<'a, 'b> LockedBundle<'a, 'b> {
    fn new(
        bundle_account_locker: &'a BundleAccountLocker,
        sanitized_bundle: &'b SanitizedBundle,
        bank: &Arc<Bank>,
    ) -> Self {
        Self {
            bundle_account_locker,
            sanitized_bundle,
            bank: bank.clone(),
            has_exclusivity: false,
        }
    }

    pub fn sanitized_bundle(&self) -> &SanitizedBundle {
        self.sanitized_bundle
    }

    pub fn try_make_exclusive(&mut self) -> Result<(), ExclusivityError> {
        assert!(
            !self.has_exclusivity,
            "Duplicate calls to try_make_exclusive"
        );

        // Compute read & write locks.
        let (read_locks, write_locks) =
            BundleAccountLocker::get_read_write_locks(self.sanitized_bundle, &self.bank)
                .expect("Existence of locked bundle implies this cannot fail");

        // Take lock on bundle_account_locker.
        let mut lock = self.bundle_account_locker.account_locks.lock().unwrap();

        // No read accounts have write locks.
        for key in read_locks.keys() {
            match lock.exclusive_locks.get(key) {
                Some(Lock::Read(_)) | None => {}
                Some(Lock::Write) => return Err(ExclusivityError),
            }
        }

        // No write accounts have any lock.
        for key in write_locks.keys() {
            if lock.exclusive_locks.contains_key(key) {
                return Err(ExclusivityError);
            }
        }

        // Insert our locks.
        for key in read_locks
            .keys()
            // NB: It is possible to have overlapping read & write locks because a bundle contains
            // multiple transactions.
            .filter(|key| !write_locks.contains_key(*key))
        {
            let lock = lock.exclusive_locks.entry_ref(key).or_insert(Lock::Read(0));
            match lock {
                Lock::Read(count) => *count += 1,
                Lock::Write => unreachable!(),
            }
        }
        for key in write_locks.keys() {
            assert!(lock.exclusive_locks.insert(*key, Lock::Write).is_none());
        }
        self.has_exclusivity = true;

        Ok(())
    }
}

// Automatically unlock bundle accounts when destructed
impl<'a, 'b> Drop for LockedBundle<'a, 'b> {
    fn drop(&mut self) {
        let _ = self.bundle_account_locker.unlock_bundle_accounts(
            self.sanitized_bundle,
            &self.bank,
            self.has_exclusivity,
        );
    }
}

#[derive(Default, Clone)]
pub struct BundleAccountLocks {
    read_locks: HashMap<Pubkey, u64>,
    write_locks: HashMap<Pubkey, u64>,
    exclusive_locks: HashMap<Pubkey, Lock>,
}

impl BundleAccountLocks {
    pub fn read_locks(&self) -> HashSet<Pubkey> {
        self.read_locks.keys().cloned().collect()
    }

    pub fn write_locks(&self) -> HashSet<Pubkey> {
        self.write_locks.keys().cloned().collect()
    }

    pub fn lock_accounts(
        &mut self,
        read_locks: HashMap<Pubkey, u64>,
        write_locks: HashMap<Pubkey, u64>,
    ) {
        for (acc, count) in read_locks {
            *self.read_locks.entry(acc).or_insert(0) += count;
        }
        for (acc, count) in write_locks {
            *self.write_locks.entry(acc).or_insert(0) += count;
        }
    }

    pub fn unlock_accounts(
        &mut self,
        read_locks: HashMap<Pubkey, u64>,
        write_locks: HashMap<Pubkey, u64>,
    ) {
        for (acc, count) in read_locks {
            if let Entry::Occupied(mut entry) = self.read_locks.entry(acc) {
                let val = entry.get_mut();
                *val = val.saturating_sub(count);
                if entry.get() == &0 {
                    let _ = entry.remove();
                }
            } else {
                warn!("error unlocking read-locked account, account: {:?}", acc);
            }
        }
        for (acc, count) in write_locks {
            if let Entry::Occupied(mut entry) = self.write_locks.entry(acc) {
                let val = entry.get_mut();
                *val = val.saturating_sub(count);
                if entry.get() == &0 {
                    let _ = entry.remove();
                }
            } else {
                warn!("error unlocking write-locked account, account: {:?}", acc);
            }
        }
    }
}

#[derive(Clone)]
enum Lock {
    Write,
    Read(u64),
}

#[derive(Clone, Default)]
pub struct BundleAccountLocker {
    account_locks: Arc<Mutex<BundleAccountLocks>>,
}

impl BundleAccountLocker {
    /// used in BankingStage during TransactionBatch construction to ensure that BankingStage
    /// doesn't lock anything currently locked in the BundleAccountLocker
    pub fn read_locks(&self) -> HashSet<Pubkey> {
        self.account_locks.lock().unwrap().read_locks()
    }

    /// used in BankingStage during TransactionBatch construction to ensure that BankingStage
    /// doesn't lock anything currently locked in the BundleAccountLocker
    pub fn write_locks(&self) -> HashSet<Pubkey> {
        self.account_locks.lock().unwrap().write_locks()
    }

    /// used in BankingStage during TransactionBatch construction to ensure that BankingStage
    /// doesn't lock anything currently locked in the BundleAccountLocker
    pub fn account_locks(&self) -> MutexGuard<BundleAccountLocks> {
        self.account_locks.lock().unwrap()
    }

    /// Prepares a locked bundle and returns a LockedBundle containing locked accounts.
    /// When a LockedBundle is dropped, the accounts are automatically unlocked
    pub fn prepare_locked_bundle<'a, 'b>(
        &'a self,
        sanitized_bundle: &'b SanitizedBundle,
        bank: &Arc<Bank>,
    ) -> BundleAccountLockerResult<LockedBundle<'a, 'b>> {
        let (read_locks, write_locks) = Self::get_read_write_locks(sanitized_bundle, bank)?;

        self.account_locks
            .lock()
            .unwrap()
            .lock_accounts(read_locks, write_locks);
        Ok(LockedBundle::new(self, sanitized_bundle, bank))
    }

    /// Unlocks bundle accounts. Note that LockedBundle::drop will auto-drop the bundle account locks
    fn unlock_bundle_accounts(
        &self,
        sanitized_bundle: &SanitizedBundle,
        bank: &Bank,
        has_exclusivity: bool,
    ) -> BundleAccountLockerResult<()> {
        let (read_locks, write_locks) = Self::get_read_write_locks(sanitized_bundle, bank)?;

        let mut lock = self.account_locks.lock().unwrap();

        // Remove exclusive keys.
        if has_exclusivity {
            for key in read_locks
                .keys()
                .filter(|key| !write_locks.contains_key(*key))
            {
                match lock.exclusive_locks.entry_ref(key) {
                    EntryRef::Occupied(mut entry) => {
                        let count = match entry.get_mut() {
                            Lock::Read(count) => count,
                            Lock::Write => unreachable!(),
                        };

                        *count -= 1;

                        if *count == 0 {
                            entry.remove();
                        }
                    }
                    EntryRef::Vacant(_) => unreachable!(),
                }
            }

            for key in write_locks.keys() {
                let removed = lock.exclusive_locks.remove(key).unwrap();
                assert!(matches!(removed, Lock::Write));
            }
        }

        // Remove interest.
        lock.unlock_accounts(read_locks, write_locks);

        Ok(())
    }

    /// Returns the read and write locks for this bundle
    /// Each lock type contains a HashMap which maps Pubkey to number of locks held
    fn get_read_write_locks(
        bundle: &SanitizedBundle,
        bank: &Bank,
    ) -> BundleAccountLockerResult<(HashMap<Pubkey, u64>, HashMap<Pubkey, u64>)> {
        let transaction_locks: Vec<TransactionAccountLocks> = bundle
            .transactions
            .iter()
            .filter_map(|tx| {
                tx.get_account_locks(bank.get_transaction_account_lock_limit())
                    .ok()
            })
            .collect();

        if transaction_locks.len() != bundle.transactions.len() {
            return Err(BundleAccountLockerError::LockingError);
        }

        let bundle_read_locks = transaction_locks
            .iter()
            .flat_map(|tx| tx.readonly.iter().map(|a| **a));
        let bundle_read_locks =
            bundle_read_locks
                .into_iter()
                .fold(HashMap::new(), |mut map, acc| {
                    *map.entry(acc).or_insert(0) += 1;
                    map
                });

        let bundle_write_locks = transaction_locks
            .iter()
            .flat_map(|tx| tx.writable.iter().map(|a| **a));
        let bundle_write_locks =
            bundle_write_locks
                .into_iter()
                .fold(HashMap::new(), |mut map, acc| {
                    *map.entry(acc).or_insert(0) += 1;
                    map
                });

        Ok((bundle_read_locks, bundle_write_locks))
    }
}
