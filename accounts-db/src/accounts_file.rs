#[cfg(feature = "dev-context-only-utils")]
use crate::append_vec::StoredAccountMeta;
use {
    crate::{
        account_info::{AccountInfo, Offset},
        account_storage::stored_account_info::{StoredAccountInfo, StoredAccountInfoWithoutData},
        accounts_db::AccountsFileId,
        accounts_update_notifier_interface::AccountForGeyser,
        append_vec::{AppendVec, AppendVecError, IndexInfo},
        storable_accounts::StorableAccounts,
        tiered_storage::{
            error::TieredStorageError, hot::HOT_FORMAT, index::IndexOffset, TieredStorage,
        },
    },
    solana_account::{AccountSharedData, ReadableAccount as _},
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::{
        mem,
        path::{Path, PathBuf},
    },
    thiserror::Error,
};

// Data placement should be aligned at the next boundary. Without alignment accessing the memory may
// crash on some architectures.
pub const ALIGN_BOUNDARY_OFFSET: usize = mem::size_of::<u64>();
#[macro_export]
macro_rules! u64_align {
    ($addr: expr) => {
        ($addr + ($crate::accounts_file::ALIGN_BOUNDARY_OFFSET - 1))
            & !($crate::accounts_file::ALIGN_BOUNDARY_OFFSET - 1)
    };
}

#[derive(Error, Debug)]
/// An enum for AccountsFile related errors.
pub enum AccountsFileError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("AppendVecError: {0}")]
    AppendVecError(#[from] AppendVecError),

    #[error("TieredStorageError: {0}")]
    TieredStorageError(#[from] TieredStorageError),
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum MatchAccountOwnerError {
    #[error("The account owner does not match with the provided list")]
    NoMatch,
    #[error("Unable to load the account")]
    UnableToLoad,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum StorageAccess {
    /// storages should be accessed by Mmap
    Mmap,
    /// storages should be accessed by File I/O
    /// ancient storages are created by 1-shot write to pack multiple accounts together more efficiently with new formats
    #[default]
    File,
}

pub type Result<T> = std::result::Result<T, AccountsFileError>;

#[derive(Debug)]
/// An enum for accessing an accounts file which can be implemented
/// under different formats.
pub enum AccountsFile {
    AppendVec(AppendVec),
    TieredStorage(TieredStorage),
}

impl AccountsFile {
    /// Create an AccountsFile instance from the specified path.
    ///
    /// The second element of the returned tuple is the number of accounts in the
    /// accounts file.
    pub fn new_from_file(
        path: impl Into<PathBuf>,
        current_len: usize,
        storage_access: StorageAccess,
    ) -> Result<(Self, usize)> {
        let (av, num_accounts) = AppendVec::new_from_file(path, current_len, storage_access)?;
        Ok((Self::AppendVec(av), num_accounts))
    }

    /// Creates a new AccountsFile for the underlying storage at `path`
    ///
    /// This version of `new()` may only be called when reconstructing storages as part of startup.
    /// It trusts the snapshot's value for `current_len`, and relies on later index generation or
    /// accounts verification to ensure it is valid.
    pub fn new_for_startup(
        path: impl Into<PathBuf>,
        current_len: usize,
        storage_access: StorageAccess,
    ) -> Result<Self> {
        let av = AppendVec::new_for_startup(path, current_len, storage_access)?;
        Ok(Self::AppendVec(av))
    }

    /// true if this storage can possibly be appended to (independent of capacity check)
    //
    // NOTE: Only used by ancient append vecs "append" method, which is test-only now.
    #[cfg(test)]
    pub(crate) fn can_append(&self) -> bool {
        match self {
            Self::AppendVec(av) => av.can_append(),
            // once created, tiered storages cannot be appended to
            Self::TieredStorage(_) => false,
        }
    }

    /// if storage is not readonly, reopen another instance that is read only
    pub(crate) fn reopen_as_readonly(&self) -> Option<Self> {
        match self {
            Self::AppendVec(av) => av.reopen_as_readonly().map(Self::AppendVec),
            Self::TieredStorage(_) => None,
        }
    }

    /// Return the total number of bytes of the zero lamport single ref accounts in the storage.
    /// Those bytes are "dead" and can be shrunk away.
    pub(crate) fn dead_bytes_due_to_zero_lamport_single_ref(&self, count: usize) -> usize {
        match self {
            Self::AppendVec(av) => av.dead_bytes_due_to_zero_lamport_single_ref(count),
            Self::TieredStorage(ts) => ts.dead_bytes_due_to_zero_lamport_single_ref(count),
        }
    }

    pub fn flush(&self) -> Result<()> {
        match self {
            Self::AppendVec(av) => av.flush(),
            Self::TieredStorage(_) => Ok(()),
        }
    }

    pub fn reset(&self) {
        match self {
            Self::AppendVec(av) => av.reset(),
            Self::TieredStorage(_) => {}
        }
    }

    pub fn remaining_bytes(&self) -> u64 {
        match self {
            Self::AppendVec(av) => av.remaining_bytes(),
            Self::TieredStorage(ts) => ts.capacity().saturating_sub(ts.len() as u64),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::AppendVec(av) => av.len(),
            Self::TieredStorage(ts) => ts.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::AppendVec(av) => av.is_empty(),
            Self::TieredStorage(ts) => ts.is_empty(),
        }
    }

    pub fn capacity(&self) -> u64 {
        match self {
            Self::AppendVec(av) => av.capacity(),
            Self::TieredStorage(ts) => ts.capacity(),
        }
    }

    pub fn file_name(slot: Slot, id: AccountsFileId) -> String {
        format!("{slot}.{id}")
    }

    /// calls `callback` with the account located at the specified index offset.
    pub fn get_stored_account_callback<Ret>(
        &self,
        offset: usize,
        callback: impl for<'local> FnMut(StoredAccountInfo<'local>) -> Ret,
    ) -> Option<Ret> {
        match self {
            Self::AppendVec(av) => av.get_stored_account_callback(offset, callback),
            Self::TieredStorage(ts) => {
                // Note: The conversion here is needed as the AccountsDB currently
                // assumes all offsets are multiple of 8 while TieredStorage uses
                // IndexOffset that is equivalent to AccountInfo::reduced_offset.
                let index_offset = IndexOffset(AccountInfo::get_reduced_offset(offset));
                ts.reader()?
                    .get_stored_account_callback(index_offset, callback)
                    .ok()?
            }
        }
    }

    /// calls `callback` with the account located at the specified index offset.
    ///
    /// Prefer get_stored_account_callback() when possible, as it does not contain file format
    /// implementation details, and thus potentially can read less and be faster.
    #[cfg(feature = "dev-context-only-utils")]
    pub fn get_stored_account_meta_callback<Ret>(
        &self,
        offset: usize,
        callback: impl for<'local> FnMut(StoredAccountMeta<'local>) -> Ret,
    ) -> Option<Ret> {
        match self {
            Self::AppendVec(av) => av.get_stored_account_meta_callback(offset, callback),
            Self::TieredStorage(_) => {
                unimplemented!("StoredAccountMeta is only implemented for AppendVec")
            }
        }
    }

    /// return an `AccountSharedData` for an account at `offset`, if any.  Otherwise return None.
    pub(crate) fn get_account_shared_data(&self, offset: usize) -> Option<AccountSharedData> {
        match self {
            Self::AppendVec(av) => av.get_account_shared_data(offset),
            Self::TieredStorage(ts) => {
                // Note: The conversion here is needed as the AccountsDB currently
                // assumes all offsets are multiple of 8 while TieredStorage uses
                // IndexOffset that is equivalent to AccountInfo::reduced_offset.
                let index_offset = IndexOffset(AccountInfo::get_reduced_offset(offset));
                ts.reader()?.get_account_shared_data(index_offset).ok()?
            }
        }
    }

    /// returns an `IndexInfo` for an account at `offset`, if any.  Otherwise, return None.
    ///
    /// Only intended to be used with the accounts index.
    pub(crate) fn get_account_index_info(&self, offset: usize) -> Option<IndexInfo> {
        match self {
            Self::AppendVec(av) => av.get_account_index_info(offset),
            Self::TieredStorage(ts) => {
                // Note: The conversion here is needed as the AccountsDB currently
                // assumes all offsets are multiple of 8 while TieredStorage uses
                // IndexOffset that is equivalent to AccountInfo::reduced_offset.
                let index_offset = IndexOffset(AccountInfo::get_reduced_offset(offset));
                ts.reader()?.get_account_index_info(index_offset).ok()?
            }
        }
    }

    pub fn account_matches_owners(
        &self,
        offset: usize,
        owners: &[Pubkey],
    ) -> std::result::Result<usize, MatchAccountOwnerError> {
        match self {
            Self::AppendVec(av) => av.account_matches_owners(offset, owners),
            // Note: The conversion here is needed as the AccountsDB currently
            // assumes all offsets are multiple of 8 while TieredStorage uses
            // IndexOffset that is equivalent to AccountInfo::reduced_offset.
            Self::TieredStorage(ts) => {
                let Some(reader) = ts.reader() else {
                    return Err(MatchAccountOwnerError::UnableToLoad);
                };
                reader.account_matches_owners(
                    IndexOffset(AccountInfo::get_reduced_offset(offset)),
                    owners,
                )
            }
        }
    }

    /// Return the path of the underlying account file.
    pub fn path(&self) -> &Path {
        match self {
            Self::AppendVec(av) => av.path(),
            Self::TieredStorage(ts) => ts.path(),
        }
    }

    /// Iterate over all accounts and call `callback` with each account.
    ///
    /// `callback` parameters:
    /// * Offset: the offset within the file of this account
    /// * StoredAccountInfoWithoutData: the account itself, without account data
    ///
    /// Note that account data is not read/passed to the callback.
    pub fn scan_accounts_without_data(
        &self,
        callback: impl for<'local> FnMut(Offset, StoredAccountInfoWithoutData<'local>),
    ) {
        match self {
            Self::AppendVec(av) => av.scan_accounts_without_data(callback),
            Self::TieredStorage(ts) => {
                if let Some(reader) = ts.reader() {
                    _ = reader.scan_accounts_without_data(callback);
                }
            }
        }
    }

    /// Iterate over all accounts and call `callback` with each account.
    ///
    /// `callback` parameters:
    /// * Offset: the offset within the file of this account
    /// * StoredAccountInfo: the account itself, with account data
    ///
    /// Prefer scan_accounts_without_data() when account data is not needed,
    /// as it can potentially read less and be faster.
    pub fn scan_accounts(
        &self,
        callback: impl for<'local> FnMut(Offset, StoredAccountInfo<'local>),
    ) {
        match self {
            Self::AppendVec(av) => av.scan_accounts(callback),
            Self::TieredStorage(ts) => {
                if let Some(reader) = ts.reader() {
                    _ = reader.scan_accounts(callback);
                }
            }
        }
    }

    /// Iterate over all accounts and call `callback` with each account.
    ///
    /// Prefer scan_accounts() when possible, as it does not contain file format
    /// implementation details, and thus potentially can read less and be faster.
    #[cfg(feature = "dev-context-only-utils")]
    pub fn scan_accounts_stored_meta(
        &self,
        callback: impl for<'local> FnMut(StoredAccountMeta<'local>),
    ) {
        match self {
            Self::AppendVec(av) => av.scan_accounts_stored_meta(callback),
            Self::TieredStorage(_) => {
                unimplemented!("StoredAccountMeta is only implemented for AppendVec")
            }
        }
    }

    /// Iterate over all accounts and call `callback` with each account.
    /// Only intended to be used by Geyser.
    pub fn scan_accounts_for_geyser(
        &self,
        mut callback: impl for<'local> FnMut(AccountForGeyser<'local>),
    ) {
        self.scan_accounts(|_offset, account| {
            let account_for_geyser = AccountForGeyser {
                pubkey: account.pubkey(),
                lamports: account.lamports(),
                owner: account.owner(),
                executable: account.executable(),
                rent_epoch: account.rent_epoch(),
                data: account.data(),
            };
            callback(account_for_geyser)
        })
    }

    /// Calculate the amount of storage required for an account with the passed
    /// in data_len
    pub(crate) fn calculate_stored_size(&self, data_len: usize) -> usize {
        match self {
            Self::AppendVec(av) => av.calculate_stored_size(data_len),
            Self::TieredStorage(ts) => ts
                .reader()
                .expect("Reader must be initalized as stored size is specific to format")
                .calculate_stored_size(data_len),
        }
    }

    /// for each offset in `sorted_offsets`, get the data size
    pub(crate) fn get_account_data_lens(&self, sorted_offsets: &[usize]) -> Vec<usize> {
        match self {
            Self::AppendVec(av) => av.get_account_data_lens(sorted_offsets),
            Self::TieredStorage(ts) => ts
                .reader()
                .and_then(|reader| reader.get_account_data_lens(sorted_offsets).ok())
                .unwrap_or_default(),
        }
    }

    /// iterate over all entries to put in index
    pub(crate) fn scan_index(&self, callback: impl FnMut(IndexInfo)) {
        match self {
            Self::AppendVec(av) => av.scan_index(callback),
            Self::TieredStorage(ts) => {
                if let Some(reader) = ts.reader() {
                    _ = reader.scan_index(callback);
                }
            }
        }
    }

    /// iterate over all pubkeys
    pub fn scan_pubkeys(&self, callback: impl FnMut(&Pubkey)) {
        match self {
            Self::AppendVec(av) => av.scan_pubkeys(callback),
            Self::TieredStorage(ts) => {
                if let Some(reader) = ts.reader() {
                    _ = reader.scan_pubkeys(callback);
                }
            }
        }
    }

    /// Copy each account metadata, account and hash to the internal buffer.
    /// If there is no room to write the first entry, None is returned.
    /// Otherwise, returns the starting offset of each account metadata.
    /// Plus, the final return value is the offset where the next entry would be appended.
    /// So, return.len() is 1 + (number of accounts written)
    /// After each account is appended, the internal `current_len` is updated
    /// and will be available to other threads.
    pub fn append_accounts<'a>(
        &self,
        accounts: &impl StorableAccounts<'a>,
        skip: usize,
    ) -> Option<StoredAccountsInfo> {
        match self {
            Self::AppendVec(av) => av.append_accounts(accounts, skip),
            // Note: The conversion here is needed as the AccountsDB currently
            // assumes all offsets are multiple of 8 while TieredStorage uses
            // IndexOffset that is equivalent to AccountInfo::reduced_offset.
            Self::TieredStorage(ts) => ts
                .write_accounts(accounts, skip, &HOT_FORMAT)
                .map(|mut stored_accounts_info| {
                    stored_accounts_info.offsets.iter_mut().for_each(|offset| {
                        *offset = AccountInfo::reduced_offset_to_offset(*offset as u32);
                    });
                    stored_accounts_info
                })
                .ok(),
        }
    }

    /// Returns the way to access this accounts file when archiving
    pub fn internals_for_archive(&self) -> InternalsForArchive {
        match self {
            Self::AppendVec(av) => av.internals_for_archive(),
            Self::TieredStorage(ts) => InternalsForArchive::Mmap(
                ts.reader()
                    .expect("must be a reader when archiving")
                    .data_for_archive(),
            ),
        }
    }
}

/// An enum that creates AccountsFile instance with the specified format.
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub enum AccountsFileProvider {
    #[default]
    AppendVec,
    HotStorage,
}

impl AccountsFileProvider {
    pub fn new_writable(&self, path: impl Into<PathBuf>, file_size: u64) -> AccountsFile {
        match self {
            Self::AppendVec => {
                AccountsFile::AppendVec(AppendVec::new(path, true, file_size as usize))
            }
            Self::HotStorage => AccountsFile::TieredStorage(TieredStorage::new_writable(path)),
        }
    }
}

/// The access method to use when archiving an AccountsFile
#[derive(Debug)]
pub enum InternalsForArchive<'a> {
    /// Accessing the internals is done via Mmap
    Mmap(&'a [u8]),
    /// Accessing the internals is done via File I/O
    FileIo(&'a Path),
}

/// Information after storing accounts
#[derive(Debug)]
pub struct StoredAccountsInfo {
    /// offset in the storage where each account was stored
    pub offsets: Vec<usize>,
    /// total size of all the stored accounts
    pub size: usize,
}

#[cfg(test)]
pub mod tests {
    use crate::accounts_file::AccountsFile;
    impl AccountsFile {
        pub(crate) fn set_current_len_for_tests(&self, len: usize) {
            match self {
                Self::AppendVec(av) => av.set_current_len_for_tests(len),
                Self::TieredStorage(_) => {}
            }
        }
    }
}
