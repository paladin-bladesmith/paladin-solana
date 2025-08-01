use {
    digest::Digest,
    itertools::Itertools,
    serde::{Deserialize, Serialize},
    sha2::Sha256,
    solana_transaction::versioned::VersionedTransaction,
};

#[derive(Debug, PartialEq, Default, Eq, Clone, Serialize, Deserialize)]
pub struct VersionedBundle {
    pub transactions: Vec<VersionedTransaction>,
}

/// Derives a bundle id from signatures, returning error if signature is missing
pub fn derive_bundle_id(transactions: &[VersionedTransaction]) -> Result<String, usize> {
    let signatures = transactions
        .iter()
        .enumerate()
        .map(|(idx, tx)| tx.signatures.first().ok_or(idx))
        .collect::<Result<Vec<_>, _>>()?;

    let mut hasher = Sha256::new();
    hasher.update(signatures.iter().join(","));
    Ok(format!("{:x}", hasher.finalize()))
}
