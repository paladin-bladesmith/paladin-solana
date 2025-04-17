use {crate::svm_message::SVMMessage, solana_signature::Signature};

mod sanitized_transaction;

pub trait SVMTransaction: SVMMessage {
    /// Get the first signature of the message.
    fn signature(&self) -> &Signature;

    /// Get all the signatures of the message.
    fn signatures(&self) -> &[Signature];

    /// Whether the transaction should be dropped from the block on revert.
    fn drop_on_revert(&self) -> bool;
}
