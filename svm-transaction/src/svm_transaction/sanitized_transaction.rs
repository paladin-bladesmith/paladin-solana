use {
    crate::svm_transaction::SVMTransaction,
    solana_sdk::{signature::Signature, transaction::SanitizedTransaction},
};

impl SVMTransaction for SanitizedTransaction {
    fn signature(&self) -> &Signature {
        SanitizedTransaction::signature(self)
    }

    fn signatures(&self) -> &[Signature] {
        SanitizedTransaction::signatures(self)
    }

    fn drop_on_revert(&self) -> bool {
        SanitizedTransaction::drop_on_revert(self)
    }
}
