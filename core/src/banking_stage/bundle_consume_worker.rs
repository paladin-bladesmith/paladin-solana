use {
    super::scheduler_messages::{BundleConsumeWork, FinishedBundleConsumeWork},
    crate::bundle_stage::bundle_consumer::BundleConsumer,
    crossbeam_channel::{Receiver, RecvError, SendError, Sender},
    solana_bundle::BundleExecutionError,
    solana_poh::poh_recorder::SharedWorkingBank,
    solana_runtime::bank::Bank,
    std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum BundleConsumeWorkerError {
    #[error("Failed to receive work from scheduler: {0}")]
    Recv(#[from] RecvError),
    #[error("Failed to send finalized bundle work to scheduler: {0}")]
    Send(#[from] SendError<FinishedBundleConsumeWork>),
}

pub(crate) struct BundleConsumeWorker {
    _id: u32,
    exit: Arc<AtomicBool>,
    bundle_work_receiver: Receiver<BundleConsumeWork>,
    bundle_consumer: BundleConsumer,
    finished_bundle_work_sender: Sender<FinishedBundleConsumeWork>,
    shared_working_bank: SharedWorkingBank,
}

impl BundleConsumeWorker {
    pub fn new(
        id: u32,
        exit: Arc<AtomicBool>,
        bundle_work_receiver: Receiver<BundleConsumeWork>,
        bundle_consumer: BundleConsumer,
        finished_bundle_work_sender: Sender<FinishedBundleConsumeWork>,
        shared_working_bank: SharedWorkingBank,
    ) -> Self {
        Self {
            _id: id,
            exit,
            bundle_work_receiver,
            bundle_consumer,
            finished_bundle_work_sender,
            shared_working_bank,
        }
    }

    pub fn run(
        mut self,
        _reservation_cb: impl Fn(&Bank) -> u64,
    ) -> Result<(), BundleConsumeWorkerError> {
        while !self.exit.load(Ordering::Relaxed) {
            let work = self.bundle_work_receiver.recv()?;
            self.do_work(work)?;
        }
        Ok(())
    }

    fn do_work(&mut self, work: BundleConsumeWork) -> Result<(), BundleConsumeWorkerError> {
        let Some(bank) = self.working_bank() else {
            return self.retry(work);
        };

        // Process the first bundle
        if self.exit.load(Ordering::Relaxed) {
            return Ok(());
        }

        if bank.is_complete() {
            return self.retry(work);
        }

        self.consume_bundle(work)?;

        // Process any additional bundles from the queue
        while let Ok(work) = self.bundle_work_receiver.try_recv() {
            if self.exit.load(Ordering::Relaxed) {
                return Ok(());
            }

            // Check if bank is still valid
            if bank.is_complete() {
                return self.retry(work);
            }

            self.consume_bundle(work)?;
        }

        Ok(())
    }

    fn consume_bundle(&mut self, work: BundleConsumeWork) -> Result<(), BundleConsumeWorkerError> {
        // Get the current bank for this bundle execution
        let Some(bank) = self.working_bank() else {
            return self.retry(work);
        };

        // Execute the bundle using BundleConsumer
        let result = self
            .bundle_consumer
            .process_single_bundle(&bank, &work.bundle);

        // Determine if the error is retryable
        let retryable = result
            .as_ref()
            .err()
            .map(Self::is_retryable_error)
            .unwrap_or(false);

        self.finished_bundle_work_sender
            .send(FinishedBundleConsumeWork { work, retryable })?;
        Ok(())
    }

    /// Determine if an error should result in a retry
    fn is_retryable_error(error: &BundleExecutionError) -> bool {
        match error {
            // Retryable errors - temporary conditions
            BundleExecutionError::PohRecordError(_)
            | BundleExecutionError::BankProcessingTimeLimitReached
            | BundleExecutionError::ExceedsCostModel => true,

            // Non-retryable errors - bundle is invalid or failed execution
            BundleExecutionError::TransactionFailure(_)
            | BundleExecutionError::TipError(_)
            | BundleExecutionError::LockError
            | BundleExecutionError::FrontRun => false,
        }
    }

    /// Get the current poh working bank without a timeout
    fn working_bank(&self) -> Option<Arc<Bank>> {
        self.shared_working_bank.load()
    }

    /// Send bundle back to scheduler as retryable
    fn retry(&self, work: BundleConsumeWork) -> Result<(), BundleConsumeWorkerError> {
        self.finished_bundle_work_sender
            .send(FinishedBundleConsumeWork {
                work,
                retryable: true,
            })?;
        Ok(())
    }
}
