use {
    crate::proxy::block_engine_stage::BlockBuilderFeeInfo,
    anchor_lang::{AccountDeserialize, InstructionData, ToAccountMetas},
    funnel::{instructions::become_receiver::BecomeReceiverAccounts, Funnel},
    jito_tip_distribution::sdk::{
        derive_config_account_address, derive_tip_distribution_account_address,
        instruction::{
            initialize_ix, initialize_tip_distribution_account_ix, InitializeAccounts,
            InitializeArgs, InitializeTipDistributionAccountAccounts,
            InitializeTipDistributionAccountArgs,
        },
    },
    jito_tip_payment::{
        Config, InitBumps, TipPaymentAccount, CONFIG_ACCOUNT_SEED, TIP_ACCOUNT_SEED_0,
        TIP_ACCOUNT_SEED_1, TIP_ACCOUNT_SEED_2, TIP_ACCOUNT_SEED_3, TIP_ACCOUNT_SEED_4,
        TIP_ACCOUNT_SEED_5, TIP_ACCOUNT_SEED_6, TIP_ACCOUNT_SEED_7,
    },
    log::warn,
    solana_account::ReadableAccount,
    solana_bundle::{derive_bundle_id_from_sanitized_transactions, SanitizedBundle, TipError},
    solana_clock::{Epoch, Slot},
    solana_instruction::Instruction,
    solana_keypair::Keypair,
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_sdk_ids::system_program,
    solana_signer::Signer,
    solana_transaction::{
        sanitized::{MessageHash, SanitizedTransaction},
        versioned::VersionedTransaction,
        Transaction,
    },
    solana_transaction_status::RewardType,
    std::{collections::HashSet, sync::Arc},
};

pub type Result<T> = std::result::Result<T, TipError>;

#[derive(Debug, Clone)]
struct TipPaymentProgramInfo {
    program_id: Pubkey,

    config_pda_bump: (Pubkey, u8),
    tip_pda_0: (Pubkey, u8),
    tip_pda_1: (Pubkey, u8),
    tip_pda_2: (Pubkey, u8),
    tip_pda_3: (Pubkey, u8),
    tip_pda_4: (Pubkey, u8),
    tip_pda_5: (Pubkey, u8),
    tip_pda_6: (Pubkey, u8),
    tip_pda_7: (Pubkey, u8),
}

/// Contains metadata regarding the tip-distribution account.
/// The PDAs contained in this struct are presumed to be owned by the program.
#[derive(Debug, Clone)]
struct TipDistributionProgramInfo {
    /// The tip-distribution program_id.
    program_id: Pubkey,

    /// Singleton [Config] PDA and bump tuple.
    config_pda_and_bump: (Pubkey, u8),
}

/// This config is used on each invocation to the `initialize_tip_distribution_account` instruction.
#[derive(Debug, Clone)]
pub struct TipDistributionAccountConfig {
    /// The account with authority to upload merkle-roots to this validator's [TipDistributionAccount].
    pub merkle_root_upload_authority: Pubkey,

    /// This validator's vote account.
    pub vote_account: Pubkey,

    /// This validator's commission rate BPS for tips in the [TipDistributionAccount].
    pub commission_bps: u16,
}

impl Default for TipDistributionAccountConfig {
    fn default() -> Self {
        Self {
            merkle_root_upload_authority: Pubkey::new_unique(),
            vote_account: Pubkey::new_unique(),
            commission_bps: 0,
        }
    }
}

#[derive(Clone)]
pub struct TipManager {
    funnel: Option<Pubkey>,
    rewards: Arc<dyn ReadRewards + Send + Sync>,
    rewards_split: Option<(u64, u16)>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,

    tip_payment_program_info: TipPaymentProgramInfo,
    tip_distribution_program_info: TipDistributionProgramInfo,
    tip_distribution_account_config: TipDistributionAccountConfig,
}

#[derive(Clone)]
pub struct TipManagerConfig {
    pub funnel: Option<Pubkey>,
    pub rewards_split: Option<(u64, u16)>,
    pub tip_payment_program_id: Pubkey,
    pub tip_distribution_program_id: Pubkey,
    pub tip_distribution_account_config: TipDistributionAccountConfig,
}

impl Default for TipManagerConfig {
    fn default() -> Self {
        TipManagerConfig {
            funnel: None,
            rewards_split: None,
            tip_payment_program_id: Pubkey::new_unique(),
            tip_distribution_program_id: Pubkey::new_unique(),
            tip_distribution_account_config: TipDistributionAccountConfig::default(),
        }
    }
}

impl TipManager {
    pub fn new(
        rewards: Arc<dyn ReadRewards + Send + Sync>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        config: TipManagerConfig,
    ) -> TipManager {
        let TipManagerConfig {
            funnel,
            rewards_split,
            tip_payment_program_id,
            tip_distribution_program_id,
            tip_distribution_account_config,
        } = config;

        let config_pda_bump =
            Pubkey::find_program_address(&[CONFIG_ACCOUNT_SEED], &tip_payment_program_id);

        let tip_pda_0 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_0], &tip_payment_program_id);
        let tip_pda_1 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_1], &tip_payment_program_id);
        let tip_pda_2 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_2], &tip_payment_program_id);
        let tip_pda_3 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_3], &tip_payment_program_id);
        let tip_pda_4 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_4], &tip_payment_program_id);
        let tip_pda_5 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_5], &tip_payment_program_id);
        let tip_pda_6 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_6], &tip_payment_program_id);
        let tip_pda_7 =
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_7], &tip_payment_program_id);

        let config_pda_and_bump = derive_config_account_address(&tip_distribution_program_id);

        TipManager {
            funnel,
            rewards_split,
            rewards,
            leader_schedule_cache,

            tip_payment_program_info: TipPaymentProgramInfo {
                program_id: tip_payment_program_id,
                config_pda_bump,
                tip_pda_0,
                tip_pda_1,
                tip_pda_2,
                tip_pda_3,
                tip_pda_4,
                tip_pda_5,
                tip_pda_6,
                tip_pda_7,
            },
            tip_distribution_program_info: TipDistributionProgramInfo {
                program_id: tip_distribution_program_id,
                config_pda_and_bump,
            },
            tip_distribution_account_config,
        }
    }

    pub fn tip_payment_program_id(&self) -> Pubkey {
        self.tip_payment_program_info.program_id
    }

    pub fn tip_distribution_program_id(&self) -> Pubkey {
        self.tip_distribution_program_info.program_id
    }

    /// Returns the [Config] account owned by the tip-payment program.
    pub fn tip_payment_config_pubkey(&self) -> Pubkey {
        self.tip_payment_program_info.config_pda_bump.0
    }

    /// Returns the [Config] account owned by the tip-distribution program.
    pub fn tip_distribution_config_pubkey(&self) -> Pubkey {
        self.tip_distribution_program_info.config_pda_and_bump.0
    }

    /// Given a bank, returns the current `tip_receiver` configured with the tip-payment program.
    pub fn get_configured_tip_receiver(&self, bank: &Bank) -> Result<Pubkey> {
        Ok(self.get_tip_payment_config_account(bank)?.tip_receiver)
    }

    pub fn get_tip_accounts(&self) -> HashSet<Pubkey> {
        HashSet::from([
            self.tip_payment_program_info.tip_pda_0.0,
            self.tip_payment_program_info.tip_pda_1.0,
            self.tip_payment_program_info.tip_pda_2.0,
            self.tip_payment_program_info.tip_pda_3.0,
            self.tip_payment_program_info.tip_pda_4.0,
            self.tip_payment_program_info.tip_pda_5.0,
            self.tip_payment_program_info.tip_pda_6.0,
            self.tip_payment_program_info.tip_pda_7.0,
        ])
    }

    pub fn get_tip_payment_config_account(&self, bank: &Bank) -> Result<Config> {
        let config_data = bank
            .get_account(&self.tip_payment_program_info.config_pda_bump.0)
            .ok_or(TipError::AccountMissing(
                self.tip_payment_program_info.config_pda_bump.0,
            ))?;

        Ok(Config::try_deserialize(&mut config_data.data())?)
    }

    /// Only called once during contract creation.
    pub fn initialize_tip_payment_program_tx(
        &self,
        bank: &Bank,
        keypair: &Keypair,
    ) -> RuntimeTransaction<SanitizedTransaction> {
        let init_ix = Instruction {
            program_id: self.tip_payment_program_info.program_id,
            data: jito_tip_payment::instruction::Initialize {
                _bumps: InitBumps {
                    config: self.tip_payment_program_info.config_pda_bump.1,
                    tip_payment_account_0: self.tip_payment_program_info.tip_pda_0.1,
                    tip_payment_account_1: self.tip_payment_program_info.tip_pda_1.1,
                    tip_payment_account_2: self.tip_payment_program_info.tip_pda_2.1,
                    tip_payment_account_3: self.tip_payment_program_info.tip_pda_3.1,
                    tip_payment_account_4: self.tip_payment_program_info.tip_pda_4.1,
                    tip_payment_account_5: self.tip_payment_program_info.tip_pda_5.1,
                    tip_payment_account_6: self.tip_payment_program_info.tip_pda_6.1,
                    tip_payment_account_7: self.tip_payment_program_info.tip_pda_7.1,
                },
            }
            .data(),
            accounts: jito_tip_payment::accounts::Initialize {
                config: self.tip_payment_program_info.config_pda_bump.0,
                tip_payment_account_0: self.tip_payment_program_info.tip_pda_0.0,
                tip_payment_account_1: self.tip_payment_program_info.tip_pda_1.0,
                tip_payment_account_2: self.tip_payment_program_info.tip_pda_2.0,
                tip_payment_account_3: self.tip_payment_program_info.tip_pda_3.0,
                tip_payment_account_4: self.tip_payment_program_info.tip_pda_4.0,
                tip_payment_account_5: self.tip_payment_program_info.tip_pda_5.0,
                tip_payment_account_6: self.tip_payment_program_info.tip_pda_6.0,
                tip_payment_account_7: self.tip_payment_program_info.tip_pda_7.0,
                system_program: system_program::id(),
                payer: keypair.pubkey(),
            }
            .to_account_metas(None),
        };
        let tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
            &[init_ix],
            Some(&keypair.pubkey()),
            &[keypair],
            bank.last_blockhash(),
        ));
        RuntimeTransaction::try_create(
            tx,
            MessageHash::Compute,
            None,
            bank,
            bank.get_reserved_account_keys(),
            false,
        )
        .unwrap()
    }

    /// Returns this validator's [TipDistributionAccount] PDA derived from the provided epoch.
    pub fn get_my_tip_distribution_pda(&self, epoch: Epoch) -> Pubkey {
        derive_tip_distribution_account_address(
            &self.tip_distribution_program_info.program_id,
            &self.tip_distribution_account_config.vote_account,
            epoch,
        )
        .0
    }

    /// Returns whether or not the tip-payment program should be initialized.
    pub fn should_initialize_tip_payment_program(&self, bank: &Bank) -> bool {
        match bank.get_account(&self.tip_payment_config_pubkey()) {
            None => true,
            Some(account) => account.owner() != &self.tip_payment_program_info.program_id,
        }
    }

    /// Returns whether or not the tip-distribution program's [Config] PDA should be initialized.
    pub fn should_initialize_tip_distribution_config(&self, bank: &Bank) -> bool {
        match bank.get_account(&self.tip_distribution_config_pubkey()) {
            None => true,
            Some(account) => account.owner() != &self.tip_distribution_program_info.program_id,
        }
    }

    /// Returns whether or not the current [TipDistributionAccount] PDA should be initialized for this epoch.
    pub fn should_init_tip_distribution_account(&self, bank: &Bank) -> bool {
        let pda = derive_tip_distribution_account_address(
            &self.tip_distribution_program_info.program_id,
            &self.tip_distribution_account_config.vote_account,
            bank.epoch(),
        )
        .0;
        match bank.get_account(&pda) {
            None => true,
            // Since anyone can derive the PDA and send it lamports we must also check the owner is the program.
            Some(account) => account.owner() != &self.tip_distribution_program_info.program_id,
        }
    }

    /// Creates an [Initialize] transaction object.
    pub fn initialize_tip_distribution_config_tx(
        &self,
        bank: &Bank,
        kp: &Keypair,
    ) -> RuntimeTransaction<SanitizedTransaction> {
        let ix = initialize_ix(
            self.tip_distribution_program_info.program_id,
            InitializeArgs {
                authority: kp.pubkey(),
                expired_funds_account: kp.pubkey(),
                num_epochs_valid: 10,
                max_validator_commission_bps: 10_000,
                bump: self.tip_distribution_program_info.config_pda_and_bump.1,
            },
            InitializeAccounts {
                config: self.tip_distribution_program_info.config_pda_and_bump.0,
                system_program: system_program::id(),
                initializer: kp.pubkey(),
            },
        );

        let tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
            &[ix],
            Some(&kp.pubkey()),
            &[kp],
            bank.last_blockhash(),
        ));
        RuntimeTransaction::try_create(
            tx,
            MessageHash::Compute,
            None,
            bank,
            bank.get_reserved_account_keys(),
            false,
        )
        .unwrap()
    }

    /// Creates an [InitializeTipDistributionAccount] transaction object using the provided Epoch.
    pub fn initialize_tip_distribution_account_tx(
        &self,
        bank: &Bank,
        kp: &Keypair,
    ) -> RuntimeTransaction<SanitizedTransaction> {
        let (tip_distribution_account, bump) = derive_tip_distribution_account_address(
            &self.tip_distribution_program_info.program_id,
            &self.tip_distribution_account_config.vote_account,
            bank.epoch(),
        );

        let ix = initialize_tip_distribution_account_ix(
            self.tip_distribution_program_info.program_id,
            InitializeTipDistributionAccountArgs {
                merkle_root_upload_authority: self
                    .tip_distribution_account_config
                    .merkle_root_upload_authority,
                validator_commission_bps: self.tip_distribution_account_config.commission_bps,
                bump,
            },
            InitializeTipDistributionAccountAccounts {
                config: self.tip_distribution_program_info.config_pda_and_bump.0,
                tip_distribution_account,
                system_program: system_program::id(),
                signer: kp.pubkey(),
                validator_vote_account: self.tip_distribution_account_config.vote_account,
            },
        );

        let tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
            &[ix],
            Some(&kp.pubkey()),
            &[kp],
            bank.last_blockhash(),
        ));
        RuntimeTransaction::try_create(
            tx,
            MessageHash::Compute,
            None,
            bank,
            bank.get_reserved_account_keys(),
            false,
        )
        .unwrap()
    }

    /// Builds a transaction that changes the current tip receiver to new_tip_receiver.
    /// The on-chain program will transfer tips sitting in the tip accounts to the tip receiver
    /// before changing ownership.
    pub fn change_tip_receiver_and_block_builder_tx(
        &self,
        new_tip_receiver: &Pubkey,
        bank: &Bank,
        keypair: &Keypair,
        block_builder: &Pubkey,
        block_builder_commission: u64,
    ) -> Result<RuntimeTransaction<SanitizedTransaction>> {
        let config = self.get_tip_payment_config_account(bank)?;

        let ixs = match self.funnel {
            Some(funnel) => self.build_become_receiver_tx(
                &config.tip_receiver,
                new_tip_receiver,
                bank,
                keypair,
                &config.block_builder,
                block_builder,
                block_builder_commission,
                (Self::get_funnel_account(bank, funnel)?, funnel),
            ),
            None => self.build_change_tip_receiver_and_block_builder_tx(
                &config.tip_receiver,
                new_tip_receiver,
                keypair,
                &config.block_builder,
                block_builder,
                block_builder_commission,
            ),
        };

        let tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
            &ixs.into_iter()
                .chain(self.build_rewards_split_ix(bank, keypair))
                .collect::<Vec<_>>(),
            Some(&keypair.pubkey()),
            &[keypair],
            bank.last_blockhash(),
        ));
        let tx = RuntimeTransaction::try_create(
            tx,
            MessageHash::Compute,
            None,
            bank,
            bank.get_reserved_account_keys(),
            false,
        )
        .unwrap();

        Ok(tx)
    }

    pub fn build_change_tip_receiver_and_block_builder_tx(
        &self,
        old_tip_receiver: &Pubkey,
        new_tip_receiver: &Pubkey,
        keypair: &Keypair,
        old_block_builder: &Pubkey,
        block_builder: &Pubkey,
        block_builder_commission: u64,
    ) -> [Instruction; 2] {
        let change_tip_ix = Instruction {
            program_id: self.tip_payment_program_info.program_id,
            data: jito_tip_payment::instruction::ChangeTipReceiver {}.data(),
            accounts: jito_tip_payment::accounts::ChangeTipReceiver {
                config: self.tip_payment_program_info.config_pda_bump.0,
                old_tip_receiver: *old_tip_receiver,
                new_tip_receiver: *new_tip_receiver,
                block_builder: *old_block_builder,
                tip_payment_account_0: self.tip_payment_program_info.tip_pda_0.0,
                tip_payment_account_1: self.tip_payment_program_info.tip_pda_1.0,
                tip_payment_account_2: self.tip_payment_program_info.tip_pda_2.0,
                tip_payment_account_3: self.tip_payment_program_info.tip_pda_3.0,
                tip_payment_account_4: self.tip_payment_program_info.tip_pda_4.0,
                tip_payment_account_5: self.tip_payment_program_info.tip_pda_5.0,
                tip_payment_account_6: self.tip_payment_program_info.tip_pda_6.0,
                tip_payment_account_7: self.tip_payment_program_info.tip_pda_7.0,
                signer: keypair.pubkey(),
            }
            .to_account_metas(None),
        };
        let change_block_builder_ix = Instruction {
            program_id: self.tip_payment_program_info.program_id,
            data: jito_tip_payment::instruction::ChangeBlockBuilder {
                block_builder_commission,
            }
            .data(),
            accounts: jito_tip_payment::accounts::ChangeBlockBuilder {
                config: self.tip_payment_program_info.config_pda_bump.0,
                tip_receiver: *new_tip_receiver, // tip receiver will have just changed in previous ix
                old_block_builder: *old_block_builder,
                new_block_builder: *block_builder,
                tip_payment_account_0: self.tip_payment_program_info.tip_pda_0.0,
                tip_payment_account_1: self.tip_payment_program_info.tip_pda_1.0,
                tip_payment_account_2: self.tip_payment_program_info.tip_pda_2.0,
                tip_payment_account_3: self.tip_payment_program_info.tip_pda_3.0,
                tip_payment_account_4: self.tip_payment_program_info.tip_pda_4.0,
                tip_payment_account_5: self.tip_payment_program_info.tip_pda_5.0,
                tip_payment_account_6: self.tip_payment_program_info.tip_pda_6.0,
                tip_payment_account_7: self.tip_payment_program_info.tip_pda_7.0,
                signer: keypair.pubkey(),
            }
            .to_account_metas(None),
        };

        [change_tip_ix, change_block_builder_ix]
    }

    /// Returns the balance of all the MEV tip accounts
    pub fn get_tip_account_balances(&self, bank: &Arc<Bank>) -> Vec<(Pubkey, u64)> {
        let accounts = self.get_tip_accounts();
        accounts
            .into_iter()
            .map(|account| {
                let balance = bank.get_balance(&account);
                (account, balance)
            })
            .collect()
    }

    /// Returns the balance of all the MEV tip accounts above the rent-exempt amount.
    /// NOTE: the on-chain program has rent_exempt = force
    pub fn get_tip_account_balances_above_rent_exempt(
        &self,
        bank: &Arc<Bank>,
    ) -> Vec<(Pubkey, u64)> {
        let accounts = self.get_tip_accounts();
        accounts
            .into_iter()
            .map(|account| {
                let account_data = bank.get_account(&account).unwrap_or_default();
                let balance = bank.get_balance(&account);
                let rent_exempt =
                    bank.get_minimum_balance_for_rent_exemption(account_data.data().len());
                // NOTE: don't unwrap here in case bug in on-chain program, don't want all validators to crash
                // if program gets stuck in bad state
                (account, balance.checked_sub(rent_exempt).unwrap_or_else(|| {
                    warn!("balance is below rent exempt amount. balance: {} rent_exempt: {} acc size: {}", balance, rent_exempt, TipPaymentAccount::SIZE);
                    0
                }))
            })
            .collect()
    }

    /// Return a bundle that is capable of calling the initialize instructions on the two tip payment programs
    /// This is mainly helpful for local development and shouldn't run on testnet and mainnet, assuming the
    /// correct TipManager configuration is set.
    pub fn get_initialize_tip_programs_bundle(
        &self,
        bank: &Bank,
        keypair: &Keypair,
    ) -> Option<SanitizedBundle> {
        let maybe_init_tip_payment_config_tx = if self.should_initialize_tip_payment_program(bank) {
            debug!("should_initialize_tip_payment_program=true");
            Some(self.initialize_tip_payment_program_tx(bank, keypair))
        } else {
            None
        };

        let maybe_init_tip_distro_config_tx =
            if self.should_initialize_tip_distribution_config(bank) {
                debug!("should_initialize_tip_distribution_config=true");
                Some(self.initialize_tip_distribution_config_tx(bank, keypair))
            } else {
                None
            };

        let transactions = [
            maybe_init_tip_payment_config_tx,
            maybe_init_tip_distro_config_tx,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<RuntimeTransaction<SanitizedTransaction>>>();

        if transactions.is_empty() {
            None
        } else {
            let bundle_id = derive_bundle_id_from_sanitized_transactions(&transactions);
            Some(SanitizedBundle {
                transactions,
                bundle_id,
            })
        }
    }

    pub fn get_tip_programs_crank_bundle(
        &self,
        bank: &Bank,
        keypair: &Keypair,
        block_builder_fee_info: &BlockBuilderFeeInfo,
    ) -> Result<Option<SanitizedBundle>> {
        let maybe_init_tip_distro_account_tx = if self.should_init_tip_distribution_account(bank) {
            debug!("should_init_tip_distribution_account=true");
            Some(self.initialize_tip_distribution_account_tx(bank, keypair))
        } else {
            None
        };
        let tip_payment_config = self.get_tip_payment_config_account(bank)?;

        let my_tip_receiver = self.get_my_tip_distribution_pda(bank.epoch());
        let requires_updating = match self.funnel {
            Some(funnel) => {
                let configured_funnel_receiver = Self::get_funnel_account(bank, funnel)?.receiver;
                tip_payment_config.tip_receiver != funnel
                    || configured_funnel_receiver != my_tip_receiver
                    || tip_payment_config.block_builder != block_builder_fee_info.block_builder
                    || tip_payment_config.block_builder_commission_pct
                        != block_builder_fee_info.block_builder_commission
            }
            None => {
                tip_payment_config.tip_receiver != my_tip_receiver
                    || tip_payment_config.block_builder != block_builder_fee_info.block_builder
                    || tip_payment_config.block_builder_commission_pct
                        != block_builder_fee_info.block_builder_commission
            }
        };

        let maybe_change_tip_receiver_tx = if requires_updating {
            debug!("change_tip_receiver=true");
            Some(self.change_tip_receiver_and_block_builder_tx(
                &my_tip_receiver,
                bank,
                keypair,
                &block_builder_fee_info.block_builder,
                block_builder_fee_info.block_builder_commission,
            )?)
        } else {
            None
        };
        debug!(
            "maybe_change_tip_receiver_tx: {:?}",
            maybe_change_tip_receiver_tx
        );

        let transactions = [
            maybe_init_tip_distro_account_tx,
            maybe_change_tip_receiver_tx,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<RuntimeTransaction<SanitizedTransaction>>>();

        if transactions.is_empty() {
            Ok(None)
        } else {
            let bundle_id = derive_bundle_id_from_sanitized_transactions(&transactions);
            Ok(Some(SanitizedBundle {
                transactions,
                bundle_id,
            }))
        }
    }

    // PALADIN

    fn build_become_receiver_tx(
        &self,
        old_tip_receiver: &Pubkey,
        new_tip_receiver: &Pubkey,
        bank: &Bank,
        keypair: &Keypair,
        old_block_builder: &Pubkey,
        block_builder: &Pubkey,
        block_builder_commission: u64,
        (funnel, funnel_key): (Funnel, Pubkey),
    ) -> [Instruction; 2] {
        let additional_lamports = self.compute_additional_lamports(bank, keypair);

        let become_receiver = funnel::instructions::become_receiver::ix(
            BecomeReceiverAccounts {
                payer: keypair.pubkey(),
                funnel_config: funnel_key,
                block_builder_old: *old_block_builder,
                tip_receiver_old: *old_tip_receiver,
                paladin_receiver_old: funnel.receiver,
                paladin_receiver_new: *new_tip_receiver,
                paladin_receiver_new_state: funnel::find_leader_state(&keypair.pubkey()).0,
            },
            &funnel.config,
            additional_lamports,
        );
        let change_block_builder_ix = Instruction {
            program_id: self.tip_payment_program_info.program_id,
            data: jito_tip_payment::instruction::ChangeBlockBuilder {
                block_builder_commission,
            }
            .data(),
            accounts: jito_tip_payment::accounts::ChangeBlockBuilder {
                config: self.tip_payment_program_info.config_pda_bump.0,
                tip_receiver: funnel_key,
                old_block_builder: *old_block_builder,
                new_block_builder: *block_builder,
                tip_payment_account_0: self.tip_payment_program_info.tip_pda_0.0,
                tip_payment_account_1: self.tip_payment_program_info.tip_pda_1.0,
                tip_payment_account_2: self.tip_payment_program_info.tip_pda_2.0,
                tip_payment_account_3: self.tip_payment_program_info.tip_pda_3.0,
                tip_payment_account_4: self.tip_payment_program_info.tip_pda_4.0,
                tip_payment_account_5: self.tip_payment_program_info.tip_pda_5.0,
                tip_payment_account_6: self.tip_payment_program_info.tip_pda_6.0,
                tip_payment_account_7: self.tip_payment_program_info.tip_pda_7.0,
                signer: keypair.pubkey(),
            }
            .to_account_metas(None),
        };

        [become_receiver, change_block_builder_ix]
    }

    fn build_rewards_split_ix(&self, bank: &Bank, keypair: &Keypair) -> Option<Instruction> {
        self.rewards_split.and_then(|(minimum_sol, split_bp)| {
            let split_bp = std::cmp::min(split_bp, 10_000);
            let identity_balance = bank.get_balance(&keypair.pubkey());
            let available_balance = identity_balance.saturating_sub(minimum_sol);
            if available_balance == 0 {
                return None;
            }

            let previous_rewards = self.compute_previous_leader_slot_lamports(bank, keypair);
            let lamports = std::cmp::min(
                previous_rewards.saturating_mul(split_bp as u64) / 10_000,
                available_balance,
            );

            // Don't transfer 0 lamports.
            if lamports == 0 {
                return None;
            }

            Some(solana_system_interface::instruction::transfer(
                &keypair.pubkey(),
                &self.tip_payment_program_info.tip_pda_0.0,
                lamports,
            ))
        })
    }

    // TODO: Do we need both `compute_additional_lamports` and `compute_previous_leader_slot_lamports`?

    fn compute_additional_lamports(&self, bank: &Bank, keypair: &Keypair) -> u64 {
        // TODO: Do we need to think about handling identity migrations? Should
        // not result in much missed rewards, right - just last leader sprint.
        let identity = keypair.pubkey();
        let current_slot = bank.slot();
        let current_epoch = bank.epoch();
        let current_epoch_start_slot = bank.epoch_schedule().get_first_slot_in_epoch(current_epoch);
        let Some(previous_epoch) = bank.epoch().checked_sub(1) else {
            return 0;
        };
        let previous_epoch_start_slot = bank
            .epoch_schedule()
            .get_first_slot_in_epoch(previous_epoch);

        // Get current & previous leader schedules.
        let Some(previous_leader_slots) = self
            .leader_schedule_cache
            .get_epoch_leader_schedule(previous_epoch)
            .map(|schedule| {
                schedule
                    .get_leader_slots_map()
                    .get(&identity)
                    .cloned()
                    .unwrap_or_default()
            })
        else {
            eprintln!("BUG: Previous leader schedule missing?");
            return 0;
        };
        let Some(current_leader_slots) = self
            .leader_schedule_cache
            .get_epoch_leader_schedule(current_epoch)
            .map(|schedule| {
                schedule
                    .get_leader_slots_map()
                    .get(&identity)
                    .cloned()
                    .unwrap_or_default()
            })
        else {
            eprintln!("BUG: Current leader schedule missing?");
            return 0;
        };

        // Compute highest paid slot.
        let highest_paid = bank
            .get_account(&funnel::find_leader_state(&identity).0)
            .and_then(|account| {
                funnel::LeaderState::try_from_bytes(account.data())
                    .ok()
                    .map(|state| state.last_slot)
            });

        // Compute the min slot and previous + current leader slots.
        let both = [
            (previous_epoch_start_slot, previous_leader_slots),
            (current_epoch_start_slot, current_leader_slots.clone()),
        ];
        let current_only = [(current_epoch_start_slot, current_leader_slots)];
        let (min_slot, offsets) = match highest_paid {
            Some(slot) => (slot, both.as_slice()),
            // Pay all outstanding only for the current epoch.
            None => (0, current_only.as_slice()),
        };

        // Sum our outstanding rewards (i.e. rewards that have not been split
        // with the funnel).
        let mut outstanding_rewards = 0;
        for slot in offsets
            .iter()
            .flat_map(|(start_slot, offsets)| {
                offsets.iter().map(|offset| *start_slot + *offset as u64)
            })
            .filter(|slot| *slot >= min_slot)
        {
            // If we've caught up to the current slot, break.
            if slot == current_slot {
                break;
            }
            if slot > current_slot {
                eprintln!("BUG: Current slot not in indexes, are we the leader?");

                return 0;
            }

            // Accumulate the rewards for the block.
            outstanding_rewards += self.rewards.read_rewards(slot);
        }

        let owing = Self::calculate_funnel_take(outstanding_rewards);
        let identity = bank.get_account(&identity).unwrap_or_default();
        let min_rent_exemption = bank
            .rent_collector()
            .rent
            .minimum_balance(identity.data().len());

        std::cmp::min(
            owing,
            identity.lamports().saturating_sub(min_rent_exemption),
        )
    }

    fn compute_previous_leader_slot_lamports(&self, bank: &Bank, keypair: &Keypair) -> u64 {
        let identity = keypair.pubkey();
        let current_slot = bank.slot();
        let (current_epoch, current_offset) = bank.get_epoch_and_slot_index(current_slot);
        let current_offset = current_offset as usize;
        let epoch_first_slot = bank.epoch_schedule().get_first_slot_in_epoch(current_epoch);

        let Some(current_leader_slots) = self
            .leader_schedule_cache
            .get_epoch_leader_schedule(current_epoch)
            .map(|schedule| {
                schedule
                    .get_leader_slots_map()
                    .get(&identity)
                    .cloned()
                    .unwrap_or_default()
            })
        else {
            eprintln!("BUG: Current leader schedule missing?");
            return 0;
        };

        // Figure out the index of the current slot.
        let current_leader_slots: Arc<Vec<usize>> = current_leader_slots;
        let index = match current_leader_slots.binary_search(&current_offset) {
            Ok(index) => index,
            Err(index) => index,
        };

        // If this is the first slot in the epoch, return 0.
        if index == 0 {
            return 0;
        }

        // Else, check the previous leader sprint to compute the payout owed.
        let mut iter = current_leader_slots[..index].iter().rev();
        let last = iter.next().unwrap();
        let mut prev = last;
        for offset in iter {
            if offset != &prev.saturating_sub(1) {
                break;
            }

            prev = offset;
        }

        // Sum the prior block rewards.
        (*prev..=*last)
            .map(|offset| {
                let slot = epoch_first_slot + offset as u64;

                if slot > current_slot {
                    eprintln!("BUG: Current slot not in indexes, are we the leader?");

                    return 0;
                }

                // Accumulate the rewards for the block.
                self.rewards.read_rewards(slot)
            })
            .sum()
    }

    fn get_funnel_account(bank: &Bank, funnel: Pubkey) -> Result<Funnel> {
        let funnel_data = bank
            .get_account(&funnel)
            .ok_or(TipError::AccountMissing(funnel))?;

        Funnel::try_from_bytes(funnel_data.data())
            .copied()
            .map_err(|err| TipError::AnchorError(err.to_string()))
    }

    fn calculate_funnel_take(reward: u64) -> u64 {
        reward / 10
    }
}

pub trait ReadRewards {
    fn read_rewards(&self, slot: Slot) -> u64;
}

impl ReadRewards for Blockstore {
    fn read_rewards(&self, slot: Slot) -> u64 {
        self.read_rewards(slot)
            .ok()
            .flatten()
            .unwrap_or_default()
            .into_iter()
            .map(|reward| match reward.reward_type {
                Some(RewardType::Fee) => reward.lamports,
                _ => 0,
            })
            .sum::<i64>()
            .try_into()
            .unwrap()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use {
        super::*,
        funnel::LeaderState,
        solana_account::Account,
        solana_accounts_db::accounts_db::CalcAccountsHashDataSource,
        solana_fee_calculator::{FeeRateGovernor, DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE},
        solana_genesis_config::ClusterType,
        solana_ledger::leader_schedule::IdentityKeyedLeaderSchedule,
        solana_native_token::sol_to_lamports,
        solana_program_test::programs::spl_programs,
        solana_rent::Rent,
        solana_runtime::genesis_utils::create_genesis_config_with_leader_ex,
        solana_vote_program::vote_state::VoteState,
        std::sync::RwLock,
    };

    #[derive(Default)]
    pub(crate) struct MockBlockstore(pub(crate) Vec<u64>);

    impl ReadRewards for RwLock<MockBlockstore> {
        fn read_rewards(&self, slot: Slot) -> u64 {
            self.read()
                .unwrap()
                .0
                .get(slot as usize)
                .copied()
                .unwrap_or(0)
        }
    }

    struct TestFixture {
        bank: Arc<Bank>,
        blockstore: Arc<RwLock<MockBlockstore>>,
        leader_keypair: Arc<Keypair>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        tip_manager: TipManager,
        paladin: Pubkey,
    }

    fn create_fixture(paladin_slots: &[u64]) -> TestFixture {
        let mint_keypair = Keypair::new();
        let leader_keypair = Arc::new(Keypair::new());
        let voting_keypair = Keypair::new();

        // Setup genesis.
        let rent = Rent::default();
        let genesis_config = create_genesis_config_with_leader_ex(
            sol_to_lamports(1000.0 as f64),
            &mint_keypair.pubkey(),
            &leader_keypair.pubkey(),
            &voting_keypair.pubkey(),
            &solana_pubkey::new_rand(),
            rent.minimum_balance(VoteState::size_of()) + sol_to_lamports(1_000_000.0),
            sol_to_lamports(1_000_000.0),
            FeeRateGovernor {
                // Initialize with a non-zero fee
                lamports_per_signature: DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE / 2,
                ..FeeRateGovernor::default()
            },
            rent.clone(), // most tests don't expect rent
            ClusterType::Development,
            spl_programs(&rent),
        );

        // Setup TipManager dependencies.
        let blockstore = Arc::new(RwLock::new(MockBlockstore::default()));
        let paladin = Arc::new(Keypair::new());
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        assert_eq!(bank.epoch(), 0);
        let bank = Arc::new(Bank::warp_from_parent(
            bank.clone(),
            &Pubkey::new_unique(),
            genesis_config.epoch_schedule.get_first_slot_in_epoch(1) - 1,
            CalcAccountsHashDataSource::IndexForTests,
        ));
        assert_eq!(bank.epoch(), 0);
        let bank = Bank::new_from_parent(bank.clone(), &Pubkey::new_unique(), bank.slot() + 1);
        assert_eq!(bank.epoch(), 1);
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));
        let config = TipManagerConfig::default();

        // Setup the paladin leader account.
        bank.store_account(
            &paladin.pubkey(),
            &Account {
                lamports: 10u64.pow(9),
                ..Default::default()
            }
            .into(),
        );

        // Setup the paladin leader state.
        let (paladin_leader_state, _) = funnel::find_leader_state(&paladin.pubkey());
        bank.store_account(
            &paladin_leader_state,
            &Account {
                lamports: rent.minimum_balance(LeaderState::LEN),
                data: LeaderState { last_slot: 0 }.as_bytes().to_vec(),
                ..Account::default()
            }
            .into(),
        );

        // Override the provided leader slots to be our paladin leader.
        for slot in paladin_slots {
            let (epoch, offset) = bank.get_epoch_and_slot_index(*slot);
            let mut slot_leaders = leader_schedule_cache
                .get_epoch_leader_schedule(epoch)
                .unwrap()
                .get_slot_leaders()
                .to_vec();

            slot_leaders[offset as usize] = paladin.pubkey();

            let leader_schedule = IdentityKeyedLeaderSchedule::new_from_schedule(slot_leaders);
            *leader_schedule_cache
                .cached_schedules
                .write()
                .unwrap()
                .0
                .get_mut(&epoch)
                .unwrap() = Arc::new(Box::new(leader_schedule));
        }

        TestFixture {
            bank: Arc::new(bank),
            blockstore: blockstore.clone(),
            leader_keypair,
            leader_schedule_cache: leader_schedule_cache.clone(),
            tip_manager: TipManager::new(blockstore, leader_schedule_cache, config),
            paladin: paladin.pubkey(),
        }
    }

    #[test]
    fn compute_additional_lamports_base() {
        // Arrange.
        let fixture = create_fixture(&[]);

        // Act.
        let additional = fixture
            .tip_manager
            .compute_additional_lamports(&fixture.bank, &fixture.leader_keypair);

        // Assert.
        assert_eq!(additional, 0);
    }

    #[test]
    fn compute_additional_lamports_prior_slot_no_rewards() {
        // Arrange.
        let fixture = create_fixture(&[]);
        let child_bank = Bank::new_from_parent(
            fixture.bank.clone(),
            &fixture.leader_keypair.pubkey(),
            fixture.bank.slot() + 1,
        );

        // Act.
        let additional = fixture
            .tip_manager
            .compute_additional_lamports(&child_bank, &fixture.leader_keypair);

        // Assert.
        assert_eq!(additional, 0);
    }

    #[test]
    fn compute_additional_lamports_prior_slot_rewards() {
        // Arrange.
        let fixture = create_fixture(&[0]);
        let child_bank = Bank::new_from_parent(
            fixture.bank.clone(),
            &fixture.leader_keypair.pubkey(),
            fixture.bank.slot() + 1,
        );
        fixture.blockstore.write().unwrap().0.push(100);

        // Act.
        let additional = fixture
            .tip_manager
            .compute_additional_lamports(&child_bank, &fixture.leader_keypair);

        // Assert.
        assert_eq!(additional, TipManager::calculate_funnel_take(100));
    }

    #[test]
    fn compute_additional_lamports_prior_slot_not_our_leader() {
        // Arrange.
        let fixture = create_fixture(&[]);
        let child_bank = Bank::new_from_parent(fixture.bank, &fixture.leader_keypair.pubkey(), 33);
        fixture.blockstore.write().unwrap().0.push(100);
        let mut slot_leaders = fixture
            .leader_schedule_cache
            .get_epoch_leader_schedule(0)
            .unwrap()
            .get_slot_leaders()
            .to_vec();
        slot_leaders[0] = Pubkey::new_unique();
        let leader_schedule = IdentityKeyedLeaderSchedule::new_from_schedule(slot_leaders);
        *fixture
            .leader_schedule_cache
            .cached_schedules
            .write()
            .unwrap()
            .0
            .get_mut(&0)
            .unwrap() = Arc::new(Box::new(leader_schedule));

        // Act.
        let additional = fixture
            .tip_manager
            .compute_additional_lamports(&child_bank, &fixture.leader_keypair);

        // Assert.
        assert_eq!(additional, 0);
    }

    #[test]
    fn compute_additional_lamports_multiple_prior_slots_with_gaps() {
        // Arrange.
        let fixture = create_fixture(&[32, 35, 37, 39]);

        // Create a bank at slot 40.
        let child_bank =
            Bank::new_from_parent(fixture.bank.clone(), &fixture.leader_keypair.pubkey(), 40);
        fixture
            .blockstore
            .write()
            .unwrap()
            .0
            .extend(std::iter::repeat(100).take(40));

        // Act.
        let additional = fixture
            .tip_manager
            .compute_additional_lamports(&child_bank, &fixture.leader_keypair);

        // Assert.
        assert_eq!(additional, TipManager::calculate_funnel_take(400));
    }

    #[test]
    fn compute_additional_lamports_use_slots_from_previous_epoch() {
        // Arrange.
        let fixture = create_fixture(&[0, 12, 20, 32]);
        let bank = Bank::new_from_parent(fixture.bank, &Pubkey::new_unique(), 33);

        // Set the block reward to to the slot index.
        fixture
            .blockstore
            .write()
            .unwrap()
            .0
            .extend((0..64).map(|i| i * 100));

        // Act.
        let additional = fixture
            .tip_manager
            .compute_additional_lamports(&bank, &fixture.leader_keypair);

        // Assert.
        assert_eq!(
            additional,
            TipManager::calculate_funnel_take(0 + 1200 + 2000 + 3200)
        );
    }

    #[test]
    fn compute_additional_lamports_no_leader_state_prev() {
        let fixture = create_fixture(&[0, 12, 20]);
        let bank = Bank::new_from_parent(fixture.bank, &Pubkey::new_unique(), 33);

        // Set the block reward to to the slot index.
        fixture
            .blockstore
            .write()
            .unwrap()
            .0
            .extend((0..64).map(|i| i * 10));

        // Remove paladin leader state account.
        let (paladin_leader_state, _) = funnel::find_leader_state(&fixture.paladin);
        bank.store_account(&paladin_leader_state, &Account::default().into());

        // Act.
        let additional = fixture
            .tip_manager
            .compute_additional_lamports(&bank, &fixture.leader_keypair);

        // Assert.
        assert_eq!(additional, 0);
    }

    #[test]
    fn compute_additional_lamports_no_leader_state_curr() {
        let fixture = create_fixture(&[32, 39]);
        let bank = Bank::new_from_parent(fixture.bank, &Pubkey::new_unique(), 45);

        // Set the block reward to to the slot index.
        fixture
            .blockstore
            .write()
            .unwrap()
            .0
            .extend((0..64).map(|i| i * 10));

        // Remove paladin leader state account.
        let (paladin_leader_state, _) = funnel::find_leader_state(&fixture.paladin);
        bank.store_account(&paladin_leader_state, &Account::default().into());

        // Act.
        let additional = fixture
            .tip_manager
            .compute_additional_lamports(&bank, &fixture.leader_keypair);

        // Assert.
        assert_eq!(additional, TipManager::calculate_funnel_take(320 + 390));
    }

    #[test]
    fn compute_additional_lamports_no_leader_state_both() {
        let fixture = create_fixture(&[9, 21, 31, 34, 39]);
        let bank = Bank::new_from_parent(fixture.bank, &Pubkey::new_unique(), 45);

        // Set the block reward to to the slot index.
        fixture
            .blockstore
            .write()
            .unwrap()
            .0
            .extend((0..64).map(|i| i * 10));

        // Remove paladin leader state account.
        let (paladin_leader_state, _) = funnel::find_leader_state(&fixture.paladin);
        bank.store_account(&paladin_leader_state, &Account::default().into());

        // Act.
        let additional = fixture
            .tip_manager
            .compute_additional_lamports(&bank, &fixture.leader_keypair);

        // Assert.
        assert_eq!(additional, TipManager::calculate_funnel_take(340 + 390));
    }

    #[test]
    fn compute_additional_lamports_rent_exemption_pays_zero() {
        let fixture = create_fixture(&[9, 21, 31, 34, 39]);
        let bank = Bank::new_from_parent(fixture.bank, &Pubkey::new_unique(), 45);

        // Set the block reward to to the slot index.
        fixture
            .blockstore
            .write()
            .unwrap()
            .0
            .extend((0..64).map(|i| i * 10));

        // Reduce the paladin leader lamport balance to just the rent exemption
        // requirement.
        bank.store_account(
            &fixture.paladin,
            &Account {
                lamports: bank.rent_collector().rent.minimum_balance(0),
                ..Account::default()
            }
            .into(),
        );

        // Act.
        let additional = fixture
            .tip_manager
            .compute_additional_lamports(&bank, &fixture.leader_keypair);

        // Assert.
        assert_eq!(additional, 0);
    }

    #[test]
    fn compute_additional_lamports_rent_exemption_pays_one() {
        let fixture = create_fixture(&[9, 21, 31, 34, 39]);
        let bank = Bank::new_from_parent(fixture.bank, &Pubkey::new_unique(), 45);

        // Set the block reward to to the slot index.
        fixture
            .blockstore
            .write()
            .unwrap()
            .0
            .extend((0..64).map(|i| i * 10));

        // Reduce the paladin leader lamport balance to just the rent exemption
        // requirement.
        bank.store_account(
            &fixture.paladin,
            &Account {
                lamports: bank.rent_collector().rent.minimum_balance(0) + 1,
                ..Account::default()
            }
            .into(),
        );

        // Act.
        let additional = fixture
            .tip_manager
            .compute_additional_lamports(&bank, &fixture.leader_keypair);

        // Assert.
        assert_eq!(additional, 1);
    }
}
