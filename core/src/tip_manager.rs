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
    solana_bundle::TipError,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_runtime::bank::Bank,
    solana_sdk::{
        account::ReadableAccount,
        bundle::{derive_bundle_id_from_sanitized_transactions, SanitizedBundle},
        clock::Slot,
        instruction::Instruction,
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
        stake_history::Epoch,
        system_program,
        transaction::{SanitizedTransaction, Transaction},
    },
    solana_transaction_status::RewardType,
    std::{collections::HashSet, sync::Arc},
};

const FUNNEL_CONFIG: Pubkey = solana_sdk::pubkey!("6RfdhWwnNBKwchqPex7RPBw2c8Cku8y4QyUqjX71YoBq");

pub type Result<T> = std::result::Result<T, TipError>;

fn calculate_funnel_take(reward: u64) -> u64 {
    reward / 10
}

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
    rewards: Arc<dyn ReadRewards + Send + Sync>,
    cluster_info: Arc<ClusterInfo>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,

    tip_payment_program_info: TipPaymentProgramInfo,
    tip_distribution_program_info: TipDistributionProgramInfo,
    tip_distribution_account_config: TipDistributionAccountConfig,
    tip_accounts: HashSet<Pubkey>,
}

#[derive(Clone)]
pub struct TipManagerConfig {
    pub tip_payment_program_id: Pubkey,
    pub tip_distribution_program_id: Pubkey,
    pub tip_distribution_account_config: TipDistributionAccountConfig,
}

impl Default for TipManagerConfig {
    fn default() -> Self {
        TipManagerConfig {
            tip_payment_program_id: Pubkey::new_unique(),
            tip_distribution_program_id: Pubkey::new_unique(),
            tip_distribution_account_config: TipDistributionAccountConfig::default(),
        }
    }
}

impl TipManager {
    pub(crate) fn new(
        rewards: Arc<dyn ReadRewards + Send + Sync>,
        cluster_info: Arc<ClusterInfo>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        config: TipManagerConfig,
    ) -> TipManager {
        let TipManagerConfig {
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
            rewards,
            cluster_info,
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
            tip_accounts: HashSet::from_iter([
                tip_pda_0.0,
                tip_pda_1.0,
                tip_pda_2.0,
                tip_pda_3.0,
                tip_pda_4.0,
                tip_pda_5.0,
                tip_pda_6.0,
                tip_pda_7.0,
            ]),
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

    pub fn get_tip_accounts(&self) -> &HashSet<Pubkey> {
        &self.tip_accounts
    }

    pub fn get_tip_payment_config_account(&self, bank: &Bank) -> Result<Config> {
        let config_data = bank
            .get_account(&self.tip_payment_program_info.config_pda_bump.0)
            .ok_or(TipError::AccountMissing(
                self.tip_payment_program_info.config_pda_bump.0,
            ))?;

        Ok(Config::try_deserialize(&mut config_data.data())?)
    }

    pub fn get_funnel_account(&self, bank: &Bank) -> Result<Funnel> {
        let funnel_data = bank
            .get_account(&FUNNEL_CONFIG)
            .ok_or(TipError::AccountMissing(FUNNEL_CONFIG))?;

        Funnel::try_from_bytes(funnel_data.data())
            .copied()
            .map_err(|err| TipError::AnchorError(err.to_string()))
    }

    /// Only called once during contract creation.
    pub fn initialize_tip_payment_program_tx(
        &self,
        bank: &Bank,
        keypair: &Keypair,
    ) -> SanitizedTransaction {
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
        SanitizedTransaction::try_from_legacy_transaction(
            Transaction::new_signed_with_payer(
                &[init_ix],
                Some(&keypair.pubkey()),
                &[keypair],
                bank.last_blockhash(),
            ),
            bank.get_reserved_account_keys(),
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
    ) -> SanitizedTransaction {
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

        SanitizedTransaction::try_from_legacy_transaction(
            Transaction::new_signed_with_payer(
                &[ix],
                Some(&kp.pubkey()),
                &[kp],
                bank.last_blockhash(),
            ),
            bank.get_reserved_account_keys(),
        )
        .unwrap()
    }

    /// Creates an [InitializeTipDistributionAccount] transaction object using the provided Epoch.
    pub fn initialize_tip_distribution_account_tx(
        &self,
        bank: &Bank,
        keypair: &Keypair,
    ) -> SanitizedTransaction {
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
                signer: keypair.pubkey(),
                validator_vote_account: self.tip_distribution_account_config.vote_account,
            },
        );

        SanitizedTransaction::try_from_legacy_transaction(
            Transaction::new_signed_with_payer(
                &[ix],
                Some(&keypair.pubkey()),
                &[keypair],
                bank.last_blockhash(),
            ),
            bank.get_reserved_account_keys(),
        )
        .unwrap()
    }

    /// Builds a transaction that changes the current tip receiver to new_tip_receiver.
    /// The on-chain program will transfer tips sitting in the tip accounts to the tip receiver
    /// before changing ownership.
    pub fn change_tip_receiver_and_block_builder_tx(
        &self,
        new_funnel_receiver: &Pubkey,
        bank: &Bank,
        keypair: &Keypair,
        block_builder: &Pubkey,
        block_builder_commission: u64,
    ) -> Result<SanitizedTransaction> {
        let jito_config = self.get_tip_payment_config_account(bank)?;
        let funnel = self.get_funnel_account(bank)?;

        Ok(self.build_change_tip_receiver_and_block_builder_tx(
            &jito_config.tip_receiver,
            new_funnel_receiver,
            bank,
            keypair,
            &jito_config.block_builder,
            block_builder,
            block_builder_commission,
            &funnel,
        ))
    }

    pub fn build_change_tip_receiver_and_block_builder_tx(
        &self,
        old_tip_receiver: &Pubkey,
        new_funnel_receiver: &Pubkey,
        bank: &Bank,
        keypair: &Keypair,
        old_block_builder: &Pubkey,
        block_builder: &Pubkey,
        block_builder_commission: u64,
        funnel: &Funnel,
    ) -> SanitizedTransaction {
        let additional_lamports = self.compute_additional_lamports(bank);

        let become_receiver = funnel::instructions::become_receiver::ix(
            BecomeReceiverAccounts {
                payer: keypair.pubkey(),
                funnel_config: FUNNEL_CONFIG,
                block_builder_old: *old_block_builder,
                tip_receiver_old: *old_tip_receiver,
                paladin_receiver_old: funnel.receiver,
                paladin_receiver_new: *new_funnel_receiver,
                paladin_receiver_new_state: funnel::find_leader_state(new_funnel_receiver).0,
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
                tip_receiver: FUNNEL_CONFIG, // tip receiver will have just changed in previous ix
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
        SanitizedTransaction::try_from_legacy_transaction(
            Transaction::new_signed_with_payer(
                &[become_receiver, change_block_builder_ix],
                Some(&keypair.pubkey()),
                &[keypair],
                bank.last_blockhash(),
            ),
            bank.get_reserved_account_keys(),
        )
        .unwrap()
    }

    /// Returns the balance of all the MEV tip accounts
    pub fn get_tip_account_balances(&self, bank: &Arc<Bank>) -> Vec<(Pubkey, u64)> {
        let accounts = self.get_tip_accounts();
        accounts
            .iter()
            .map(|account| {
                let balance = bank.get_balance(account);

                (*account, balance)
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
            .iter()
            .map(|account| {
                let account_data = bank.get_account(account).unwrap_or_default();
                let balance = bank.get_balance(account);
                let rent_exempt =
                    bank.get_minimum_balance_for_rent_exemption(account_data.data().len());
                // NOTE: don't unwrap here in case bug in on-chain program, don't want all validators to crash
                // if program gets stuck in bad state
                (*account, balance.checked_sub(rent_exempt).unwrap_or_else(|| {
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
        .collect::<Vec<SanitizedTransaction>>();

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
        let configured_funnel_receiver = self.get_funnel_account(bank)?.receiver;
        let my_funnel_receiver = self.get_my_tip_distribution_pda(bank.epoch());

        let maybe_change_tip_receiver_tx = if tip_payment_config.tip_receiver != FUNNEL_CONFIG
            || configured_funnel_receiver != my_funnel_receiver
            || tip_payment_config.block_builder != block_builder_fee_info.block_builder
            || tip_payment_config.block_builder_commission_pct
                != block_builder_fee_info.block_builder_commission
        {
            debug!("change_tip_receiver=true");
            Some(self.change_tip_receiver_and_block_builder_tx(
                &my_funnel_receiver,
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
        .collect::<Vec<SanitizedTransaction>>();

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

    fn compute_additional_lamports(&self, bank: &Bank) -> u64 {
        let identity = self.my_identity();
        let current_slot = bank.slot();
        let current_epoch = bank.epoch();
        let previous_epoch = bank.epoch().checked_sub(1);

        // Lookup last payment slot, defaulting to current slot.
        let last_paid_slot = self.highest_paid(bank, &identity).unwrap_or(current_slot);
        let (last_paid_epoch, last_paid_offset) = bank.get_epoch_and_slot_index(last_paid_slot);

        // We only process the current and previous epoch. If no payment was
        // made in either of these we just assume 0.
        match (last_paid_epoch, previous_epoch) {
            (_, Some(previous_epoch)) if last_paid_epoch >= previous_epoch => {}
            (_, None) if last_paid_epoch >= current_epoch => {}
            _ => return 0,
        }

        // Get the leader schedule (should never fail).
        let Some(leader_schedule) = self
            .leader_schedule_cache
            .get_epoch_leader_schedule(last_paid_epoch)
        else {
            eprintln!("BUG: Missing leader schedule for last_paid_epoch");

            return 0;
        };

        // Binary search to find the index of this slot in our leader slots.
        let last_paid_offset = last_paid_offset as usize;
        let indexes = leader_schedule
            .get_index()
            .get(&identity)
            .cloned()
            .unwrap_or_default();
        let Ok(start_index) = indexes.binary_search(&(last_paid_offset as usize)) else {
            eprintln!(
                "BUG: highest_paid_offset not in indexes; identity={identity}; indexes={indexes:?}"
            );

            return 0;
        };

        // If our last paid epoch was not the current epoch, then search all the
        // processed slots in this epoch.
        let first_slot_in_current_epoch =
            bank.epoch_schedule().get_first_slot_in_epoch(current_epoch);
        let additional_indexes = match last_paid_epoch == current_epoch {
            true => Arc::default(),
            false => {
                let leader_schedule = match self
                    .leader_schedule_cache
                    .get_epoch_leader_schedule(current_epoch)
                {
                    Some(schedule) => schedule,
                    None => {
                        eprintln!("BUG: Current leader schedule missing?");
                        return 0;
                    }
                };

                leader_schedule
                    .get_index()
                    .get(&identity)
                    .cloned()
                    .unwrap_or_default()
            }
        };

        // Iterator that converts offsets to slots.
        let first_slot_in_epoch = bank
            .epoch_schedule()
            .get_first_slot_in_epoch(last_paid_epoch);
        let indexes_to_search = indexes[start_index..]
            .iter()
            .map(|offset| first_slot_in_epoch + *offset as u64)
            .chain(
                additional_indexes
                    .iter()
                    .map(|offset| first_slot_in_current_epoch + *offset as u64),
            );

        // Sum our outstanding rewards (i.e. rewards that have not been split
        // with the funnel).
        let mut outstanding_rewards = 0;
        for slot in indexes_to_search {
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

        calculate_funnel_take(outstanding_rewards)
    }

    fn highest_paid(&self, bank: &Bank, identity: &Pubkey) -> Option<Slot> {
        bank.get_account(&funnel::find_leader_state(identity).0)
            .and_then(|account| {
                funnel::LeaderState::try_from_bytes(account.data())
                    .ok()
                    .map(|state| state.last_slot)
            })
    }

    fn my_identity(&self) -> Pubkey {
        self.cluster_info.id()
    }
}

pub(crate) trait ReadRewards {
    fn read_rewards(&self, slot: Slot) -> u64;
}

impl ReadRewards for Blockstore {
    fn read_rewards(&self, slot: Slot) -> u64 {
        self.read_rewards(slot)
            .ok()
            .flatten()
            .unwrap_or(vec![])
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
        solana_gossip::contact_info::ContactInfo,
        solana_ledger::leader_schedule::LeaderSchedule,
        solana_program_test::programs::spl_programs,
        solana_runtime::genesis_utils::create_genesis_config_with_leader_ex,
        solana_sdk::{
            account::Account,
            fee_calculator::{FeeRateGovernor, DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE},
            genesis_config::ClusterType,
            native_token::sol_to_lamports,
            rent::Rent,
        },
        solana_streamer::socket::SocketAddrSpace,
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
        bank: Bank,
        blockstore: Arc<RwLock<MockBlockstore>>,
        leader_keypair: Arc<Keypair>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        tip_manager: TipManager,
    }

    fn create_fixture() -> TestFixture {
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
            &solana_sdk::pubkey::new_rand(),
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
        let mut rng = rand::thread_rng();
        let blockstore = Arc::new(RwLock::new(MockBlockstore::default()));
        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_rand(&mut rng, Some(leader_keypair.pubkey())),
            leader_keypair.clone(),
            SocketAddrSpace::Unspecified,
        ));
        let bank = Bank::new_for_tests(&genesis_config);
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));
        let config = TipManagerConfig::default();

        // Setup the leader state.
        let (leader_state, _) = funnel::find_leader_state(&leader_keypair.pubkey());
        bank.store_account(
            &leader_state,
            &Account {
                lamports: rent.minimum_balance(LeaderState::LEN),
                data: LeaderState { last_slot: 0 }.as_bytes().to_vec(),
                ..Account::default()
            }
            .into(),
        );

        TestFixture {
            bank,
            blockstore: blockstore.clone(),
            leader_keypair,
            leader_schedule_cache: leader_schedule_cache.clone(),
            tip_manager: TipManager::new(blockstore, cluster_info, leader_schedule_cache, config),
        }
    }

    #[test]
    fn compute_additional_lamports_base() {
        // Arrange.
        let fixture = create_fixture();

        // Act.
        let additional = fixture
            .tip_manager
            .compute_additional_lamports(&fixture.bank);

        // Assert.
        assert_eq!(additional, 0);
    }

    #[test]
    fn compute_additional_lamports_prior_slot_no_rewards() {
        // Arrange.
        let fixture = create_fixture();
        let child_bank =
            Bank::new_from_parent(Arc::new(fixture.bank), &fixture.leader_keypair.pubkey(), 1);

        // Act.
        let additional = fixture.tip_manager.compute_additional_lamports(&child_bank);

        // Assert.
        assert_eq!(additional, 0);
    }

    #[test]
    fn compute_additional_lamports_prior_slot_rewards() {
        // Arrange.
        let fixture = create_fixture();
        let child_bank =
            Bank::new_from_parent(Arc::new(fixture.bank), &fixture.leader_keypair.pubkey(), 1);
        fixture.blockstore.write().unwrap().0.push(100);

        // Act.
        let additional = fixture.tip_manager.compute_additional_lamports(&child_bank);

        // Assert.
        assert_eq!(additional, calculate_funnel_take(100));
    }

    #[test]
    fn compute_additional_lamports_prior_slot_not_our_leader() {
        // Arrange.
        let fixture = create_fixture();
        let child_bank =
            Bank::new_from_parent(Arc::new(fixture.bank), &fixture.leader_keypair.pubkey(), 1);
        fixture.blockstore.write().unwrap().0.push(100);
        let mut slot_leaders = fixture
            .leader_schedule_cache
            .get_epoch_leader_schedule(0)
            .unwrap()
            .get_slot_leaders()
            .to_vec();
        slot_leaders[0] = Pubkey::new_unique();
        let leader_schedule = LeaderSchedule::new_from_schedule(slot_leaders);
        *fixture
            .leader_schedule_cache
            .cached_schedules
            .write()
            .unwrap()
            .0
            .get_mut(&0)
            .unwrap() = Arc::new(leader_schedule);

        // Act.
        let additional = fixture.tip_manager.compute_additional_lamports(&child_bank);

        // Assert.
        assert_eq!(additional, 0);
    }

    #[test]
    fn compute_additional_lamports_multiple_prior_slots_with_gaps() {
        // Arrange.
        let fixture = create_fixture();

        // Create a bank at slot 8.
        let child_bank =
            Bank::new_from_parent(Arc::new(fixture.bank), &fixture.leader_keypair.pubkey(), 8);
        fixture
            .blockstore
            .write()
            .unwrap()
            .0
            .extend(std::iter::repeat(100).take(8));

        // Update the leader schedule
        let mut slot_leaders = fixture
            .leader_schedule_cache
            .get_epoch_leader_schedule(0)
            .unwrap()
            .get_slot_leaders()
            .to_vec();
        slot_leaders[1] = Pubkey::new_unique();
        slot_leaders[2] = Pubkey::new_unique();
        slot_leaders[4] = Pubkey::new_unique();
        slot_leaders[6] = Pubkey::new_unique();
        let leader_schedule = LeaderSchedule::new_from_schedule(slot_leaders);
        *fixture
            .leader_schedule_cache
            .cached_schedules
            .write()
            .unwrap()
            .0
            .get_mut(&0)
            .unwrap() = Arc::new(leader_schedule);

        // Act.
        let additional = fixture.tip_manager.compute_additional_lamports(&child_bank);

        // Assert.
        assert_eq!(additional, calculate_funnel_take(400));
    }

    #[test]
    fn compute_additional_lamports_use_slots_from_previous_epoch() {
        // Arrange.
        let fixture = create_fixture();

        // Set the block reward to to the slot index.
        fixture
            .blockstore
            .write()
            .unwrap()
            .0
            .extend((0..64).map(|i| i * 100));

        // Set all slots to random leaders.
        let mut epoch_0_leaders = fixture
            .leader_schedule_cache
            .get_epoch_leader_schedule(0)
            .unwrap()
            .get_slot_leaders()
            .to_vec();
        epoch_0_leaders
            .iter_mut()
            .for_each(|leader| *leader = Pubkey::new_unique());

        // Set 3 slots for our leader.
        epoch_0_leaders[0] = fixture.leader_keypair.pubkey();
        epoch_0_leaders[12] = fixture.leader_keypair.pubkey();
        epoch_0_leaders[20] = fixture.leader_keypair.pubkey();

        // Update the leader schedule.
        *fixture
            .leader_schedule_cache
            .cached_schedules
            .write()
            .unwrap()
            .0
            .get_mut(&0)
            .unwrap() = Arc::new(LeaderSchedule::new_from_schedule(epoch_0_leaders));

        // Roll to the next epoch.
        let next_epoch =
            Bank::new_from_parent(Arc::new(fixture.bank), &fixture.leader_keypair.pubkey(), 33);
        assert_eq!(next_epoch.epoch(), 1);

        // Act.
        let additional = fixture.tip_manager.compute_additional_lamports(&next_epoch);

        // Assert.
        assert_eq!(additional, calculate_funnel_take(0 + 1200 + 2000 + 3200));
    }
}
