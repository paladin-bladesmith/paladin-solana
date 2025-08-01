use {
    crossbeam_channel::Receiver as CrossbeamReceiver,
    futures::future::BoxFuture,
    solana_cli_config::ConfigInput,
    solana_commitment_config::CommitmentConfig,
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_signer::Signer,
    solana_streamer::{
        nonblocking::testing_utilities::{
            make_client_endpoint, setup_quic_server, SpawnTestServerResult, TestServerConfig,
        },
        packet::PacketBatch,
        streamer::StakedNodes,
    },
    solana_tpu_client_next::{
        connection_workers_scheduler::{
            BindTarget, ConnectionWorkersSchedulerConfig, Fanout, StakeIdentity,
        },
        leader_updater::create_leader_updater,
        send_transaction_stats::SendTransactionStatsNonAtomic,
        transaction_batch::TransactionBatch,
        ConnectionWorkersScheduler, ConnectionWorkersSchedulerError, SendTransactionStats,
    },
    std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        num::Saturating,
        sync::{atomic::Ordering, Arc},
        time::Duration,
    },
    tokio::{
        sync::{
            mpsc::{channel, Receiver},
            oneshot, watch,
        },
        task::JoinHandle,
        time::{sleep, Instant},
    },
    tokio_util::sync::CancellationToken,
};

fn test_config(stake_identity: Option<Keypair>) -> ConnectionWorkersSchedulerConfig {
    let address = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 0);
    ConnectionWorkersSchedulerConfig {
        bind: BindTarget::Address(address),
        stake_identity: stake_identity.map(|identity| StakeIdentity::new(&identity)),
        num_connections: 1,
        skip_check_transaction_age: false,
        // At the moment we have only one strategy to send transactions: we try
        // to put to worker channel transaction batch and in case of failure
        // just drop it. This requires to use large channels here. In the
        // future, we are planning to add an option to send with backpressure at
        // the speed of fastest leader.
        worker_channel_size: 100,
        max_reconnect_attempts: 4,
        leaders_fanout: Fanout {
            send: 1,
            connect: 1,
        },
    }
}

async fn setup_connection_worker_scheduler(
    tpu_address: SocketAddr,
    transaction_receiver: Receiver<TransactionBatch>,
    stake_identity: Option<Keypair>,
) -> (
    JoinHandle<Result<Arc<SendTransactionStats>, ConnectionWorkersSchedulerError>>,
    watch::Sender<Option<StakeIdentity>>,
    CancellationToken,
) {
    let json_rpc_url = "http://127.0.0.1:8899";
    let (_, websocket_url) = ConfigInput::compute_websocket_url_setting("", "", json_rpc_url, "");

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        json_rpc_url.to_string(),
        CommitmentConfig::confirmed(),
    ));

    // Setup sending txs
    let leader_updater = create_leader_updater(rpc_client, websocket_url, Some(tpu_address))
        .await
        .expect("Leader updates was successfully created");

    let cancel = CancellationToken::new();
    let (update_identity_sender, update_identity_receiver) = watch::channel(None);
    let scheduler = ConnectionWorkersScheduler::new(
        leader_updater,
        transaction_receiver,
        update_identity_receiver,
        cancel.clone(),
    );
    let config = test_config(stake_identity);
    let scheduler = tokio::spawn(scheduler.run(config));

    (scheduler, update_identity_sender, cancel)
}

async fn join_scheduler(
    scheduler_handle: JoinHandle<
        Result<Arc<SendTransactionStats>, ConnectionWorkersSchedulerError>,
    >,
) -> SendTransactionStatsNonAtomic {
    let scheduler_stats = scheduler_handle
        .await
        .unwrap()
        .expect("Scheduler should stop successfully.");
    scheduler_stats.read_and_reset()
}

// Specify the pessimistic time to finish generation and result checks.
const TEST_MAX_TIME: Duration = Duration::from_millis(2500);

struct SpawnTxGenerator {
    tx_receiver: Receiver<TransactionBatch>,
    tx_sender_shutdown: BoxFuture<'static, ()>,
    tx_sender_done: oneshot::Receiver<()>,
}

/// Generates `num_tx_batches` batches of transactions, each holding a single transaction of
/// `tx_size` bytes.
///
/// It will not close the returned `tx_receiver` until `tx_sender_shutdown` is invoked.  Otherwise,
/// there is a race condition, that exists between the last transaction being scheduled for delivery
/// and the server connection being closed.
fn spawn_tx_sender(
    tx_size: usize,
    num_tx_batches: usize,
    time_per_tx: Duration,
) -> SpawnTxGenerator {
    let num_tx_batches: u32 = num_tx_batches
        .try_into()
        .expect("`num_tx_batches` fits into u32 for all the tests");
    let (tx_sender, tx_receiver) = channel(1);
    let cancel = CancellationToken::new();
    let (done_sender, tx_sender_done) = oneshot::channel();

    let sender = tokio::spawn({
        let start = Instant::now();

        let tx_sender = tx_sender.clone();

        let main_loop = async move {
            for i in 0..num_tx_batches {
                let txs = vec![vec![i as u8; tx_size]; 1];
                tx_sender
                    .send(TransactionBatch::new(txs))
                    .await
                    .expect("Receiver should not close their side");

                // Pretend the client runs at the specified TPS.
                let sleep_time = time_per_tx
                    .saturating_mul(i)
                    .saturating_sub(start.elapsed());
                if !sleep_time.is_zero() {
                    sleep(sleep_time).await;
                }
            }

            // It is OK if the receiver has disconnected.
            let _ = done_sender.send(());
        };

        let cancel = cancel.clone();
        async move {
            tokio::select! {
                () = main_loop => (),
                () = cancel.cancelled() => (),
            }
        }
    });

    let tx_sender_shutdown = Box::pin(async move {
        cancel.cancel();
        // This makes sure that the sender exists up until the shutdown is invoked.
        drop(tx_sender);

        sender.await.unwrap();
    });

    SpawnTxGenerator {
        tx_receiver,
        tx_sender_shutdown,
        tx_sender_done,
    }
}

#[tokio::test]
async fn test_basic_transactions_sending() {
    let SpawnTestServerResult {
        join_handle: server_handle,
        exit,
        receiver,
        server_address,
        stats: _stats,
    } = setup_quic_server(None, TestServerConfig::default());

    // Setup sending txs
    let tx_size = 1;
    let expected_num_txs: usize = 100;
    // Pretend that we are running at ~100 TPS.
    let SpawnTxGenerator {
        tx_receiver,
        tx_sender_shutdown,
        ..
    } = spawn_tx_sender(tx_size, expected_num_txs, Duration::from_millis(10));

    let (scheduler_handle, update_identity_sender, _scheduler_cancel) =
        setup_connection_worker_scheduler(server_address, tx_receiver, None).await;
    // dropping sender will not lead to stop the scheduler.
    drop(update_identity_sender);

    // Check results
    let mut received_data = Vec::with_capacity(expected_num_txs);
    let now = Instant::now();
    let mut actual_num_packets = 0;
    while actual_num_packets < expected_num_txs {
        {
            let elapsed = now.elapsed();
            assert!(
                elapsed < TEST_MAX_TIME,
                "Failed to send {} transaction in {:?}.  Only sent {}",
                expected_num_txs,
                elapsed,
                actual_num_packets,
            );
        }

        let Ok(packets) = receiver.try_recv() else {
            sleep(Duration::from_millis(10)).await;
            continue;
        };

        actual_num_packets += packets.len();
        for p in packets.iter() {
            let packet_id = p.data(0).expect("Data should not be lost by server.");
            received_data.push(*packet_id);
            assert_eq!(p.meta().size, 1);
        }
    }

    received_data.sort_unstable();
    for i in 1..received_data.len() {
        assert_eq!(received_data[i - 1] + 1, received_data[i]);
    }

    // Stop sending
    tx_sender_shutdown.await;
    let stats = join_scheduler(scheduler_handle).await;
    assert_eq!(stats.successfully_sent, expected_num_txs as u64,);

    // Stop server
    exit.store(true, Ordering::Relaxed);
    server_handle.await.unwrap();
}

async fn count_received_packets_for(
    receiver: CrossbeamReceiver<PacketBatch>,
    expected_tx_size: usize,
    receive_duration: Duration,
) -> usize {
    let now = Instant::now();
    let mut num_packets_received = Saturating(0usize);

    while now.elapsed() < receive_duration {
        if let Ok(packets) = receiver.try_recv() {
            num_packets_received += packets.len();
            for p in packets.iter() {
                assert_eq!(p.meta().size, expected_tx_size);
            }
        } else {
            sleep(Duration::from_millis(100)).await;
        }
    }

    num_packets_received.0
}

// Check that client can create connection even if the first several attempts were unsuccessful.
#[tokio::test]
async fn test_connection_denied_until_allowed() {
    let SpawnTestServerResult {
        join_handle: server_handle,
        exit,
        receiver,
        server_address,
        stats: _stats,
    } = setup_quic_server(None, TestServerConfig::default());

    // To prevent server from accepting a new connection, we use the following observation.
    // Since max_connections_per_peer == 1 (< max_unstaked_connections == 500), if we create a first
    // connection and later try another one, the second connection will be immediately closed.
    //
    // Since client is not retrying sending failed transactions, this leads to the packets loss.
    // The connection has been created and closed when we already have sent the data.
    let throttling_connection = make_client_endpoint(&server_address, None).await;

    // Setup sending txs
    let tx_size = 1;
    let expected_num_txs: usize = 10;
    let SpawnTxGenerator {
        tx_receiver,
        tx_sender_shutdown,
        ..
    } = spawn_tx_sender(tx_size, expected_num_txs, Duration::from_millis(100));

    let (scheduler_handle, _update_identity_sender, _scheduler_cancel) =
        setup_connection_worker_scheduler(server_address, tx_receiver, None).await;

    // Check results
    let actual_num_packets = count_received_packets_for(receiver, tx_size, TEST_MAX_TIME).await;
    assert!(
        actual_num_packets < expected_num_txs,
        "Expected to receive {expected_num_txs} packets in {TEST_MAX_TIME:?}\n\
         Got packets: {actual_num_packets}"
    );

    // Wait for the exchange to finish.
    tx_sender_shutdown.await;
    let stats = join_scheduler(scheduler_handle).await;
    // in case of pruning, server closes the connection with code 1 and error
    // message b"dropped". This might lead to connection error
    // (ApplicationClosed::ApplicationClose) or to stream error
    // (ConnectionLost::ApplicationClosed::ApplicationClose).
    assert_eq!(
        stats.write_error_connection_lost + stats.connection_error_application_closed,
        1
    );

    drop(throttling_connection);

    // Exit server
    exit.store(true, Ordering::Relaxed);
    server_handle.await.unwrap();
}

// Check that if the client connection has been pruned, client manages to
// reestablish it. Pruning will lead to 1 packet loss, because when we send the
// next packet we will reestablish connection.
#[tokio::test]
async fn test_connection_pruned_and_reopened() {
    let SpawnTestServerResult {
        join_handle: server_handle,
        exit,
        receiver,
        server_address,
        stats: _stats,
    } = setup_quic_server(
        None,
        TestServerConfig {
            max_connections_per_peer: 100,
            max_unstaked_connections: 1,
            ..Default::default()
        },
    );

    // Setup sending txs
    let tx_size = 1;
    let expected_num_txs: usize = 16;
    let SpawnTxGenerator {
        tx_receiver,
        tx_sender_shutdown,
        ..
    } = spawn_tx_sender(tx_size, expected_num_txs, Duration::from_millis(100));

    let (scheduler_handle, _update_identity_sender, _scheduler_cancel) =
        setup_connection_worker_scheduler(server_address, tx_receiver, None).await;

    sleep(Duration::from_millis(400)).await;
    let _connection_to_prune_client = make_client_endpoint(&server_address, None).await;

    // Check results
    let actual_num_packets = count_received_packets_for(receiver, tx_size, TEST_MAX_TIME).await;
    assert!(actual_num_packets < expected_num_txs);

    // Wait for the exchange to finish.
    tx_sender_shutdown.await;
    let stats = join_scheduler(scheduler_handle).await;
    // in case of pruning, server closes the connection with code 1 and error
    // message b"dropped". This might lead to connection error
    // (ApplicationClosed::ApplicationClose) or to stream error
    // (ConnectionLost::ApplicationClosed::ApplicationClose).
    assert_eq!(
        stats.connection_error_application_closed + stats.write_error_connection_lost,
        1,
    );

    // Exit server
    exit.store(true, Ordering::Relaxed);
    server_handle.await.unwrap();
}

/// Check that client creates staked connection. To do that prohibit unstaked
/// connection and verify that all the txs has been received.
#[tokio::test]
async fn test_staked_connection() {
    let stake_identity = Keypair::new();
    let stakes = HashMap::from([(stake_identity.pubkey(), 100_000)]);
    let staked_nodes = StakedNodes::new(Arc::new(stakes), HashMap::<Pubkey, u64>::default());

    let SpawnTestServerResult {
        join_handle: server_handle,
        exit,
        receiver,
        server_address,
        stats: _stats,
    } = setup_quic_server(
        Some(staked_nodes),
        TestServerConfig {
            // Must use at least the number of endpoints (10) because
            // `max_staked_connections` and `max_unstaked_connections` are
            // cumulative for all the endpoints.
            max_staked_connections: 10,
            max_unstaked_connections: 0,
            ..Default::default()
        },
    );

    // Setup sending txs
    let tx_size = 1;
    let expected_num_txs: usize = 10;
    let SpawnTxGenerator {
        tx_receiver,
        tx_sender_shutdown,
        ..
    } = spawn_tx_sender(tx_size, expected_num_txs, Duration::from_millis(100));

    let (scheduler_handle, _update_certificate_sender, _scheduler_cancel) =
        setup_connection_worker_scheduler(server_address, tx_receiver, Some(stake_identity)).await;

    // Check results
    let actual_num_packets = count_received_packets_for(receiver, tx_size, TEST_MAX_TIME).await;
    assert_eq!(actual_num_packets, expected_num_txs);

    // Wait for the exchange to finish.
    tx_sender_shutdown.await;
    let stats = join_scheduler(scheduler_handle).await;
    assert_eq!(
        stats,
        SendTransactionStatsNonAtomic {
            successfully_sent: expected_num_txs as u64,
            ..Default::default()
        }
    );

    // Exit server
    exit.store(true, Ordering::Relaxed);
    server_handle.await.unwrap();
}

// Check that if client sends transactions at a reasonably high rate that is
// higher than what the server accepts, nevertheless all the transactions are
// delivered and there are no errors on the client side.
#[tokio::test]
async fn test_connection_throttling() {
    let SpawnTestServerResult {
        join_handle: server_handle,
        exit,
        receiver,
        server_address,
        stats: _stats,
    } = setup_quic_server(None, TestServerConfig::default());

    // Setup sending txs
    let tx_size = 1;
    let expected_num_txs: usize = 50;
    // Send at 1000 TPS - x10 more than the throttling interval of 10ms used in other tests allows.
    let SpawnTxGenerator {
        tx_receiver,
        tx_sender_shutdown,
        ..
    } = spawn_tx_sender(tx_size, expected_num_txs, Duration::from_millis(1));

    let (scheduler_handle, _update_certificate_sender, _scheduler_cancel) =
        setup_connection_worker_scheduler(server_address, tx_receiver, None).await;

    // Check results
    let actual_num_packets =
        count_received_packets_for(receiver, tx_size, Duration::from_secs(1)).await;
    assert_eq!(actual_num_packets, expected_num_txs);

    // Stop sending
    tx_sender_shutdown.await;
    let stats = join_scheduler(scheduler_handle).await;
    assert_eq!(
        stats,
        SendTransactionStatsNonAtomic {
            successfully_sent: expected_num_txs as u64,
            ..Default::default()
        }
    );

    // Exit server
    exit.store(true, Ordering::Relaxed);
    server_handle.await.unwrap();
}

// Check that when the host cannot be reached, the client exits gracefully.
#[tokio::test]
async fn test_no_host() {
    // A "black hole" address for the TPU.
    let server_ip = IpAddr::V6(Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, 0, 1));
    let server_address = SocketAddr::new(server_ip, 49151);

    // Setup sending side.
    let tx_size = 1;
    let max_send_attempts: usize = 10;

    let SpawnTxGenerator {
        tx_receiver,
        tx_sender_shutdown,
        tx_sender_done,
        ..
    } = spawn_tx_sender(tx_size, max_send_attempts, Duration::from_millis(10));

    let (scheduler_handle, _update_certificate_sender, _scheduler_cancel) =
        setup_connection_worker_scheduler(server_address, tx_receiver, None).await;

    // Wait for all the transactions to be sent, and some extra time for the delivery to be
    // attempted.
    tx_sender_done.await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Wait for the generator to finish.
    tx_sender_shutdown.await;

    // While attempting to establish a connection with a nonexistent host, we fill the worker's
    // channel.
    let stats = join_scheduler(scheduler_handle).await;
    // `5` because `config.max_reconnect_attempts` is 4
    assert_eq!(stats.connect_error_invalid_remote_address, 5);
}

// Check that when the client is rate-limited by server, we update counters
// accordingly. To implement it we:
// * set the connection limit per minute to 1
// * create a dummy connection to reach the limit and immediately close it
// * set up client which will try to create a new connection which it will be
// rate-limited. This test doesn't check what happens when the rate-limiting
// period ends because it too long for test (1min).
#[tokio::test]
async fn test_rate_limiting() {
    let SpawnTestServerResult {
        join_handle: server_handle,
        exit,
        receiver,
        server_address,
        stats: _stats,
    } = setup_quic_server(
        None,
        TestServerConfig {
            max_connections_per_peer: 100,
            max_connections_per_ipaddr_per_min: 1,
            ..Default::default()
        },
    );

    // open a connection to consume the limit
    let connection_to_reach_limit = make_client_endpoint(&server_address, None).await;
    drop(connection_to_reach_limit);

    // Setup sending txs which are full packets in size
    let tx_size = 1024;
    let expected_num_txs: usize = 16;
    let SpawnTxGenerator {
        tx_receiver,
        tx_sender_shutdown,
        ..
    } = spawn_tx_sender(tx_size, expected_num_txs, Duration::from_millis(100));

    let (scheduler_handle, _update_certificate_sender, scheduler_cancel) =
        setup_connection_worker_scheduler(server_address, tx_receiver, None).await;

    let actual_num_packets = count_received_packets_for(receiver, tx_size, TEST_MAX_TIME).await;
    assert_eq!(actual_num_packets, 0);

    // Stop the sender.
    tx_sender_shutdown.await;

    // And the scheduler.
    scheduler_cancel.cancel();
    let stats = join_scheduler(scheduler_handle).await;

    // we get 2 transactions registered as sent (but not acked) because of how QUIC works
    // before ratelimiter kicks in.
    assert!(
        stats
            == SendTransactionStatsNonAtomic {
                successfully_sent: 2,
                write_error_connection_lost: 2,
                ..Default::default()
            }
    );

    // Stop the server.
    exit.store(true, Ordering::Relaxed);
    server_handle.await.unwrap();
}

// The same as test_rate_limiting but here we wait for 1 min to check that the
// connection has been established.
#[tokio::test]
// TODO Provide an alternative testing interface for `streamer::nonblocking::quic::spawn_server`
// that would accept throttling at a granularity below 1 minute.
#[ignore = "takes 70s to complete"]
async fn test_rate_limiting_establish_connection() {
    let SpawnTestServerResult {
        join_handle: server_handle,
        exit,
        receiver,
        server_address,
        stats: _stats,
    } = setup_quic_server(
        None,
        TestServerConfig {
            max_connections_per_peer: 100,
            max_connections_per_ipaddr_per_min: 1,
            ..Default::default()
        },
    );

    let connection_to_reach_limit = make_client_endpoint(&server_address, None).await;
    drop(connection_to_reach_limit);

    // Setup sending txs
    let tx_size = 1;
    let expected_num_txs: usize = 65;
    let SpawnTxGenerator {
        tx_receiver,
        tx_sender_shutdown,
        ..
    } = spawn_tx_sender(tx_size, expected_num_txs, Duration::from_millis(1000));

    let (scheduler_handle, _update_certificate_sender, scheduler_cancel) =
        setup_connection_worker_scheduler(server_address, tx_receiver, None).await;

    let actual_num_packets =
        count_received_packets_for(receiver, tx_size, Duration::from_secs(70)).await;
    assert!(
        actual_num_packets > 0,
        "As we wait longer than 1 minute, at least one transaction should be delivered.  \
         After 1 minute the server is expected to accept our connection.\n\
         Actual packets delivered: {actual_num_packets}"
    );

    // Stop the sender.
    tx_sender_shutdown.await;

    // And the scheduler.
    scheduler_cancel.cancel();
    let mut stats = join_scheduler(scheduler_handle).await;
    assert!(
        stats.connection_error_timed_out > 0,
        "As the quinn timeout is below 1 minute, a few connections will fail to connect during \
         the 1 minute delay.\n\
         Actual connection_error_timed_out: {}",
        stats.connection_error_timed_out
    );
    assert!(
        stats.successfully_sent > 0,
        "As we run the test for longer than 1 minute, we expect a connection to be established, \
         and a number of transactions to be delivered.\n\
         Actual successfully_sent: {}",
        stats.successfully_sent
    );

    // All the rest of the error counters should be 0.
    stats.connection_error_timed_out = 0;
    stats.successfully_sent = 0;
    assert_eq!(stats, SendTransactionStatsNonAtomic::default());

    // Stop the server.
    exit.store(true, Ordering::Relaxed);
    server_handle.await.unwrap();
}

// Check that identity is updated successfully using corresponding channel.
//
// Since the identity update and the transactions are sent concurrently to their channels
// and scheduler selects randomly which channel to handle first, we cannot
// guarantee in this test that the identity has been updated before we start
// sending transactions. Hence, instead of checking that all the transactions
// have been delivered, we check that at least some have been.
#[tokio::test]
async fn test_update_identity() {
    let stake_identity = Keypair::new();
    let stakes = HashMap::from([(stake_identity.pubkey(), 100_000)]);
    let staked_nodes = StakedNodes::new(Arc::new(stakes), HashMap::<Pubkey, u64>::default());

    let SpawnTestServerResult {
        join_handle: server_handle,
        exit,
        receiver,
        server_address,
        stats: _stats,
    } = setup_quic_server(
        Some(staked_nodes),
        TestServerConfig {
            // Must use at least the number of endpoints (10) because
            // `max_staked_connections` and `max_unstaked_connections` are
            // cumulative for all the endpoints.
            max_staked_connections: 10,
            // Deny all unstaked connections.
            max_unstaked_connections: 0,
            ..Default::default()
        },
    );

    // Setup sending txs
    let tx_size = 1;
    let num_txs: usize = 100;
    let SpawnTxGenerator {
        tx_receiver,
        tx_sender_shutdown,
        ..
    } = spawn_tx_sender(tx_size, num_txs, Duration::from_millis(50));

    let (scheduler_handle, update_identity_sender, scheduler_cancel) =
        setup_connection_worker_scheduler(
            server_address,
            tx_receiver,
            // Create scheduler with unstaked identity.
            None,
        )
        .await;
    // Update identity.
    update_identity_sender
        .send(Some(StakeIdentity::new(&stake_identity)))
        .unwrap();

    let actual_num_packets = count_received_packets_for(receiver, tx_size, TEST_MAX_TIME).await;
    assert!(actual_num_packets > 0);

    // Stop the sender.
    tx_sender_shutdown.await;

    // And the scheduler.
    scheduler_cancel.cancel();

    let stats = join_scheduler(scheduler_handle).await;
    assert!(stats.successfully_sent > 0);

    // Exit server
    exit.store(true, Ordering::Relaxed);
    server_handle.await.unwrap();
}
