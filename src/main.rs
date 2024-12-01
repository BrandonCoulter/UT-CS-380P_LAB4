#[macro_use]
#[allow(unused)]
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
extern crate ipc_channel;
use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::process::{Child,Command};
use ipc_channel::ipc;
use ipc_channel::ipc::IpcSender as Sender;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::channel;
use stderrlog::new;
use std::thread;
use std::time::Duration;
pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
use message::ProtocolMessage;

///
/// pub fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     child_opts: CLI options for child process
///
/// 1. Set up IPC
/// 2. Spawn a child process using the child CLI options
/// 3. Do any required communication to set up the parent / child communication channels
/// 4. Return the child process handle and the communication channels for the parent
///
/// HINT: You can change the signature of the function if necessary
///
fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    trace!("Spawning child and connecting to coordinator");
    // 1. Set up IPC
    let (server, server_name): (IpcOneShotServer<ProtocolMessage>, String) = IpcOneShotServer::new().unwrap();
    child_opts.ipc_path = server_name.clone();

    info!("Setting up IPC Server at {}.", server_name);

    // 2. Spawn a child process using the child CLI options
    let child = Command::new(env::current_exe().unwrap())
        .args(child_opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");

    info!("Spawned child process with PID: {}.", child.id());

    info!("Attempting to accept IPC connection in Coordinator");
    let (child_rx, accept_msg) = server.accept().unwrap();

    // 3. Accept the IPC connection from the child process
    info!("Server Accept Message: {:?}", accept_msg);

    info!("Creating Child Channels");
    let child_tx : Sender<ProtocolMessage> = Sender::<ProtocolMessage>::connect(child_opts.ipc_path.clone()).unwrap();
    
    // 4. Return the child process handle and the communication channels for the parent
    trace!("Child channels created, returning.");
    return (child, child_tx, child_rx)
}








///
/// pub fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     opts: CLI options for this process
///
/// 1. Connect to the parent via IPC
/// 2. Do any required communication to set up the parent / child communication channels
/// 3. Return the communication channels for the child
///
/// HINT: You can change the signature of the function if necessasry
///
fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    trace!("Conntecting to coordinator");
    let ipc_path = opts.ipc_path.to_string();
    info!("Connecting to IPC server at {}.", ipc_path);
    let coordinator_tx = Sender::<ProtocolMessage>::connect(ipc_path).unwrap();
    info!("Sender channel connected to Coordinator.");

    info!("Creating child tx/rx channels.");
    let (_, child_rx): (_, Receiver<ProtocolMessage>) = channel().unwrap();

    trace!("Connected to coordinator");
    return (coordinator_tx, child_rx)
}

///
/// pub fn run(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Creates a new coordinator
/// 2. Spawns and connects to new clients processes and then registers them with
///    the coordinator
/// 3. Spawns and connects to new participant processes and then registers them
///    with the coordinator
/// 4. Starts the coordinator protocol
/// 5. Wait until the children finish execution
///
fn run(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let coord_log_path = format!("{}//{}", opts.log_path, "coordinator.log");

    // 1. Creates a new coordinator
    let mut coordinator = coordinator::Coordinator::new(coord_log_path.clone(), &running);

    info!("Coordinator Created");

    // 2. Spawns and connects to new clients processes and then registers them with
    //    the coordinator
    let mut clients: Vec<Child> = Vec::new();

    for i in 0..opts.num_clients {
        
        let mut client_opts = opts.clone();
        client_opts.mode = "client".to_string();
        client_opts.num = i;

        info!("Spawning Client {}", i);

        let (child, tx, rx) = spawn_child_and_connect(&mut client_opts);
        let name = format!("client_{}", i);
        coordinator.client_join(&name, tx, rx);

        clients.push(child);

    }

    info!("All Clients Spawned");

    // 3. Spawns and connects to new participant processes and then registers them
    //    with the coordinator
    let mut participants: Vec<Child> = Vec::new();

    for i in 0..opts.num_participants {
        let mut participant_opts = opts.clone();
        participant_opts.mode = "participant".to_string();
        participant_opts.num = i;

        info!("Spawning Participant {}", i);

        let (child, tx, rx) = spawn_child_and_connect(&mut participant_opts);
        let name = format!("participant_{}", i);

        coordinator.participant_join(&name, tx, rx);
        info!("Participant {} joined", i);

        participants.push(child);
    }

    info!("All Participants Spawned");


    // 4. Starts the coordinator protocol
    coordinator.protocol();


    info!("Waiting on Clients");
    // 5. Wait until the children finish execution
    for mut client in clients{
        let _ = client.wait();
    }

    info!("Waiting on Participants");
    for mut participant in participants{
        let _ = participant.wait();
    }

}

///
/// pub fn run_client(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new client
/// 3. Starts the client protocol
///
fn run_client(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    // 1. Connects to the coordinator to get tx/rx
    let (client_tx, client_rx) = connect_to_coordinator(opts);
    
    // 2. Constructs a new client
    let client_id_str = format!("client_{}", opts.num);
    let mut client = client::Client::new(client_id_str.clone(), running.clone(), client_tx.clone(), client_rx);

    // 3. Starts the client protocol
    client.protocol(opts.num_requests);

}

///
/// pub fn run_participant(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new participant
/// 3. Starts the participant protocol
///
fn run_participant(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    // 1. Connects to the coordinator to get tx/rx
    let (participant_tx, participant_rx) = connect_to_coordinator(opts);
    
    // 2. Constructs a new participant
    let participant_id_str = format!("participant_{}", opts.num);
    let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str);

    let mut participant = participant::Participant::new(participant_id_str.clone(),
        participant_log_path.clone(),
        running.clone(),
        opts.send_success_probability.clone(),
        opts.operation_success_probability.clone(),
        participant_tx.clone(),
        participant_rx);

    info!("Stating participant protocol for {}", participant_id_str.clone());

    // 3. Starts the participant protocol
    participant.protocol();
}

fn main() {
    // Parse CLI arguments
    let opts = tpcoptions::TPCOptions::new();

    // Print out options for debug verification purposes
    if opts.debug_verbosity > 0 {
        opts.print_opts();
    }
    
    // Set-up logging and create OpLog path if necessary
    stderrlog::new()
            .module(module_path!())
            .quiet(false)
            .timestamp(stderrlog::Timestamp::Millisecond)
            .verbosity(opts.verbosity)
            .init()
            .unwrap();
    match fs::create_dir_all(opts.log_path.clone()) {
        Err(e) => error!("Failed to create log_path: \"{:?}\". Error \"{:?}\"", opts.log_path, e),
        _ => (),
    }

    // Set-up Ctrl-C / SIGINT handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = opts.mode.clone();

    info!("Initializing Ctrl-C Handler");
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        if m == "run" {
            print!("\n");
        }
    }).expect("Error setting signal handler!");

    // Execute main logic
    match opts.mode.as_ref() {
        "run" => run(&opts, running),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => checker::check_last_run(opts.num_clients, opts.num_requests, opts.num_participants, &opts.log_path),
        _ => panic!("Unknown mode"),
    }
}
