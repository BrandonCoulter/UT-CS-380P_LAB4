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

    // thread::sleep(Duration::from_secs(1));

    // 3. Accept the IPC connection from the child process
    let (rx, rx_msg): (Receiver<ProtocolMessage>, ProtocolMessage) = server.accept().unwrap();

    info!("Server Accept Message: {:?}", rx_msg);

    let (tx, rx_local) = channel().unwrap();

    return (child, tx, rx_local)


    // match server.accept() {
    //     Ok((rx, _)) => {
    //         info!("Accepted IPC connection from child.");
    //         let (tx, rx_local) = channel().unwrap();
    //         info!("Created IPC communication channels");

    //         // 4. Return the child process handle and the communication channels for the parent
    //         return (child, tx, rx_local)
    //     }
    //     Err(e) => {
    //         info!("Failed to accept IPC connection while spawning child: {:?}", e);
    //         panic!("Failed to accept IPC connection while spawning child.");
    //     }
    // }
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
    let ipc_path = opts.ipc_path.to_string();
    info!("Connecting to IPC server at {}.", ipc_path);

    let c_tx = Sender::<ProtocolMessage>::connect(ipc_path).unwrap();

    info!("Sender channel connected to Coordinator.");

    let (tx, c_rx) = channel().unwrap();

    info!("Creating child tx/rx channels.");

    return (c_tx, c_rx)

    


    // Retry mechanism to connect to the IPC server
    // let mut connected = false;
    // let mut attempts = 0;
    // let max_attempts = 5;

    // while !connected && attempts < max_attempts {
    //     match Sender::<ProtocolMessage>::connect(ipc_path.clone()) {
    //         Ok(tx) => {
    //             let (local_tx, local_rx) = channel().unwrap();
    //             info!("Connected to IPC server.");

    //             // Keep the child process alive for an extended period
    //             thread::sleep(Duration::from_secs(2)); // Increased sleep duration

    //             return (local_tx, local_rx);
    //         }
    //         Err(e) => {
    //             attempts += 1;
    //             println!("Failed to connect to IPC server: {:?}. Retrying {}/{}", e, attempts, max_attempts);
    //             thread::sleep(Duration::from_millis(1500)); // Increased retry wait time
    //         }
    //     }
    // }


    // panic!("Failed to connect to IPC server after {} attempts", max_attempts);
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

    println!("Coordinator Created");

    // 2. Spawns and connects to new clients processes and then registers them with
    //    the coordinator
    let mut clients: Vec<Child> = Vec::new();

    for i in 0..opts.num_clients {
        
        let mut client_opts = opts.clone();
        client_opts.mode = "client".to_string();
        client_opts.num = i;

        println!("Spawning Client {}", i);

        let (child, tx, rx) = spawn_child_and_connect(&mut client_opts);
        let name = format!("client_{}", i);
        coordinator.client_join(&name, tx, rx);

        clients.push(child);

    }

    println!("Clients Spawned");

    // 3. Spawns and connects to new participant processes and then registers them
    //    with the coordinator
    let mut participants: Vec<Child> = Vec::new();

    for i in 0..opts.num_participants {
        let mut participant_opts = opts.clone();
        participant_opts.mode = "participant".to_string();
        participant_opts.num = i;

        let (child, tx, rx) = spawn_child_and_connect(&mut participant_opts);
        let name = format!("participant_{}", i);

        coordinator.participant_join(&name, tx, rx);

        participants.push(child);
    }

    println!("Participants Spawned");


    // 4. Starts the coordinator protocol
    coordinator.protocol();

    // 5. Wait until the children finish execution
    // for mut client in clients{
    //     let _ = client.wait();
    // }

    // for mut participant in participants{
    //     let _ = participant.wait();
    // }

    while running.load(Ordering::SeqCst) {
        info!("Running in main.rs");
        thread::sleep(Duration::from_millis(500));
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
    let (tx, rx) = connect_to_coordinator(opts);
    
    // 2. Constructs a new client
    let client_id_str = format!("client_{}", opts.num);
    let mut client = client::Client::new(client_id_str, running, tx, rx);

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
    let participant_id_str = format!("participant_{}", opts.num);
    
    // 1. Connects to the coordinator to get tx/rx
    let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str);
    let (tx, rx) = connect_to_coordinator(opts);

    // 2. Constructs a new participant
    let mut participant = participant::Participant::new(participant_id_str,
        participant_log_path,
        running,
        opts.send_success_probability,
        opts.operation_success_probability,
        tx,
        rx); //TODO Add channels

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
