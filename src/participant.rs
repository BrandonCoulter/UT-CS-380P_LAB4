//!
//! participant.rs
//! Implementation of 2PC participant
//!
#[allow(unused)]
extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::thread;

use participant::rand::prelude::*;
use participant::ipc_channel::ipc::IpcReceiver as Receiver;
use participant::ipc_channel::ipc::TryRecvError;
use participant::ipc_channel::ipc::IpcSender as Sender;

use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

///
/// ParticipantState
/// enum for Participant 2PC state machine
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {
    Quiescent,
    ReceivedP1,
    VotedAbort,
    VotedCommit,
    AwaitingGlobalDecision,
}

///
/// Participant
/// Structure for maintaining per-participant state and communication/synchronization objects to/from coordinator
///
#[derive(Debug)]
pub struct Participant {
    id_str: String,
    state: ParticipantState,
    log: oplog::OpLog,
    running: Arc<AtomicBool>,
    send_success_prob: f64,
    operation_success_prob: f64,
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>,
}

///
/// Participant
/// Implementation of participant for the 2PC protocol
/// Required:
/// 1. new -- Constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- Implements participant side protocol for 2PC
///
impl Participant {

    ///
    /// new()
    ///
    /// Return a new participant, ready to run the 2PC protocol with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course.
    ///
    pub fn new(
        id_str: String,
        log_path: String,
        r: Arc<AtomicBool>,
        send_success_prob: f64,
        operation_success_prob: f64,
        ctx: Sender<ProtocolMessage>,
        crx: Receiver<ProtocolMessage>) -> Participant {

        Participant {
            id_str: id_str,
            state: ParticipantState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r,
            send_success_prob: send_success_prob,
            operation_success_prob: operation_success_prob,
            // TODO
            tx: ctx,
            rx: crx,

        }
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator. This can fail depending on
    /// the success probability. For testing purposes, make sure to not specify
    /// the -S flag so the default value of 1 is used for failproof sending.
    ///
    /// HINT: You will need to implement the actual sending
    ///
    pub fn send(&mut self, pm: ProtocolMessage) {
        let x: f64 = random();
        if x <= self.send_success_prob {
            // TODO: Send success
            self.tx.send(pm).expect("Failed to send protocol message to coordnator.");
        } else {
            // TODO: Send fail
            info!("Message sending failed due to probabilistic failure.");
        }
    }

    ///
    /// perform_operation
    /// Perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the
    /// command-line option success_probability.
    ///
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic.
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than
    ///       bool if it's more convenient for your design).
    ///
    pub fn perform_operation(&mut self, request_option: &Option<ProtocolMessage>) -> bool {

        trace!("{}::Performing operation", self.id_str.clone());
        let x: f64 = random();
        if x <= self.operation_success_prob {
            // TODO: Successful operation
            info!("Operation performed successfully for request: {:?}", request_option);
            return true
        } else {
            // TODO: Failed operation
            info!("Operation probabilistically failed for request: {:?}", request_option);
            return false
        }
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        let successful_ops: u64 = 0;
        let failed_ops: u64 = 0;
        let unknown_ops: u64 = 0;

        println!("{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", self.id_str.clone(), successful_ops, failed_ops, unknown_ops);
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        // TODO
        while self.running.load(Ordering::SeqCst){
            println!("Participant: {} is running.", self.id_str.clone());
            thread::sleep(Duration::from_secs(1));
        }

        trace!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        trace!("{}::Beginning protocol", self.id_str.clone());
        while self.running.load(Ordering::SeqCst) {
            match self.rx.try_recv() {
                Ok(msg) => {
                match msg.mtype {
                    MessageType::CoordinatorPropose => {
                        self.state = ParticipantState::ReceivedP1;
                        let result = self.perform_operation(&Some(msg.clone()));
                        let response_type = if result { MessageType::ParticipantVoteCommit } else { MessageType::ParticipantVoteAbort };
                        let response = ProtocolMessage::generate(response_type, msg.txid.clone(), self.id_str.clone(), msg.opid); self.send(response);
                    },
                    MessageType::CoordinatorCommit => {
                        self.state = ParticipantState::VotedCommit;
                        self.log.append(MessageType::CoordinatorCommit, msg.txid.clone(), msg.senderid.clone(), msg.opid);
                    },
                    MessageType::CoordinatorAbort => {
                        self.state = ParticipantState::VotedAbort;
                        self.log.append(MessageType::CoordinatorAbort, msg.txid.clone(), msg.senderid.clone(), msg.opid);
                    }, 
                    _ => (), 
                    }
                },
                Err(TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(100));
                },
                Err(e) => {
                    panic!("PANIC: {}::Failed to receive message: {:?}", self.id_str.clone(), e);
                }
            }
        }
        self.wait_for_exit_signal();
        self.report_status();
    }
}
