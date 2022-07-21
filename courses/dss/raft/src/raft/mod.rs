use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::mpsc::UnboundedSender;
use futures::channel::mpsc::{self, Sender};
use futures::channel::oneshot::{channel, Receiver};
use futures::stream::FuturesUnordered;
use futures::{select, FutureExt, StreamExt};
use futures_timer::Delay;
use optional::{none, some, Optioned};
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(450);
const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(750);
const HEARTBEAT_TIME: Duration = Duration::from_millis(200);

mod seal {
    #[derive(Clone, Copy, Debug)]
    pub struct Seal;
}
/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
#[derive(Clone, Debug)]
pub enum ApplyMsg {
    // Noop will not go through apply_channel, but commit every log entry before it.
    Noop(seal::Seal),
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

// Each entry contains command for state machine, and term when entry was
// received by leader, and index for simplcity sake.
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct LogEntry {
    command: ApplyMsg,
    term: u64,
    index: u64,
}

/// Role for raft peer.
#[derive(Default, Clone, Copy, Debug)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
    Killed,
}
use Role::*;

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub role: Role,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        matches!(self.role, Leader)
    }
}

type Logs = Vec<LogEntry>;

#[derive(Debug, Clone, Copy)]
enum Event {
    HigherTerm,
    VoteToCandidate,
    VoteTimeOut,
    ElectionTimeout,
    GetMajority,
    HeartBeat,
    Kill,
}
use Event::*;

// A single Raft peer.
#[allow(dead_code)]
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<Arc<RaftClient>>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: State,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    // Persistent state on all servers
    // (Updated on stable storage before responding to RPCs)
    voted_for: Optioned<u64>, // candidateId that received vote in current term
    logs: Logs,               // log entries

    // Volatile state on all servers
    commit_index: u64, // index of highest log entry known to be committed (initialized to 0, increases monotonically)
    last_applied: u64, // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

    // Volatile state on leaders
    // (Reinitialized after election)
    next_index: Vec<u64>, // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    match_index: Vec<u64>, // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

    // other state
    apply_ch: UnboundedSender<ApplyMsg>,
    leader_id: Optioned<u64>,

    // threadpool
    pool: Arc<RaftClient>,

    // event sender
    tx: Sender<Event>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        let peers: Vec<_> = peers.into_iter().map(Arc::new).collect();
        let pool = Arc::clone(&peers[me]);

        let (tx, _) = mpsc::channel(0);

        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Default::default(),

            voted_for: none(),
            logs: Vec::new(),

            commit_index: 0,
            last_applied: 0,

            next_index: Vec::new(),
            match_index: Vec::new(),

            apply_ch,
            leader_id: none(),

            pool,
            tx,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        assert_ne!(self.me, server);
        let peer = &self.peers[server];
        let peer_clone = Arc::clone(peer);
        let (tx, rx) = channel::<Result<RequestVoteReply>>();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    fn send_append_entries(
        &self,
        server: usize,
        args: AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        assert_ne!(self.me, server);
        let peer = &self.peers[server];
        let peer_clone = Arc::clone(peer);
        let (tx, rx) = channel::<Result<AppendEntriesReply>>();
        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Raft>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        Node { raft: raft.run() }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().state.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().state.is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        self.raft.lock().unwrap().state.clone()
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        let mut rf = self.raft.lock().unwrap();
        rf.state.role = Killed;
        rf.send_event(Kill);
    }

    /// A service wants to switch to snapshot.
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).

        // Get state
        let mut rf = self.raft.lock().unwrap();
        let (last_log_index, last_log_term) = rf.last_log_info();

        // 1. Reply false if term < currentTerm (§5.1)
        if !rf.check_term(args.term) {
            return Ok(RequestVoteReply {
                term: rf.state.term,
                vote_granted: false,
            });
        }
        let voted_for = rf.voted_for;
        // 2. If votedFor is null or candidateId
        if voted_for.is_some() && voted_for.unwrap() != args.candidate_id {
            return Ok(RequestVoteReply {
                term: rf.state.term,
                vote_granted: false,
            });
        }
        // and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        if args.last_log_term < last_log_term {
            return Ok(RequestVoteReply {
                term: rf.state.term,
                vote_granted: false,
            });
        }
        if args.last_log_index < last_log_index {
            return Ok(RequestVoteReply {
                term: rf.state.term,
                vote_granted: false,
            });
        }

        rf.vote(args.candidate_id);
        Ok(RequestVoteReply {
            term: rf.state.term,
            vote_granted: true,
        })
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        // Get state
        let mut rf = self.raft.lock().unwrap();

        // 1. Reply false if term < currentTerm (§5.1)
        if !rf.check_term(args.term) {
            return Ok(AppendEntriesReply {
                term: rf.state.term,
                success: false,
            });
        }

        // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)

        // 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
        // 4. Append any new entries not already in the log
        // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        // todo!();

        rf.transform(Follower);
        rf.leader_id = some(args.leader_id);
        rf.send_event(HeartBeat);

        Ok(AppendEntriesReply {
            term: rf.state.term,
            success: true,
        })
    }
}

impl Raft {
    /// return last_log_index, last_log_term
    fn last_log_info(&self) -> (u64, u64) {
        self.logs
            .last()
            .map(|log| (log.index, log.term))
            .unwrap_or((0, 0))
    }

    fn get_entries_info(&self, _server: usize) -> (u64, u64, Vec<Vec<u8>>) {
        (0, 0, Vec::new())
    }

    fn append_entries_args(&self, server: usize) -> AppendEntriesArgs {
        let (term, leader_id, leader_commit) = (self.state.term, self.me as u64, self.commit_index);
        let (prev_log_index, prev_log_term, entries) = self.get_entries_info(server);
        AppendEntriesArgs {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }

    /// change term
    fn change_term(&mut self, new_term: u64) {
        info!("TERM S{} T{} -> T{}", self.me, self.state.term, new_term);
        self.state.term = new_term;
        self.voted_for = none();
        self.leader_id = none();
    }

    /// send event
    fn send_event(&mut self, event: Event) {
        info!("EVNT S{} Get event {:?}", self.me, event);
        let _ = self.tx.try_send(event);
    }

    /// check term for every request and reply
    fn check_term(&mut self, term: u64) -> bool {
        if self.state.term < term {
            // must change before return
            self.change_term(term);
            self.transform(Follower);
            self.send_event(HigherTerm);
            return true;
        }
        self.state.term == term
    }

    /// vote to candidate
    fn vote(&mut self, candidate_id: u64) {
        assert!(matches!(self.state.role, Follower));
        assert!(self.voted_for.is_none() || self.voted_for.unwrap() == candidate_id);
        info!(
            "VOTE S{} => S{} at T{}",
            self.me, candidate_id, self.state.term
        );

        self.voted_for = some(candidate_id);
        self.send_event(VoteToCandidate);
    }

    /// run raft services
    fn run(mut self) -> Arc<Mutex<Self>> {
        let pool = Arc::clone(&self.pool);
        let (tx, rx) = mpsc::channel(1);
        self.tx = tx;

        let node = Arc::new(Mutex::new(self));
        let raft = Arc::clone(&node);

        pool.spawn(Self::routine(raft, rx));

        node
    }

    /// change state
    fn transform(&mut self, target: Role) {
        match target {
            Follower => {
                if let Follower = self.state.role {
                    return;
                }
            }
            Candidate => {
                assert!(matches!(self.state.role, Follower | Candidate));
                self.change_term(self.state.term + 1);
                self.voted_for = some(self.me as u64);
            }
            Leader => {
                assert!(matches!(self.state.role, Candidate));
                self.leader_id = some(self.me as u64);
            }
            Killed => {}
        }
        info!(
            "TRAN S{} {:?} => {:?} at T{}",
            self.me, self.state.role, target, self.state.term
        );
        self.state.role = target;
    }

    /// state machine
    async fn routine(raft: Arc<Mutex<Raft>>, mut rx: mpsc::Receiver<Event>) {
        loop {
            let state = raft.lock().unwrap().state.clone();
            match state.role {
                Follower => Self::handle_follower(&raft, &mut rx).await,
                Candidate => Self::handle_candidate(&raft, &mut rx).await,
                Leader => Self::handle_leader(&raft, &mut rx).await,
                Killed => return,
            }
        }
    }

    /// follower routine
    async fn handle_follower(raft: &Arc<Mutex<Raft>>, rx: &mut mpsc::Receiver<Event>) {
        let dur = rand::thread_rng().gen_range(ELECTION_TIMEOUT_MIN..ELECTION_TIMEOUT_MAX);
        let mut election_timeout = Delay::new(dur).fuse();
        loop {
            let event = select! {
                _ = election_timeout => ElectionTimeout,
                e = rx.next() => e.unwrap(),
            };
            match event {
                ElectionTimeout => {
                    let mut rf = raft.lock().unwrap();
                    info!("TIME S{} election timeout at T{}", rf.me, rf.state.term);
                    rf.transform(Candidate);
                    return;
                }
                HigherTerm | VoteToCandidate | HeartBeat | Kill => return,
                VoteTimeOut => (),
                GetMajority => (),
            }
        }
    }

    /// candidate routine
    async fn handle_candidate(raft: &Arc<Mutex<Raft>>, rx: &mut mpsc::Receiver<Event>) {
        Self::candidate_request_vote(raft);

        let dur = rand::thread_rng().gen_range(ELECTION_TIMEOUT_MIN..ELECTION_TIMEOUT_MAX);
        let mut vote_timeout = Delay::new(dur).fuse();
        loop {
            let event = select! {
                _ = vote_timeout => VoteTimeOut,
                e = rx.next() => e.unwrap(),
            };
            match event {
                GetMajority => {
                    return;
                }
                VoteTimeOut => {
                    let mut rf = raft.lock().unwrap();
                    rf.transform(Candidate);
                    return;
                }
                HigherTerm | HeartBeat | Kill => return,
                VoteToCandidate => (),
                ElectionTimeout => (),
            }
        }
    }

    /// handle request vote replys
    async fn handle_vote_reply(
        raft: Arc<Mutex<Raft>>,
        mut rxs: FuturesUnordered<Receiver<Result<RequestVoteReply>>>,
        me: usize,
        threshold: usize,
        old_term: u64,
    ) {
        let mut cnt = 1;
        while let Some(Ok(res)) = rxs.next().await {
            match res {
                Ok(RequestVoteReply { term, vote_granted }) => {
                    let mut rf = raft.lock().unwrap();
                    if !rf.check_term(term)
                        || !matches!(rf.state.role, Candidate)
                        || old_term != rf.state.term
                    {
                        return;
                    }
                    if vote_granted {
                        cnt += 1;
                        info!(
                            "VOTE S{} get vote {}/{} at T{}",
                            me, cnt, threshold, old_term
                        );
                        if cnt >= threshold {
                            rf.transform(Leader);
                            info!("LEAD S{} become leader at T{}!", rf.me, rf.state.term);
                            rf.send_event(GetMajority);
                            return;
                        }
                    }
                }
                Err(Error::Rpc(rpc)) => debug!("RPC error: {:?}", rpc),
                _ => unreachable!(),
            }
        }
    }

    /// start request_vote
    fn candidate_request_vote(raft: &Arc<Mutex<Raft>>) {
        let rf = raft.lock().unwrap();
        let term = rf.state.term;
        let me = rf.me;
        let (last_log_index, last_log_term) = rf.last_log_info();
        let args = RequestVoteArgs {
            term,
            candidate_id: me as u64,
            last_log_index,
            last_log_term,
        };
        let peers = rf.peers.len();
        let threshold = peers / 2 + 1;
        let rxs = (0..me)
            .chain((me + 1)..peers)
            .map(|i| rf.send_request_vote(i, args.clone()))
            .collect();
        rf.pool.spawn(Self::handle_vote_reply(
            Arc::clone(raft),
            rxs,
            me,
            threshold,
            term,
        ));
    }

    /// leader routine
    async fn handle_leader(raft: &Arc<Mutex<Raft>>, rx: &mut mpsc::Receiver<Event>) {
        Self::leader_append_entries(raft);

        let mut heartbeat = Delay::new(HEARTBEAT_TIME).fuse();
        loop {
            let event = select! {
                _ = heartbeat => return,
                e = rx.next() => e.unwrap(),
            };
            match event {
                HigherTerm | HeartBeat | Kill => return,
                VoteToCandidate => (),
                VoteTimeOut => (),
                ElectionTimeout => (),
                GetMajority => (),
            }
        }
    }

    fn leader_append_entries(raft: &Arc<Mutex<Raft>>) {
        let rf = raft.lock().unwrap();
        let old_term = rf.state.term;
        let me = rf.me;
        let peers = rf.peers.len();
        let rxs: FuturesUnordered<_> = (0..me)
            .chain((me + 1)..peers)
            .map(|i| rf.send_append_entries(i, rf.append_entries_args(i)))
            .collect();
        rf.pool.spawn(Self::handle_append_reply(
            Arc::clone(raft),
            rxs,
            me,
            // threshold,
            old_term,
        ));
    }

    async fn handle_append_reply(
        raft: Arc<Mutex<Raft>>,
        mut rxs: FuturesUnordered<Receiver<Result<AppendEntriesReply>>>,
        me: usize,
        old_term: u64,
    ) {
        while let Some(Ok(res)) = rxs.next().await {
            match res {
                Ok(AppendEntriesReply { term, success }) => {
                    let mut rf = raft.lock().unwrap();
                    if !rf.check_term(term)
                        || !matches!(rf.state.role, Leader)
                        || old_term != rf.state.term
                    {
                        return;
                    }
                    if success {
                        info!("HEAR S{} Get append reply", me);
                    }
                }
                Err(Error::Rpc(rpc)) => debug!("RPC error: {:?}", rpc),
                _ => unreachable!(),
            }
        }
    }
}
