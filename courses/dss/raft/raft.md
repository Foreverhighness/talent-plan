# raft paper's notes

At start up, all server begin with `Follower` state, after a timeout, a election is started.

Raft ensures that there is at most one leader in a given term.(Page 5 right) This is guaranteed by the persistence of the `voteFor` varibles.

> Each server stores a current term number.

> Current terms are exchanged whenever servers communicate.

> if one server’s current term is smaller than the other’s, then it updates its current term to the larger value.

> If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.

> If a server receives a request with a stale term number, it rejects the request.

> Servers retry RPCs if they do not receive a response in a timely manner.

> A server remains in follower state as long as it receives valid RPCs from a leader or **candidate**.

> If a follower receives no communication over a period of time called the election timeout, then it assumes there is no viable leader and begins an election to choose a new leader.

To begin an election, a follower increments its current
term and transitions to candidate state. It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.

A candidate continues in this state until one of three things happens:

- it wins the election.
- another server establishes itself as leader
- a period of time goes by with no winner. These outcomes are discussed separately in the paragraphs below.

> Once a candidate wins an election, it becomes leader. It then sends heartbeat messages to all of the other servers to establish its authority and prevent new elections.

## Leader Election

The RPC includes information about the candidate’s log, and the voter denies its vote if its own log is more up-to-date than that of the candidate.

Raft determines which of two logs is more up-to-date
by comparing the index and term of the last entries in the logs. If the logs have last entries with different terms, then the log with the later term is more up-to-date. If the logs end with the same term, then whichever log is longer is more up-to-date.

if a server receives a RequestVote RPC within the minimum election timeout of hearing from a current leader, it does not update its term or grant its vote.(Page 11 right)