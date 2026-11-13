# raftkv — design

A Raft-replicated key-value store in Java 21. This document is written **before** any Raft logic exists
and is the contract the code is held to. It is updated at the end of every stage (see §9).

## 0. How to read the citations

* **[P x.y]** = Raft paper, *In Search of an Understandable Consensus Algorithm* (extended version). Section
  numbers: 5.1 basics, 5.2 leader election, 5.3 log replication, 5.4 safety (5.4.1 election restriction,
  5.4.2 committing entries from previous terms, 5.4.3 safety argument), 5.5 follower/candidate crashes,
  5.6 timing and availability, 6 membership changes, 7 log compaction, 8 client interaction.
  Figures: 2 (state + RPCs), 3 (the five guarantees), 4 (server states), 8 (previous-term commit hazard),
  13 (InstallSnapshot).
* **[T x.y]** = Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014). Chapters: 3 basic
  algorithm, 4 membership changes, 5 log compaction, 6 client interaction, 9 leader-election evaluation.
* A trailing **(?)** means *I am not sure of the exact subsection number*. I have no copy of either document in
  this environment; all numbers here are from memory and **must be checked against the PDFs before stage 1 is
  accepted**. Chapter-level citations are much more reliable than subsection-level ones.

**Paper vs. thesis.** Default is the paper for the core algorithm (Figure 2 is the specification). Where the
thesis is more precise or differs, the divergence is listed in §8 with the choice made and why.

## 1. Module layout

| Module | Depends on | Purpose |
|---|---|---|
| `raft-core` | JDK only (tests: JUnit 5, jqwik) | The consensus state machine. Pure, deterministic, single-threaded. |
| `raft-sim` | `raft-core` | Deterministic simulator: virtual clock, seeded network/fault model, simulated disk, invariant checkers. |
| `raft-server` | `raft-core` | Durable storage (WAL + fsync), network transport, the KV state machine and client-facing service. |
| `raft-jepsen` | `raft-core`, `raft-server` | Fault-injection harness (nemesis) and history checkers (linearizability) against real servers. |

`raft-core` having no dependencies is enforced by the build (`verifyNoRuntimeDependencies`, wired into `check`).
Determinism rules for `raft-core` (no threads, wall-clock, randomness, or I/O) are enforced by review now and by
a source/bytecode scan test planned for stage 1 (see §9, open question Q1).

Packages: `raftkv.core`, `raftkv.sim`, `raftkv.server`, `raftkv.jepsen`.

## 2. RPC messages

All messages are immutable records implementing one sealed interface. Messages carry **no transport envelope**:
sender/receiver live in `Receive(from, …)` and `Send(to, …)` (§4). Types: `NodeId` = value record over an `int`;
`term`, `index` = `long`. Log index starts at 1 [P Fig 2]; index 0 / term 0 denote "before the first entry".

```java
public sealed interface Message permits
        RequestVote, RequestVoteResponse,
        AppendEntries, AppendEntriesResponse,
        InstallSnapshot, InstallSnapshotResponse {
    long term();                       // every RPC carries the sender's term [P 5.1]
}
```

### RequestVote [P Fig 2]
```java
record RequestVote(long term, NodeId candidateId, long lastLogIndex, long lastLogTerm) implements Message {
    // EXT(pre-vote): boolean preVote            -- thesis 9.6 (?), only if we adopt pre-vote (Q4)
    // EXT(leadership transfer): boolean force   -- not planned
}
record RequestVoteResponse(long term, boolean voteGranted) implements Message {
    // EXT(pre-vote): echo of preVote flag
}
```

### AppendEntries [P Fig 2]
```java
record AppendEntries(long term, NodeId leaderId, long prevLogIndex, long prevLogTerm,
                     List<LogEntry> entries, long leaderCommit) implements Message {
    // EXT(read-index): long readSeq  -- heartbeat round id used to confirm leadership for ReadIndex [T 6.4]
    // EXT(config): none on the wire; configuration travels as a log entry payload (see LogEntry)
}
record AppendEntriesResponse(long term, boolean success) implements Message {
    // ---- REQUIRED ADDITION (stage 2, not optional) -------------------------------------------
    // EXT(correlation): long matchIndex
    //   Figure 2's response has only {term, success}. The paper assumes RPC request/response pairing, so the
    //   leader knows which request a reply answers and can compute matchIndex = prevLogIndex + entries.size().
    //   Our boundary is message passing (Receive/Send): replies can be duplicated, reordered or delayed, and
    //   without a correlation field a stale success could advance matchIndex past what the follower holds.
    //   This is a *safety* issue, not an optimisation. Decision D1 in §8.
    // EXT(conflict hint): long conflictIndex, long conflictTerm   -- fast log backtracking [P 5.3 (?), T (?)]
    // EXT(read-index): long readSeq                               -- echo of the probe round [T 6.4]
}
```

### InstallSnapshot [P Fig 13] (note: Figure 13, not Figure 2)
```java
record InstallSnapshot(long term, NodeId leaderId, long lastIncludedIndex, long lastIncludedTerm,
                       long offset, byte[] data /* wrapped as immutable Bytes */, boolean done) implements Message {
    // EXT(config): the cluster configuration as of lastIncludedIndex must be in the snapshot [T 5 (?), P 7]
}
record InstallSnapshotResponse(long term) implements Message {
    // EXT(correlation, REQUIRED in stage 5): long lastIncludedIndex -- same reasoning as AppendEntriesResponse (D1)
}
```
Whether we implement chunking (`offset`/`done`) or always send one chunk (`offset = 0, done = true`) is open (Q6).
The fields stay in the record either way so we match the paper.

### Log entries and commands
```java
record LogEntry(long index, long term, Payload payload) {}   // index explicit, see D2
sealed interface Payload permits Command, NoOp /*, EXT(config): ConfigChangeEntry */ {}
record Command(Bytes data) implements Payload {}
record NoOp() implements Payload {}                           // new leader's first entry [P 8]
```
`Bytes` is a small immutable value type with value equality (a raw `byte[]` in a record compares by identity,
which would make invariant checkers and tests lie). Decision D2 (explicit `index`) in §8.

## 3. Node state

Names and meanings follow [P Fig 2] unless marked.

### Persistent (survives restart; **must be fsynced before any reply or acknowledgement that depends on it**)
| Field | Notes |
|---|---|
| `currentTerm` | monotonically non-decreasing [P Fig 2] |
| `votedFor` | `Optional<NodeId>`; at most one value per term [P 5.2] |
| `log[]` | entries after the snapshot point; index ≥ 1 [P Fig 2] |
| snapshot metadata + data | `lastIncludedIndex`, `lastIncludedTerm`, state machine image, config at that index [P 7] — stage 5 |
| EXT(config) | latest configuration is derived from the log (+snapshot); not a separate persisted field [T 4.1 (?)] |

### Volatile on all nodes
| Field | Notes |
|---|---|
| `commitIndex` | highest entry known committed [P Fig 2]. Resets to the snapshot index on restart. |
| `lastApplied` | highest entry handed to the host to apply [P Fig 2]. Advanced when the entry is *emitted* in `Output.toApply` (§4). |
| `role` | Follower / Candidate / Leader [P Fig 4] (not in Fig 2) |
| `leaderHint` | last known leader id, for client redirection [P 8] |
| `electionElapsed`, `electionTimeout` | in ticks; timeout drawn from the injected seeded source, uniformly in `[min, max]` [P 5.2] |
| `votesGranted` | candidate only |

### Volatile on leaders (reinitialised on every election win) [P Fig 2]
| Field | Notes |
|---|---|
| `nextIndex[peer]` | initialised to leader last index + 1 |
| `matchIndex[peer]` | initialised to 0; leader's own entry counts toward its own match once persisted (see §4 ordering) |
| `heartbeatElapsed` | ticks since last broadcast |
| EXT(read-index) | pending `ReadRequest`s, probe round id, acks per round [T 6.4] |
| EXT(config) | whether a config change is in flight [T 4.1 (?)] |

A new leader appends a `NoOp` in its term immediately [P 8]; this is also what makes it legal to commit prior-term
entries (see invariant I-commit-5.4.2) and to serve reads [T 6.4].

## 4. Core API

```java
public final class Node {
    public Node(NodeId self, Cluster initialConfig, Options options,
                RandomGenerator rng,            // injected, seeded; the ONLY source of randomness
                RestoredState restored);        // persisted state read by the host at boot (may be empty)
    public Output step(Input input);            // the only entry point
}
```
`step` is synchronous, total (never throws for a well-typed `Input`; invalid/stale input is ignored and the rule is
documented where it is ignored), and deterministic: same constructor args + same input sequence ⇒ same output
sequence, bit for bit, including the number of `rng` draws.

### Input (sealed)
```java
sealed interface Input permits Tick, Receive, Propose, ReadRequest, ChangeConfig {}
record Tick() implements Input {}                              // one unit of logical time
record Receive(NodeId from, Message message) implements Input {}
record Propose(Command command) implements Input {}
record ReadRequest(ReadId id) implements Input {}              // asks for a linearization point, not a value
record ChangeConfig(ConfigChange change) implements Input {}   // shape defined in stage 6
```

### Output (nothing else may cross the boundary)
```java
record Output(
    List<Send> messages,              // Send(NodeId to, Message message)
    Persist persist,                  // hard state change, log truncate/append, snapshot metadata
    List<LogEntry> toApply,           // newly committed entries, contiguous and in order
    List<ReadResult> readResults,     // ReadReady(id, readIndex) once safe
    List<RoleChange> roleChanges)     // (role, term, leaderHint) transitions caused by this step
```
`Persist` carries: new `(currentTerm, votedFor)` if changed; `truncateFrom(index)` if a conflict was found
[P 5.3]; entries to append; snapshot metadata. All lists are immutable copies.

**Host obligations (this is rule 5, written as a contract):**
1. Fully write **and fsync** `persist` before releasing anything else from the same `Output`
   (sending `messages`, applying `toApply`, publishing `readResults`).
2. Call `step` strictly sequentially, never concurrently.
3. Apply `toApply` entries in order. After a restart the core re-emits committed entries as `commitIndex`
   advances again (it restarts from the snapshot index), so the state machine must either be restored from its
   snapshot or tolerate re-application.
4. Serve a `ReadReady(id, readIndex)` only after the local state machine has applied through `readIndex`.

Because the host fsyncs before the core is stepped again, the core may treat entries it just emitted in `persist`
as durable when counting its own `matchIndex` (a crash before the fsync loses the core's volatile state too).

There is deliberately **no** "persisted/applied" acknowledgement input: the listed `Input` set is closed.

### Proposed addition — needs sign-off (Q2)
As specified, nothing tells the host (a) which `(index, term)` a `Propose` got, or (b) that a `Propose` /
`ChangeConfig` was rejected because the node is not leader. The KV layer needs both to answer clients and to detect
that its proposal was overwritten after a leader change [P 8]. Proposed: add `List<Rejection> rejections`
(`NotLeader(leaderHint)`, later `ConfigChangeInProgress`, …) and `List<Accepted> accepted` (`index`, `term`) to
`Output`. Until approved, they are **not** part of the boundary. `ReadResult` already has a `Rejected(id, leaderHint)`
variant so reads do not need this.

### Time and randomness
* One `Tick` = one logical time unit; `Options` give heartbeat interval and election timeout range in ticks
  (paper's 150–300 ms guidance [P 5.6] is just a ratio: timeout ≫ broadcast time ≪ MTBF).
* Randomness: only `rng.nextInt(...)` for the election timeout. Every draw is part of the determinism contract.

## 5. Safety invariants and liveness properties

### Roadmap (proposed; stages 1+ are *not* started)
| Stage | Scope |
|---|---|
| 0 | This skeleton: Gradle modules, DESIGN.md, CI, smoke tests. No Raft logic. |
| 1 | Simulator core (virtual clock, seeded network, invariant-check-after-every-step loop) + leader election: terms, `RequestVote`, timers, persisted term/vote. |
| 2 | Log replication and commit: `AppendEntries`, log matching, commit rule incl. [P 5.4.2], apply. |
| 3 | Durability and crashes: real WAL + fsync in `raft-server`, simulated disk that loses unsynced writes, crash/restart in the sim. |
| 4 | Client interaction: no-op on election, client sessions / dedup [T 6.3 (?)], linearizable reads via ReadIndex [T 6.4], KV service. |
| 5 | Log compaction and `InstallSnapshot`. |
| 6 | Membership changes. |
| 7 | `raft-jepsen` nemesis + linearizability checker against real servers; optimisations (conflict hints); optional pre-vote / check-quorum. |

### Safety (executable checkers in `raft-sim`, run **after every simulated step**)
Checkers observe only what crosses the boundary (every `Output`) plus the simulator's own simulated disk and
network; they do not reach into `Node` internals (see Q3).

| ID | Property | Source | Testable from |
|---|---|---|---|
| I-election | **Election Safety**: at most one leader per term | [P Fig 3, 5.2] | stage 1 |
| I-term-mono | `currentTerm` never decreases; at most one `votedFor` per term, never changed once set | [P Fig 2, 5.2] | stage 1 |
| I-persist | `currentTerm`/`votedFor`/log are on (simulated) stable storage before any message or ack that depends on them leaves the node; crash drops unsynced writes | [P Fig 2], rule 5 | stage 1 for term/vote; stage 3 for full crash model |
| I-append-only | **Leader Append-Only**: a leader never overwrites or deletes entries in its own log | [P Fig 3, 5.3] | stage 2 |
| I-log-matching | **Log Matching**: same index+term ⇒ same command and identical prefix | [P Fig 3, 5.3] | stage 2 |
| I-commit-5.4.2 | A leader never commits a prior-term entry by counting replicas; only by committing a current-term entry (Figure 8 scenario as a directed test) | [P 5.4.2, Fig 8] | stage 2 |
| I-commit-mono | `commitIndex` never decreases; committed entries are never changed | [P Fig 2] | stage 2 |
| I-leader-completeness | **Leader Completeness**: an entry committed in term T is in the log of every leader of term > T | [P Fig 3, 5.4.3] | stage 2 (meaningful under crashes from stage 3) |
| I-sm-safety | **State Machine Safety**: no two nodes apply different entries at the same index (compare all `toApply` streams) | [P Fig 3, 5.4.3] | stage 2 |
| I-linearizable | Client-visible history of the KV service is linearizable (including reads) | [P 8], [T 6.3–6.4] | stage 4 (sim), stage 7 (real servers) |
| I-snapshot-equiv | State restored from snapshot + suffix equals state from full replay | [P 7] | stage 5 |
| I-config | No two disjoint majorities can exist across a config change; at most one uncommitted config change; new leader commits an entry in its term before changing config | [P 6], [T 4.1 (?)] | stage 6 |

### Liveness (checked as *bounded* simulation runs, not proofs; they depend on timing assumptions [P 5.6])
| ID | Property | Testable from |
|---|---|---|
| L-election | After the network becomes stable and a majority is mutually connected, a leader is elected within a bounded number of ticks | stage 1 |
| L-stable-leader | A stable leader is not deposed while a majority keeps hearing from it | stage 1 |
| L-progress | A `Propose` to a stable leader commits and is applied on a majority within a bounded number of ticks | stage 2 |
| L-recovery | After a minority crashes and restarts, the cluster keeps committing; restarted nodes catch up | stage 3 |
| L-read | A `ReadRequest` to a stable leader becomes `ReadReady` within a bounded number of ticks | stage 4 |
| L-catchup | A node that is behind the compaction point catches up via `InstallSnapshot` | stage 5 |
| L-config | A proposed membership change eventually takes effect when a majority is available | stage 6 |

## 6. Simulator (`raft-sim`) in one paragraph
Single thread, virtual clock, one seeded `RandomGenerator` for the whole run (a failing run is reproduced by
its seed alone). It owns the network (drop, duplicate, delay, reorder, partition/heal), the disk (a write is lost on
crash unless fsynced), the crash/restart schedule, and the client workload. Each step: pick the next event
deterministically, deliver it to one `Node`, route the `Output` (persist → network → apply), then run **all**
invariant checkers. A violation fails the run and prints the seed and the trace.

## 7. What exists / deliberately missing

### What exists (stages 1-2: election and log replication, plus the simulator)
* `raft-core`: `Node.step` implementing Figure 2 and [P 5.1-5.4]: randomized tick-driven election timeouts, heartbeats,
  the election restriction [5.4.1], commit only via a current-term entry [5.4.2], a no-op at the start of every term [8],
  plain one-step-back `nextIndex` decrement. Messages, `Input` (Tick/Receive/Propose), `Output`, `Persist`, `RestoredState`.
* `raft-sim`: `Simulation` (one seeded PRNG, virtual clock, priority queue of in-flight messages), network faults
  (drop, duplicate, reorder via random delay, partitions as arbitrary sets of directed links), crash/restart over an in-memory
  `FakeDisk`, `InvariantChecker` run after every step (election safety, leader append-only, log matching, leader completeness,
  state machine safety), `SimRunner.run(seed, steps, faultSchedule)` which prints seed + trace on failure and ends each run with a
  heal-then-liveness phase.
* Tests: one per Figure 2 rule, Figure 8 scripted by hand, 10,000 random seeds with faults, a liveness test, a replay-determinism
  test, and a mutation test that injects each of seven bugs (`Bug` enum, test-only) and requires the simulator to catch it.
* `raft-server` and `raft-jepsen` are still empty skeletons with smoke tests.

### Deliberately missing (and where it arrives)
* `InstallSnapshot` and snapshots (stage 5); `ReadRequest` / `ChangeConfig` inputs and read results (stages 4, 6).
* The conflict-term fast backtrack (planned after everything is green; not started).
* Real durable storage with fsync (`raft-server`, stage 3). The sim's `FakeDisk` is durable on every `Persist`; it does not
  model lost unsynced writes yet, so invariant I-persist is only checked structurally (the host applies `Persist` before routing).
* The `rejections`/`accepted` additions to `Output` (Q2): not added. `Propose` on a non-leader is silently ignored; the sim
  proposes to the leader it believes in.
* Pre-vote, check-quorum, leadership transfer, client sessions.

### Out of scope (explicit, to prevent drift)
* **Multi-raft** (multiple groups per process, shared transport/storage across groups).
* **Batching** (of client commands, of log entries across proposals, of fsyncs across steps). One `Propose` ⇒ one entry.
* **Pipelining** (more than one in-flight `AppendEntries` per follower). The leader is stop-and-wait per follower. Note that the
  *receiving* side must still be correct under duplicated/reordered/delayed messages regardless (D1).
* Leader leases / clock-based reads [T 6.4]: forbidden by "no wall-clock in the core".
* Byzantine faults, authentication/TLS, dynamic discovery, multi-key transactions, query language, compression, flow control,
  rate limiting, observability beyond logs, non-voting learner members unless Q5 says otherwise.

## 8. Decisions where paper/thesis differ or the paper is silent

| ID | Decision | Why |
|---|---|---|
| D1 | Add a correlation field (`matchIndex` / `lastIncludedIndex`) to append/snapshot responses. | Fig 2 assumes RPC pairing; we use unpaired messages. Without it a stale/duplicate reply can corrupt `matchIndex` (safety). Placed in the marked EXT slot but **required** from stage 2. |
| D2 | `LogEntry` carries an explicit `index`. | The paper derives it from position. Explicit index makes `Persist`, snapshots and the invariant checkers unambiguous. It is redundant with `prevLogIndex + offset` on the wire; the receiver must verify consistency. |
| D3 | Membership: follow the **thesis** single-server changes [T 4.1 (?)], not the paper's joint consensus [P 6]. Include the fix that a leader must have committed an entry in its own term before starting a change (raised by Ongaro on the raft-dev list in 2015; I do not know where, or whether, it is in the thesis text — verify). | Simpler to implement and test. Decision deferred to stage 6; flagged in Q5. |
| D4 | Reads: **ReadIndex** [T 6.4] (leader commits/has committed a term entry, records `commitIndex`, confirms leadership with a heartbeat round, serves once applied). No leases. | Leases need clocks (forbidden). The paper only sketches this in [P 8]. |
| D5 | `lastApplied` is advanced when entries are emitted in `toApply`. | The `Input` set is closed (no "applied" ack). Host contract #3 covers restart. |
| D7 | `Persist` is ordered: hard state, truncate, append. A step may truncate then append, never the reverse. | Mirrors AppendEntries rules 3-4 and keeps the fake disk and the real WAL simple. |
| D8 | A node ignores messages from non-members and any `RequestVote`/`AppendEntries` whose `candidateId`/`leaderId` differs from the authenticated sender. | Defence in depth: the paper has no notion of a spoofed sender; the host authenticates `from`. |
| D9 | Responses with `matchIndex` are folded in with `max` and capped at the leader's last index; a failure never moves `nextIndex` below `matchIndex + 1`. | Duplicated or delayed replies must not regress or inflate replication progress (D1). |
| D10 | A node resets its election timer whenever it adopts a higher term (not only on a valid AppendEntries or a granted vote). | Matches common implementations; the paper's timer rule [5.2] is silent on this. Liveness only; no safety effect. |
| D11 | `Options.maxEntriesPerAppend` bounds message size (default 64). The simulator uses 1. | Bounded allocation per message. With 64 the Figure 8 hazard almost never arises randomly, because a new leader's no-op travels in the same message as older entries; with 1 the mutation test finds it. |
| D12 | The sim's liveness phase after healing demands a leader and a committed command within 1,000 ticks. | Needed to catch bugs that are not safety violations (e.g. not clearing `votedFor`). It is a bounded check, not a proof. |
| D6 | InstallSnapshot fields kept per Fig 13 (including `offset`/`done`) even if we send a single chunk. | Fidelity to the paper; chunking decision is Q6. |

## 9. Known bugs
None known. Not verified: behaviour with N other than 5, or with `maxEntriesPerAppend` other than 1 in the simulator
(the 10,000-seed test runs only the default configuration).

## 10. Open questions (need your answer or a check against the source documents)
* **Q1** Determinism guard for `raft-core`: a test that scans classes/sources for forbidden APIs (`Thread`, `synchronized`,
  `System.currentTimeMillis/nanoTime`, `Random`/`ThreadLocalRandom`, `java.io`, `java.nio.file`, `java.net`). Proposed for stage 1. OK?
* **Q2** Approve the `rejections`/`accepted` additions to `Output` (§4)? Without them the KV layer cannot correlate proposals.
* **Q3** Checkers only see `Output`s plus the sim's disk/network. If that proves too weak (e.g. observing `commitIndex`
  of a node that has not applied anything), do we add a read-only `Node.inspect()` for tests, or keep the boundary pure?
* **Q4** Pre-vote and check-quorum [T 9.6 (?), T 6.2 (?)]: in or out of scope? Currently out unless needed for liveness tests.
* **Q5** Membership changes: single-server (thesis) per D3, plus a non-voting catch-up phase [T 4.2.1 (?)]? Or joint consensus?
* **Q6** InstallSnapshot chunking: implement `offset`/`done` for real, or single-chunk only?
* **Q7** **All section numbers in this file marked (?) and the subsection-level ones in general must be verified against the PDFs.**
