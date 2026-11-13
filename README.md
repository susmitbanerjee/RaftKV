# raft-kv-store

Raft consensus in Java 21, building towards a replicated key-value store. Learning project, not production ready.

Right now only the consensus part is done. The server and jepsen modules are empty placeholders.

## Modules

| module | what it do |
|---|---|
| `raft-core` | The Raft node itself. Pure state machine, no threads, no clock, no I/O, no dependencies. |
| `raft-sim` | Deterministic simulator. Fake network, fake disk, crashes, partitions, and checks the Raft safety rules after every step. |
| `raft-server` | Placeholder. Will have the WAL, network and the KV service. |
| `raft-jepsen` | Placeholder. Will have fault injection against real servers. |

## How the core works

You give `Node.step()` an input (a tick, a message, or a client command) and it gives back an `Output`: messages to send, things to persist, entries to apply. It never does I/O by itself, so the host has to save `Output.persist()` to disk first before sending anything out.

Because of this the same seed always gives the same run, which is how the simulator can replay a failing case.

What is done: leader election, log replication, commit rules (including the Figure 8 case). Not done yet: snapshots, membership change, read-only queries.

## Build and test

You need JDK 21.

```bash
./gradlew check
```

This runs all tests, the jqwik property tests and the simulator, and makes a JaCoCo report per module. The build also fails if `raft-core` gets any dependency, that is on purpose.

## Simulator

The simulator runs random seeds with dropped, duplicated and delayed messages, partitions and crashes. After the chaos it heals the network and checks that a leader comes up and a command commits.

If a seed fails, run it again:

```java
SimRunner.run(seed, steps, FaultSchedule.chaos());
```

It prints the seed and the whole event trace.

There is also mutation testing. `Bug` enum has 7 ways to break Raft on purpose (skip the vote check, commit old term by counting, etc) and the tests must catch every one. `Bug` is only for tests, normal code should use `Bug.NONE`.

## More

Design notes and decisions are in [DESIGN.md](DESIGN.md). Some section numbers there are from memory and not checked against the paper yet, so double check.
