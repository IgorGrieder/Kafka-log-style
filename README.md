# Kafka-Style Distributed Log

A distributed append-only log written in Go and tested with
[Maelstrom](https://github.com/jepsen-io/maelstrom). The project implements the
Fly.io distributed systems Kafka challenges, from a thread-safe single-node log
to an efficient two-node design.

## Protocol

The service supports the four RPCs required by the Maelstrom `kafka` workload:

- `send` appends an integer value and returns its offset.
- `poll` returns `[offset, value]` pairs at or after a requested offset.
- `commit_offsets` records the latest processed offset for each log.
- `list_committed_offsets` returns committed offsets for requested logs.

Offsets are independent per log, unique, and monotonically increasing.

## Design evolution

### [5a: single node](https://fly.io/dist-sys/5a/)

Messages and committed offsets were held in memory behind an `RWMutex`. The
write lock made offset allocation atomic for concurrent requests within one
process.

### [5b: multiple nodes](https://fly.io/dist-sys/5b/)

An in-process mutex cannot coordinate independent nodes, so logs were moved to
Maelstrom's linearizable `lin-kv` service. Appends used a compare-and-swap loop:

1. Read the current log.
2. Allocate the next offset.
3. Compare-and-swap the updated log.
4. Retry if another node won the race.

This preserved correctness across nodes, but required several internal messages
per operation and introduced CAS contention.

### [5c: efficient log](https://fly.io/dist-sys/5c/)

The final design selects the lexicographically first node ID as the deterministic
owner. Requests received by another node are forwarded to the owner, which
serializes operations against a thread-safe local store.

```text
client ──> owner ──> local log
client ──> peer ──> owner ──> local log
```

This removes shared-store CAS retries and keeps a single serialization point for
offset allocation.

## Results

The final two-node evaluation, run on August 11, 2026, used 4 concurrent clients
for 20 seconds at a target rate of 1,000 requests per second:

- 16,585 successful operations
- 0 failed operations
- 99.96% availability
- 1.02 internal server messages per operation
- approximately 76% fewer internal messages than the 5b implementation
- no lost messages and valid monotonic offsets

## Trade-offs

The owner-based design is optimized for the fault-free 5c evaluation. It avoids
CAS contention and substantially reduces network traffic, but the owner is a
single point of failure and the in-memory state is not durable. A production
design would require replication, durable storage, leader election, and
failover.

Forwarded RPCs use a timeout so an unavailable owner does not block a request
indefinitely.

## Run locally

From this directory, build the node:

```bash
go build -o maelstrom-kafka .
```

Run the final two-node workload, assuming the Maelstrom binary is in the sibling
`maelstrom` directory:

```bash
../maelstrom/maelstrom test \
  -w kafka \
  --bin ./maelstrom-kafka \
  --node-count 2 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000
```

Run the unit tests and Go race detector:

```bash
go test -race ./...
```

## Project structure

- `main.go` starts the Maelstrom node.
- `handlers.go` registers RPC handlers and forwards requests to the owner.
- `store.go` contains the synchronized append-only log and committed offsets.
- `requests.go` defines the JSON protocol types.
- `store_test.go` verifies offsets, polling, commits, and concurrent appends.
