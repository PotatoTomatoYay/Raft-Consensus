To build and run:

```go build raft.go```

```./raft```

Flags:

| Flag | Type | Description |
| --- | --- | --- |
| -servers | int | Number of Raft servers |
| -requests | int | Number of client requests |
| -failure-rate | float64 | Every heartbeat, there will be a chance that a server will suddenly drop for some amount of time. |
| -drop-rate | float64 | Rate of failure for message sends |
| -heartbeat | int | Time between heartbeats/AppendEntry RPCs, in milliseconds |

Other flags can be found in the source code
