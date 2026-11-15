# Transport checks

Run from the repository root with a .NET 9 SDK:

```powershell
dotnet run --project Tests/TransportChecks/TransportChecks.csproj --configuration Release
```

This executable links production `MpTransport`, `LoopbackLink`, `EngineWire` and
`LifecycleSequencer` sources. A deterministic link decorator refuses selected
acceptance attempts; accepted traffic traverses the real loopback send/receive
path. Only archive producer bytes are synthetic. It runs no game, Steam, socket
or SDK calls and adds no packages. Linked legacy sources emit nullable warnings.

The checks cover repeated identical Control retries, later-frame ordering,
mixed Control/fragmented Bulk with continuous inflation, Failed sends, permanent
Busy, ingress byte/count/aggregate limits, connection replacement, bounded
disconnect flushing beyond 256 requests and 1 MiB, join-rejection ordering, mutable Control ownership, State
Lost/ACK resolution, bounded Poll maintenance, Busy Hello, reassembly expiry,
parallel Send/Disconnect and actual worker compression accounting.

The queue has two explicit options: 16 MiB of Control and 65,536 total messages
per connection. Total original-byte reservation allows two maximum Bulk messages
plus that Control budget (528 MiB host / 80 MiB client with defaults). It includes
ingress, waiting compression and partially accepted messages; reservations stay
at original size after compression. Bytes are a finite acceptance budget, not a
claim of unique native bytes or process memory usage. Bulk/State retain their
existing caller-owned memory lifetime; only Control is copied at Send.

Poll dispatches at most 256 ingress requests per connection and attempts at most
256 frames / approximately 1 MiB for each reliable lane per service. A Busy
Control head stops Bulk admission but State/ACK/maintenance continue. Explicit
disconnect seals ingress, drains preceding healthy Control across bounded polls
(preserving rejection notices), then purges Bulk and attempts Bye once. Bulk
compression/completion never delays closing. Native refusal or the existing age
ceiling ends the drain immediately, so a congested/failed link may still prevent
a final notice delivery.

Oldest managed work and inactive reassembly use the existing ten-times-Pong
grace: 300 seconds by default. This bounds failure even if Pongs remain healthy;
it is an absolute managed message-age ceiling, so a maximum 256 MiB uncompressed
transfer averaging below roughly 0.85 MiB/s can expire. These managed checks
establish acceptance and recovery behavior, not real Steam/network throughput.
