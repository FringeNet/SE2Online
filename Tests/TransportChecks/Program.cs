using Y51_SE2Online.Core;
using Y51_SE2Online.Engine;
using Y51_SE2Online.Net;
using Y51_SE2Online.Sync;

ControlBusy();
MixedStream();
Failure();
Limits();
Generation();
Disconnect();
RejectionBeforeDisconnect();
Ownership();
StateBusy();
PollBudget();
HelloBusy();
ReassemblyExpiry();
ConcurrentSend();
PackingAccounting();
Console.WriteLine("PASS 14 production-source-linked transport checks; no sockets/game/SDK.");

static void Check(bool ok, string why) { if (!ok) throw new Exception(why); }

// Mirrors the producer's Finish(nextSeq++) + continuous deflate boundary, with synthetic archive bytes.
static byte[] World(uint seq, int bodyBytes = 128, EngineWire.StreamDeflater? deflater = null)
{
    var body = new byte[bodyBytes];
    new Random(123).NextBytes(body);
    BitConverter.GetBytes(seq).CopyTo(body, 0);
    var writer = new EngineWire.Writer(SyncMsg.EngineFrame);
    writer.Archive(EngineWire.Unit.ReplicationArchive, body);
    var plain = writer.Finish(seq, compress: false);
    return deflater?.Pack(plain) ?? plain;
}

static void ControlBusy()
{
    using var f = new Pair();
    var deflater = new EngineWire.StreamDeflater();
    f.A.Send(Pair.BId, Channel.Control, World(1, deflater: deflater));
    f.Tick();
    int refusals = 3;
    f.Link.Refuse = (h, _, _) => h.Channel == Channel.Control && h.Kind == FrameKind.Data && refusals-- > 0 ? LinkSendResult.Busy : null;
    f.A.Send(Pair.BId, Channel.Control, World(2, deflater: deflater));
    f.A.Send(Pair.BId, Channel.Control, World(3, deflater: deflater));
    for (int i = 0; i < 3; i++) f.Tick();
    Check(f.AStats.Channels[0].MessagesSent == 1 && f.A.Sent[(byte)SyncMsg.EngineFrame].Count == 1, "Busy is not accepted useful traffic");
    Check(f.AStats.SendQueuedMessages == 2 && f.AStats.ControlQueuedBytes > 0, "Control backlog retained and visible");
    var retry = f.Link.Attempts.Where(a => a.Header.Kind == FrameKind.Data && a.Result == LinkSendResult.Busy).ToArray();
    Check(retry.Length == 3 && retry.All(a => a.Bytes.SequenceEqual(retry[0].Bytes)), "repeated Busy retries identical encoded bytes once per poll");
    f.A.Send(Pair.BId, Channel.Control, World(4, deflater: deflater));
    f.Tick();
    Check(f.TakeInflated().SequenceEqual(new uint[] { 1, 2, 3, 4 }), "no overtaking and continuous stream still inflates exactly");
    Check(f.AStats.SendQueuedMessages == 0 && f.AStats.SendQueuedBytes == 0 && f.BStats.SeqGaps == 0, "accepted queue drains with no sequence gap");
    Check(f.AStats.Channels[0].MessagesSent == 4 && f.A.Sent[(byte)SyncMsg.EngineFrame].Count == 4, "accepted counters increment once");
    Console.WriteLine("Control: three Busy refusals preserve exact head, later frames wait; accepted world=[1,2,3,4], exact continuous inflate.");
}

static void MixedStream()
{
    using var f = new Pair();
    var deflater = new EngineWire.StreamDeflater();
    bool bulkBusy = false, controlBusy = false;
    f.Link.Refuse = (h, _, _) =>
        h.Channel == Channel.Bulk && h.Seq == 2 && !bulkBusy ? (bulkBusy = true, LinkSendResult.Busy).Item2 :
        h.Channel == Channel.Control && h.Kind == FrameKind.Data && !controlBusy ? (controlBusy = true, LinkSendResult.Busy).Item2 : null;
    for (uint i = 1; i <= 4; i++) f.A.Send(Pair.BId, i % 2 == 1 ? Channel.Bulk : Channel.Control, World(i, i % 2 == 1 ? 2200 : 128, deflater));
    for (int i = 0; i < 5; i++) f.Tick();
    Check(f.TakeInflated().SequenceEqual(new uint[] { 1, 2, 3, 4 }), "mixed lanes restore world order before continuous inflation");
    var retry = f.Link.Attempts.Where(a => a.Header.Channel == Channel.Bulk && a.Header.Seq == 2).ToArray();
    Check(retry.Length == 2 && retry[0].Bytes.SequenceEqual(retry[1].Bytes), "Bulk keeps the identical refused fragment");
    Check(f.AStats.SendQueuedBytes == 0 && f.AStats.BulkQueuedBytes == 0 && f.BStats.ProtocolErrors == 0, "mixed queues fully released");
    Console.WriteLine("Mixed: Control/fragmented Bulk Busy recovery preserves one continuous engine stream.");
}

static void Failure()
{
    foreach (var channel in new[] { Channel.Control, Channel.Bulk })
    foreach (uint refusedSeq in channel == Channel.Control ? new uint[] { 2 } : new uint[] { 1, 2 })
    {
        using var f = new Pair();
        var losses = new List<DisconnectInfo>();
        f.A.PeerDisconnected += (_, info) => losses.Add(info);
        f.Link.Refuse = (h, _, _) => h.Channel == channel && h.Seq == refusedSeq ? LinkSendResult.Failed : null;
        f.A.Send(Pair.BId, channel, World(1, 2200));
        f.A.Send(Pair.BId, channel, World(2));
        f.Tick(); f.Tick();
        Check(f.A.Peers.Count == 0 && losses.Count == 1 && losses[0].Reason.Contains("send failed"), "Failed retires reliable stream explicitly");
        Check(!f.Link.Attempts.Any(a => a.Header.Channel == channel && a.Header.Seq > refusedSeq && a.Result == LinkSendResult.Ok), "no later reliable data after Failed");
    }
    Console.WriteLine("Failed: Control and first/mid-fragment Bulk failures close once, with no later reliable acceptance.");
}

static void Limits()
{
    using (var f = new Pair())
    {
        f.RawA.SessionAlive = f.RawB.SessionAlive = true;
        string? reason = null;
        f.A.PeerDisconnected += (_, info) => reason = info.Reason;
        f.Link.Refuse = (h, _, _) => h.Kind == FrameKind.Data ? LinkSendResult.Busy : null;
        f.A.Send(Pair.BId, Channel.Control, World(1));
        f.Tick();
        f.Now += 300000; f.Tick();
        Check(f.A.Peers.Count == 1, "full existing 300 second grace remains available");
        f.Now++; f.Tick();
        Check(f.A.Peers.Count == 0 && reason!.Contains("backlog"), "permanent Busy closes on finite managed age even with live native session");
    }
    foreach (bool countLimit in new[] { false, true })
    {
        using var f = new Pair(o => { if (countLimit) o.MaxQueuedSendMessages = 2; else o.MaxQueuedControlBytes = 128; });
        byte[] body = countLimit ? Array.Empty<byte>() : new byte[100];
        Check(f.A.Send(Pair.BId, Channel.Control, body) != 0, "first bounded item admitted");
        if (countLimit) Check(f.A.Send(Pair.BId, Channel.Control, body) != 0, "second empty item admitted");
        Check(f.A.Send(Pair.BId, Channel.Control, body) == 0, "ingress itself enforces byte/count bound");
        f.Tick();
        Check(f.A.Peers.Count == 0 && f.Link.Attempts.All(a => a.Header.Kind != FrameKind.Data), "overflow closes and purges before dispatch");
    }
    using (var f = new Pair(o => { o.MaxBulkMessageBytes = 2048; o.MaxQueuedControlBytes = 128; }))
    {
        byte[] bytes = new byte[2048]; bytes[0] = 92;
        Check(f.A.Send(Pair.BId, Channel.Bulk, bytes) != 0 && f.A.Send(Pair.BId, Channel.Bulk, bytes) != 0, "two legal maximum Bulk messages fit aggregate capacity");
        Check(f.A.Send(Pair.BId, Channel.Bulk, bytes) == 0, "third maximum Bulk exceeds receive-derived aggregate capacity");
        f.Tick(); Check(f.A.Peers.Count == 0, "aggregate exhaustion retires the connection");
    }
    Console.WriteLine("Limits: finite 300s age, pre-Poll bytes and empty-message count exhaustion fail closed.");
}

static void Generation()
{
    foreach (bool pendingDisconnect in new[] { false, true })
    {
        using var f = new Pair();
        f.A.Send(Pair.BId, Channel.Control, World(1));
        if (pendingDisconnect) f.A.Disconnect(Pair.BId, 1000, "old generation close");
        var conns = LoopbackLink.Connect(f.RawA, f.RawB);
        var hello = new WireWriter(128).U16(Frame.ProtocolVersion).U8(1).U64(2).Str("test").Str("test").Str("B-new").U64(202);
        f.RawB.Send(conns.ConnB, Channel.Control, true, Frame.Build(new FrameHeader(Channel.Control, FrameKind.Hello, 1, 0), hello.ToArray()));
        f.A.Poll();
        Check(!f.Link.Attempts.Any(a => a.Header.Kind == FrameKind.Data), "old ingress cannot target replacement generation");
        Check(f.A.Send(Pair.BId, Channel.Control, World(1)) != 0, "replacement remains sendable after old queued close");
        f.Tick();
        Check(f.Link.Attempts.Single(a => a.Header.Kind == FrameKind.Data).Conn == conns.ConnA, "fresh request captures new generation");
    }
    Console.WriteLine("Generation: old queued data and disconnect never reach/close a same-pump ready replacement.");
}

static void Disconnect()
{
    using var f = new Pair();
    f.Link.Refuse = (h, _, _) => h.Kind is FrameKind.Data or FrameKind.Bye ? LinkSendResult.Busy : null;
    f.A.Send(Pair.BId, Channel.Control, World(1)); f.Tick();
    f.A.Send(Pair.BId, Channel.Control, World(2));
    f.A.Disconnect(Pair.BId, 1000, "test close");
    Check(f.A.Send(Pair.BId, Channel.Control, World(3)) == 0, "disconnect stops ingress immediately");
    f.Tick(); f.Tick();
    Check(f.A.Peers.Count == 0 && f.B.Peers.Count == 0, "Busy data and Bye do not block close");
    Check(f.Link.Attempts.Count(a => a.Header.Kind == FrameKind.Data) == 2 && f.Link.Attempts.Count(a => a.Header.Kind == FrameKind.Bye) == 1, "close tries the Busy head once then purges retries and attempts Bye only once");
    f.A.Dispose();
    Check(f.A.Send(Pair.BId, Channel.Control, new byte[] { 92 }) == 0, "disposed transport refuses Send");
    Console.WriteLine("Disconnect: bypasses Busy FIFO, purges pending data, attempts Bye once and blocks later Send.");
}

static void RejectionBeforeDisconnect()
{
    foreach (var (count, size) in new[] { (0, 0), (300, 2), (12, 131072) })
    {
        using var f = new Pair();
        var order = new List<string>();
        byte[] rejection = { (byte)SyncMsg.JoinReject, 42 };
        f.B.MessageReceived += (_, _, bytes) => { if (bytes.SequenceEqual(rejection)) order.Add("reject"); };
        f.B.PeerDisconnected += (_, _) => order.Add("close");
        for (int i = 0; i < count; i++) { byte[] bytes = new byte[size]; bytes[0] = 92; f.A.Send(Pair.BId, Channel.Control, bytes); }
        f.A.Send(Pair.BId, Channel.Control, rejection);
        f.A.Disconnect(Pair.BId, DisconnectCodes.NotAllowed, "join refused");
        Check(f.A.Send(Pair.BId, Channel.Control, new byte[] { 92 }) == 0, "disconnect seals new ingress while draining old Control");
        f.Tick();
        if (count > 0) Check(f.A.Peers.Count == 1 && order.Count == 0, "poll budget retains healthy preceding traffic and its rejection");
        for (int i = 0; i < 10 && f.A.Peers.Count > 0; i++) f.Tick();
        Check(f.Received.Count == count + 1 && order.SequenceEqual(new[] { "reject", "close" }), "all healthy Control then JoinReject precede Bye even beyond per-poll ingress/byte budgets");
    }
    using (var f = new Pair())
    {
        f.Link.Refuse = (h, _, _) => h.Channel == Channel.Bulk ? LinkSendResult.Busy : null;
        f.A.Send(Pair.BId, Channel.Bulk, World(1, 2200)); f.Tick();
        f.A.Send(Pair.BId, Channel.Bulk, new byte[BulkCompression.PackLaterBytes]);
        byte[] rejection = { (byte)SyncMsg.JoinReject, 42 };
        f.A.Send(Pair.BId, Channel.Control, rejection);
        f.A.Disconnect(Pair.BId, DisconnectCodes.NotAllowed, "join refused"); f.Tick();
        Check(f.A.Peers.Count == 0 && f.Received.Single().SequenceEqual(rejection), "disconnect does not await old Bulk or start queued Bulk packing");
        Check(f.Link.Attempts.Count(a => a.Header.Channel == Channel.Bulk) == 1, "disconnect drain never pumps Bulk");
    }
    using (var f = new Pair())
    {
        string? reason = null; f.A.PeerDisconnected += (_, info) => reason = info.Reason;
        for (int i = 0; i < 300; i++) f.A.Send(Pair.BId, Channel.Control, new byte[] { 92 });
        f.A.Disconnect(Pair.BId, 1000, "test close"); f.Tick();
        f.Now += 300001; f.Tick();
        Check(f.A.Peers.Count == 0 && reason!.Contains("maximum age"), "disconnect drain still enforces the finite queue age");
    }
    Console.WriteLine("Rejection: healthy >256 ingress and >1MiB Control drain across bounded polls before Bye; Bulk never delays close, and age stays finite.");
}

static void Ownership()
{
    using var f = new Pair();
    byte[] bytes = { 92, 1 };
    bool busy = true;
    f.Link.Refuse = (h, _, _) => busy && h.Kind == FrameKind.Data ? LinkSendResult.Busy : null;
    f.A.Send(Pair.BId, Channel.Control, bytes);
    bytes[1] = 9; f.Tick();
    bytes[1] = 7; busy = false; f.Tick();
    Check(f.Received.Single()[1] == 1, "Control owns its snapshot before Poll and throughout Busy retry");
    var tries = f.Link.Attempts.Where(a => a.Header.Kind == FrameKind.Data).ToArray();
    Check(tries.Length == 2 && tries[0].Bytes.SequenceEqual(tries[1].Bytes), "caller mutation never changes encoded retry");
    Console.WriteLine("Ownership: Control snapshot and Busy frame survive mutable caller storage; Bulk retains its existing caller lifetime.");
}

static void StateBusy()
{
    using var f = new Pair();
    var lost = new List<ulong>(); var acked = new List<ulong>();
    f.A.StateLost += (_, ids) => lost.AddRange(ids); f.A.StateAcked += (_, ids) => acked.AddRange(ids);
    f.Link.Refuse = (h, _, _) => h.Kind == FrameKind.Data ? LinkSendResult.Busy : null;
    f.A.Send(Pair.BId, Channel.Control, World(1));
    ulong refused = f.A.Send(Pair.BId, Channel.State, new byte[] { 91, 1 }); f.Tick();
    Check(lost.SequenceEqual(new[] { refused }) && f.AStats.StateInFlight == 0 && f.AStats.StateLost == 1, "refused State token resolves Lost exactly once");
    f.Link.Refuse = (h, _, _) => h.Channel == Channel.Control && h.Kind == FrameKind.Data ? LinkSendResult.Busy : null;
    ulong accepted = f.A.Send(Pair.BId, Channel.State, new byte[] { 91, 2 }); f.Tick();
    f.Now += 40; f.Tick(); f.Tick();
    Check(acked.SequenceEqual(new[] { accepted }) && lost.Count == 1 && f.AStats.ControlQueuedBytes > 0, "State heartbeat and ack proceed while reliable head Busy");
    Check(f.AStats.Channels[2].MessagesSent == 1 && f.A.Sent[91].Count == 1, "State acceptance counters exclude Busy");
    Console.WriteLine("State: refused token resolves Lost; accepted State/ack remains live behind Busy Control.");
}

static void PollBudget()
{
    using var f = new Pair(o => o.PingIntervalMs = 1000);
    for (int i = 0; i < 700; i++) f.A.Send(Pair.BId, Channel.State, new byte[] { 91 });
    f.Now += 1000; f.Tick();
    Check(f.AStats.Channels[2].MessagesSent == 256 && f.AStats.SendQueuedMessages == 444, "ingress dispatch has finite effort");
    Check(f.AStats.PingsSent == 1, "bounded dispatch still services keepalive in same poll");
    f.Tick(); f.Tick();
    Check(f.AStats.SendQueuedBytes == 0, "bounded ingress eventually drains");
    Console.WriteLine("Poll budget: 256 ingress messages per peer, maintenance runs under continuous producer backlog.");
}

static void HelloBusy()
{
    using var rawA = new LoopbackLink("A", 1); using var rawB = new LoopbackLink("B", 2);
    var link = new ScriptLink(rawA); int busy = 2;
    link.Refuse = (h, _, _) => h.Kind == FrameKind.Hello && busy-- > 0 ? LinkSendResult.Busy : null;
    using var a = new MpTransport(link, Pair.Options(1, 0), new Log(), () => 100);
    using var b = new MpTransport(rawB, Pair.Options(2, 1), new Log(), () => 100);
    byte[]? received = null; b.MessageReceived += (_, _, data) => received = data;
    LoopbackLink.Connect(rawA, rawB);
    a.Poll(); b.Poll(); a.Poll(); b.Poll();
    Check(a.Send(Pair.BId, Channel.Control, new byte[] { 92 }) != 0, "ready direction may queue Data behind Busy Hello");
    a.Poll(); b.Poll();
    var hello = link.Attempts.Where(x => x.Header.Kind == FrameKind.Hello).ToArray();
    Check(hello.Length == 3 && hello.All(x => x.Bytes.SequenceEqual(hello[0].Bytes)), "Hello retries exact bytes before Data");
    Check(received?.SequenceEqual(new byte[] { 92 }) == true && b.TryGetStats(Pair.AId, out var stats) && stats.SeqGaps == 0, "Hello/Data share FIFO sequencing");
    using var f = new Pair();
    int pongBusy = 2;
    f.Link.Refuse = (h, _, _) => h.Kind == FrameKind.Pong && pongBusy-- > 0 ? LinkSendResult.Busy : null;
    var ping = new WireWriter(16).U32(1).I64(f.Now).ToArray();
    var incoming = f.RawB.Send(f.ConnB, Channel.Control, true, Frame.Build(new FrameHeader(Channel.Control, FrameKind.Ping, 2, 0), ping));
    Check(incoming == LinkSendResult.Ok, "inject Ping over real loopback");
    f.A.Send(Pair.BId, Channel.Control, World(1));
    f.Tick(); f.Tick(); f.Tick();
    var pongs = f.Link.Attempts.Where(x => x.Header.Kind == FrameKind.Pong).ToArray();
    Check(pongs.Length == 3 && pongs.All(x => x.Bytes.SequenceEqual(pongs[0].Bytes)), "Pong retains its exact frame across repeated Busy");
    Check(f.Link.Attempts.Single(x => x.Header.Kind == FrameKind.Data).Header.Seq == 3 && f.BStats.SeqGaps == 0, "later Data cannot overtake Busy Pong");
    Console.WriteLine("Hello/Pong: exact Busy retries precede later Data without a Control sequence hole.");
}

static void ReassemblyExpiry()
{
    using var f = new Pair();
    f.RawB.SessionAlive = true;
    f.Link.Refuse = (h, _, _) => h.Channel == Channel.Bulk && h.Seq == 2 ? LinkSendResult.Busy : null;
    f.A.Send(Pair.BId, Channel.Bulk, World(1, 2200)); f.Tick();
    Check(f.BStats.ReassemblyInProgress == 1, "partial reliable transfer held");
    string? reason = null; f.B.PeerDisconnected += (_, info) => reason = info.Reason;
    f.Now += 300001;
    var pong = new WireWriter(16).U32(1).I64(f.Now).ToArray();
    f.RawA.Send(f.ConnA, Channel.Control, true, Frame.Build(new FrameHeader(Channel.Control, FrameKind.Pong, 2, 0), pong));
    f.B.Poll();
    Check(f.B.Peers.Count == 0 && reason!.Contains("reassembly"), "expired reliable transfer closes despite a healthy fresh Pong");
    Console.WriteLine("Reassembly: expired partial reliable message closes the failed stream despite healthy Pong.");
}

static void ConcurrentSend()
{
    using var f = new Pair();
    Parallel.For(0, 512, i => f.A.Send(Pair.BId, Channel.Control, new byte[] { 92, (byte)i }));
    f.Tick(); f.Tick();
    Check(f.Received.Count == 512 && f.AStats.SendQueuedMessages == 0 && f.BStats.SeqGaps == 0, "concurrent producers have one owned FIFO and balanced accounting");
    Parallel.Invoke(() => Parallel.For(0, 1000, _ => f.A.Send(Pair.BId, Channel.Control, new byte[] { 92 })), () => f.A.Disconnect(Pair.BId, 1000, "concurrent close"));
    f.Tick();
    Check(f.A.Peers.Count == 0 && f.A.Send(Pair.BId, Channel.Control, new byte[] { 92 }) == 0, "concurrent disconnect seals ingress");
    Console.WriteLine("Concurrency: parallel producers retain owned ordering; disconnect seals their connection queue.");
}

static void PackingAccounting()
{
    using var f = new Pair();
    byte[] bytes = new byte[BulkCompression.PackLaterBytes + 4096];
    new Random(77).NextBytes(bytes);
    for (int at = 16384; at < bytes.Length; at++) if (at / 16384 % 5 != 4) bytes[at] = bytes[at % 16384];
    bytes[0] = 92;
    f.Link.Refuse = (h, _, _) => h.Channel == Channel.Bulk ? LinkSendResult.Busy : null;
    f.A.Send(Pair.BId, Channel.Bulk, bytes);
    var timer = System.Diagnostics.Stopwatch.StartNew();
    while (f.Link.Attempts.All(x => x.Header.Channel != Channel.Bulk) && timer.ElapsedMilliseconds < 5000)
    {
        f.Tick(); Thread.Sleep(1);
    }
    Check(f.AStats.BulkQueuedBytes > 0 && f.AStats.BulkQueuedBytes < bytes.Length, "actual worker compression shrinks the Bulk queue");
    Check(f.AStats.SendQueuedBytes == bytes.Length + Frame.HeaderSize && f.AStats.SendQueuedMessages == 1, "original memory reservation remains conservative after packing");
    f.Link.Refuse = null;
    for (int i = 0; i < 100 && f.Received.Count == 0; i++) f.Tick();
    Check(f.Received.Single().SequenceEqual(bytes), "packed Bulk delivers original bytes");
    Check(f.AStats.SendQueuedBytes == 0 && f.AStats.SendQueuedMessages == 0, "packing releases original reservation once at final acceptance");
    Console.WriteLine("Packing: real worker compression keeps original reservation, then releases it once after complete delivery.");
}

sealed class Pair : IDisposable
{
    public static readonly PeerId AId = new(1), BId = new(2);
    public readonly LoopbackLink RawA = new("A", 1), RawB = new("B", 2);
    public readonly ScriptLink Link;
    public readonly MpTransport A, B;
    public readonly uint ConnA;
    public readonly uint ConnB;
    public readonly LifecycleSequencer<byte[]> World = new();
    public readonly List<byte[]> Received = new();
    public long Now = 100;
    public Pair(Action<TransportOptions>? configure = null)
    {
        Link = new ScriptLink(RawA);
        var options = Options(1, 0); configure?.Invoke(options);
        A = new(Link, options, new Log(), () => Now);
        B = new(RawB, Options(2, 1), new Log(), () => Now);
        B.MessageReceived += (_, _, message) => { Received.Add(message); if (message.Length > 0 && message[0] == (byte)SyncMsg.EngineFrame) World.Offer(EngineWire.SeqOf(message), message); };
        (ConnA, ConnB) = LoopbackLink.Connect(RawA, RawB);
        Tick(); Tick();
        if (A.Peers.Count != 1 || B.Peers.Count != 1) throw new Exception("handshake");
    }
    public static TransportOptions Options(ulong id, byte role) => new() { LocalId = id, Role = role, LocalName = id.ToString(), PluginVersion = "test", GameVersion = "test", InstanceId = id + 100, FragmentPayload = 1024, PingIntervalMs = int.MaxValue };
    public void Tick() { A.Poll(); B.Poll(); }
    public PeerStats AStats => A.TryGetStats(BId, out var s) ? s : throw new Exception("A stats");
    public PeerStats BStats => B.TryGetStats(AId, out var s) ? s : throw new Exception("B stats");
    public List<uint> TakeInflated()
    {
        var inflater = new EngineWire.StreamInflater(); var seqs = new List<uint>();
        while (World.TryTake(out uint seq, out var frame))
        {
            byte[] plain = inflater.Unpack(frame);
            int bodyBytes = plain.Length - EngineWire.HeaderBytes - 5;
            if (!plain.SequenceEqual(ProgramWorld(seq, bodyBytes))) throw new Exception("continuous stream bytes changed");
            seqs.Add(seq);
        }
        return seqs;
    }
    // Top-level local functions are not visible from a type; reproduce only this tiny uncompressed fixture boundary.
    static byte[] ProgramWorld(uint seq, int length)
    {
        var body = new byte[length]; new Random(123).NextBytes(body); BitConverter.GetBytes(seq).CopyTo(body, 0);
        var writer = new EngineWire.Writer(SyncMsg.EngineFrame); writer.Archive(EngineWire.Unit.ReplicationArchive, body); return writer.Finish(seq, false);
    }
    public void Dispose() { A.Dispose(); B.Dispose(); RawA.Dispose(); RawB.Dispose(); }
}

sealed class ScriptLink(LoopbackLink inner) : IPacketLink
{
    public record Attempt(uint Conn, FrameHeader Header, LinkSendResult Result, byte[] Bytes);
    public readonly List<Attempt> Attempts = new();
    public Func<FrameHeader, bool, byte[], LinkSendResult?>? Refuse;
    public string Name => inner.Name;
    public int MaxMessageSize => inner.MaxMessageSize;
    public bool AuthenticatesPeers => inner.AuthenticatesPeers;
    public void Pump(List<LinkEvent> events, List<LinkMessage> messages) => inner.Pump(events, messages);
    public LinkSendResult Send(uint conn, Channel channel, bool reliable, ReadOnlySpan<byte> frame)
    {
        if (!Frame.TryRead(frame, out var h, out _, out var error)) throw new Exception(error);
        byte[] copy = frame.ToArray();
        var result = Refuse?.Invoke(h, reliable, copy) ?? inner.Send(conn, channel, reliable, frame);
        Attempts.Add(new(conn, h, result, copy));
        return result;
    }
    public int GetPendingReliableBytes(uint conn) => inner.GetPendingReliableBytes(conn);
    public bool IsSessionAlive(uint conn) => inner.IsSessionAlive(conn);
    public void Close(uint conn, int code, string reason, bool linger) => inner.Close(conn, code, reason, linger);
    public void Dispose() => inner.Dispose();
}

sealed class Log : IMpLog
{
    public void Info(string message) { }
    public void Warn(string message) { }
    public void Error(string message) => Console.WriteLine("ERROR " + message);
}
