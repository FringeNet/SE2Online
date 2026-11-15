using System.Reflection;
using Y51_SE2Online.Engine;
using Y51_SE2Online.Sync;

const long Timeout = 300_000;
var frames = new LifecycleSequencer<byte[]>();
Check(!frames.TryGetExpiredGap(Timeout * 2, Timeout, out _), "no traffic creates no gap");
Check(frames.Offer(2, new byte[] { 2 }, 0), "later frame is retained at time zero");
Check(!frames.TryGetExpiredGap(Timeout - 1, Timeout, out _), "a delayed frame has the full grace period");
Check(frames.TryGetExpiredGap(Timeout, Timeout, out uint missing) && missing == 1, "a permanent gap expires without more offers or a dead connection");
Check(!frames.Offer(2, new byte[] { 99 }, Timeout - 1), "duplicate early data is ignored");
Check(frames.TryGetExpiredGap(Timeout, Timeout, out _), "duplicates cannot prolong a gap");

frames = new();
frames.Offer(2, new byte[] { 2 }, 10);
frames.Offer(4, new byte[] { 4 }, 20);
frames.Offer(1, new byte[] { 1 }, Timeout);
Check(frames.NextExpected == 3 && frames.Ready == 2 && frames.Buffered == 1, "a delayed missing frame releases only its contiguous prefix");
Check(!frames.TryGetExpiredGap(Timeout + 100, Timeout, out _), "advancing the missing sequence starts a fresh deadline");
Check(frames.TryGetExpiredGap(Timeout * 2, Timeout, out missing) && missing == 3, "the new missing sequence has its own bound");
frames.Offer(3, new byte[] { 3 }, Timeout * 2);
Check(!frames.TryGetExpiredGap(Timeout * 10, Timeout, out _), "a ready-only scene backlog never becomes a gap");
Check(Take(frames).Select(f => f[0]).SequenceEqual(new byte[] { 1, 2, 3, 4 }), "delayed gaps release in original order");
frames.Offer(6, new byte[] { 6 }, Timeout * 10);
Check(!frames.TryGetExpiredGap(Timeout * 10 + 1, Timeout, out _), "a later independent gap starts from its own arrival");

var wrap = new LifecycleSequencer<byte[]>();
typeof(LifecycleSequencer<byte[]>).GetField("_next", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(wrap, uint.MaxValue);
wrap.Offer(0, new byte[] { 0 }, 100);
Check(wrap.TryGetExpiredGap(Timeout + 100, Timeout, out missing) && missing == uint.MaxValue, "wraparound later data still exposes the missing sequence");
wrap.Offer(uint.MaxValue, new byte[] { 255 }, 200);
Check(wrap.NextExpected == 1 && Take(wrap).Select(f => f[0]).SequenceEqual(new byte[] { 255, 0 }), "wraparound releases the exact contiguous stream");
Check(!wrap.TryGetExpiredGap(Timeout * 2, Timeout, out _), "resolved wraparound gaps clear");

var bounded = new LifecycleSequencer<byte[]> { MaxBuffered = 1 };
Check(bounded.Offer(2, new byte[] { 2 }, 0) && !bounded.Offer(3, new byte[] { 3 }, 1), "early count overflow is rejected");
Check(bounded.Overflow == 1 && bounded.NextExpected == 1 && bounded.Buffered == 1 && bounded.Ready == 0, "overflow never skips or releases a missing sequence");

var deflater = new EngineWire.StreamDeflater();
var inflater = new EngineWire.StreamInflater();
var encoded = Enumerable.Range(1, 3).Select(i =>
{
    var writer = new EngineWire.Writer(SyncMsg.EngineFrame);
    writer.Archive(EngineWire.Unit.ReplicationArchive, Enumerable.Repeat((byte)i, 512).ToArray());
    return deflater.Pack(writer.Finish((uint)i, compress: false));
}).ToArray();
Check(encoded.All(f => (f[5] & 2) != 0), "the regression uses the real continuous deflater");
frames = new();
frames.Offer(1, encoded[0], 0);
frames.Offer(3, encoded[2], 1000);
var first = Take(frames).Single();
Check(ReferenceEquals(first, encoded[0]), "the sequencer retains exact encoded frame references");
var firstPlain = inflater.Unpack(first);
Check(!frames.TryGetExpiredGap(257_000, Timeout, out _), "a 256 second large Bulk delay remains within the conservative bound");
frames.Offer(2, encoded[1], 257_000);
foreach (byte[] frame in Take(frames))
{
    Check(ReferenceEquals(frame, encoded[EngineWire.SeqOf(frame) - 1]), "later compressed frames are never replaced or re-encoded");
    byte[] plain = inflater.Unpack(frame);
    Check(plain.Length == firstPlain.Length && plain[^1] == (byte)EngineWire.SeqOf(frame), "the real inflater remains continuous across a delayed gap");
}

Console.WriteLine("PASS world gap expiry, delayed release/reset, duplicates, ready backlog, wraparound and ordered continuous compression.");

static IEnumerable<byte[]> Take(LifecycleSequencer<byte[]> frames)
{
    while (frames.TryTake(out _, out byte[] frame)) yield return frame;
}

static void Check(bool pass, string message)
{
    if (!pass) throw new Exception(message);
}
