using System.Reflection;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;

if (args.Length != 2) throw new ArgumentException("Usage: RecoveryIntegrationChecks <Game2> <built Y51_SE2Online.dll>");
var game = Path.GetFullPath(args[0]);
var pluginPath = Path.GetFullPath(args[1]);
AssemblyLoadContext.Default.Resolving += (_, name) =>
{
    foreach (var directory in new[] { Path.GetDirectoryName(pluginPath)!, game })
    {
        var path = Path.Combine(directory, name.Name + ".dll");
        if (File.Exists(path)) return AssemblyLoadContext.Default.LoadFromAssemblyPath(path);
    }
    return null;
};
const BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;
var plugin = AssemblyLoadContext.Default.LoadFromAssemblyPath(pluginPath);
var clientType = plugin.GetType("Y51_SE2Online.Sync.ClientSession", true)!;
var transportType = plugin.GetType("Y51_SE2Online.Net.ITransport", true)!;
var logType = plugin.GetType("Y51_SE2Online.Core.IMpLog", true)!;
var peerType = plugin.GetType("Y51_SE2Online.Net.PeerId", true)!;
var peer = Activator.CreateInstance(peerType, [42UL]);
var transport = DispatchProxy.Create(transportType, typeof(CallSpy));
var log = DispatchProxy.Create(logType, typeof(CallSpy));
int disconnects = 0, warnings = 0;
((CallSpy)transport).Call = (method, values) =>
{
    Check(method.Name == "Disconnect" && Equals(values![0], peer), "Recovery must close its own transport peer.");
    Check((int)values![1]! == 1001 && (string)values[2]! == "missing world frame", "Recovery must preserve its failure code and diagnostic.");
    disconnects++;
    return null;
};
((CallSpy)log).Call = (method, _) => { Check(method.Name == "Warn", "Expected one recovery diagnostic."); warnings++; return null; };
object Session(string phase = "Accepted", bool detached = false, bool withHost = true)
{
    var session = RuntimeHelpers.GetUninitializedObject(clientType);
    clientType.GetField("_t", flags)!.SetValue(session, transport);
    clientType.GetField("_log", flags)!.SetValue(session, log);
    clientType.GetField("_phase", flags)!.SetValue(session, Enum.Parse(clientType.GetNestedType("Phase")!, phase));
    clientType.GetField("_detached", flags)!.SetValue(session, detached);
    if (withHost) clientType.GetProperty("Host")!.SetValue(session, peer);
    return session;
}
var fail = clientType.GetMethod("FailConnection", flags)!;
void Fail(object session) => fail.Invoke(session, ["missing world frame"]);
var active = Session();
Fail(active); Fail(active);
Check(disconnects == 1 && warnings == 1, "Repeated watchdog/receive failures must request loss only once.");
Check(!(bool)clientType.GetProperty("LeftOnPurpose")!.GetValue(active)!, "Recoverable failure must not become an intentional leave.");
Check((bool)clientType.GetProperty("ConnectionFailureRequested", flags)!.GetValue(active)!, "Further receive callbacks must see the retired stream.");
foreach (var inactive in new[] { Session("Rejected"), Session("Disconnected"), Session(detached: true), Session(withHost: false) }) Fail(inactive);
Check(disconnects == 1 && warnings == 1, "Rejected, detached, disconnected or unbound sessions must not close another stream.");
Console.WriteLine("PASS compiled ClientSession recovery requests one real peer disconnect, preserves diagnostic, avoids LeftOnPurpose and ignores inactive sessions.");

var engineType = plugin.GetType("Y51_SE2Online.Engine.Y51PeerTransport", true)!;
var engineClientType = plugin.GetType("Y51_SE2Online.Engine.Y51ClientTransport", true)!;
var hostType = plugin.GetType("Y51_SE2Online.Engine.OwnHost", true)!;
var sequencerType = plugin.GetType("Y51_SE2Online.Sync.LifecycleSequencer`1", true)!.MakeGenericType(typeof(byte[]));
var offerEngine = engineType.GetMethod("Offer", flags)!;
var checkFailure = engineType.GetMethod("CheckInboundFailure", flags)!;
var takeInbound = engineType.GetMethod("TryTakeInbound", flags)!;
var pendingBytes = engineType.GetField("_pendingBytes", flags)!;
var framesField = engineType.GetField("_frames", flags)!;
long byteLimit = (long)engineType.GetField("MaxPendingBytesFromHost", flags)!.GetRawConstantValue()!;
int frameLimit = (int)engineType.GetField("MaxPendingFramesFromHost", flags)!.GetRawConstantValue()!;
long gapTimeout = (long)engineType.GetField("WorldGapTimeoutMs", flags)!.GetRawConstantValue()!;

object Engine()
{
    // Skip world/session constructors: only the production admission queue and its host role are needed here.
    var engine = RuntimeHelpers.GetUninitializedObject(engineClientType);
    var ownHost = RuntimeHelpers.GetUninitializedObject(hostType);
    engineType.GetField("Host", flags)!.SetValue(engine, ownHost);
    engineType.GetField("Name", flags)!.SetValue(engine, "recovery-check");
    engineType.GetField("_inboundGate", flags)!.SetValue(engine, new object());
    framesField.SetValue(engine, Activator.CreateInstance(sequencerType));
    engineType.GetField("_state", flags)!.SetValue(engine, new ConcurrentQueue<byte[]>());
    return engine;
}
bool OfferEngine(object engine, byte[] frame) => (bool)offerEngine.Invoke(engine, [frame])!;
string? Failure(object engine, long now) => (string?)checkFailure.Invoke(engine, [now]);
long Pending(object engine) => (long)pendingBytes.GetValue(engine)!;
int FrameCount(object engine, string property) => (int)sequencerType.GetProperty(property)!.GetValue(framesField.GetValue(engine))!;
byte[]? TakeEngine(object engine, bool state = false)
{
    object?[] values = [state, null];
    return (bool)takeInbound.Invoke(engine, values)! ? (byte[])values[1]! : null;
}
byte[] Frame(uint seq, int length = 6)
{
    var frame = new byte[length];
    frame[0] = 90; // SyncMsg.EngineFrame, followed by its inner world sequence.
    BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(1), seq);
    return frame;
}
int StateCount(object engine) => ((ConcurrentQueue<byte[]>)engineType.GetField("_state", flags)!.GetValue(engine)!).Count;

var emptyEngine = Engine();
Check(Failure(emptyEngine, long.MaxValue) == null, "No world frames must not create a gap failure.");
var counted = Engine();
var firstFrame = Frame(1, 8);
var earlyFrame = Frame(3, 16);
var stateFrame = new byte[] { 91, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
Check(OfferEngine(counted, firstFrame) && OfferEngine(counted, earlyFrame) && OfferEngine(counted, stateFrame), "Guest admission must retain ready, early and sampled state work.");
Check(Pending(counted) == firstFrame.Length + earlyFrame.Length + stateFrame.Length, "The byte budget must include ready, early and State queues together.");
Check(ReferenceEquals(TakeEngine(counted), firstFrame) && Pending(counted) == earlyFrame.Length + stateFrame.Length, "Taking a ready frame must debit its exact bytes.");
Check(TakeEngine(counted) == null && Pending(counted) == earlyFrame.Length + stateFrame.Length, "A missing sequence must not release or debit early data.");
Check(ReferenceEquals(TakeEngine(counted, state: true), stateFrame) && Pending(counted) == earlyFrame.Length, "Taking sampled state must debit its exact bytes.");
var missingFrame = Frame(2, 12);
Check(OfferEngine(counted, missingFrame), "A delayed missing frame must remain admissible.");
Check(ReferenceEquals(TakeEngine(counted), missingFrame) && ReferenceEquals(TakeEngine(counted), earlyFrame) && Pending(counted) == 0, "Resolving a gap must drain exact original frames and restore the budget.");

foreach (uint seq in new uint[] { 1, 2 })
{
    var fullBytes = Engine();
    var frame = Frame(seq);
    // Seed resident byte accounting so the real limit can be exercised without allocating 512 MiB per case.
    pendingBytes.SetValue(fullBytes, byteLimit - frame.Length);
    Check(OfferEngine(fullBytes, frame) && Pending(fullBytes) == byteLimit, "A ready or early frame may fill the byte budget exactly.");
    Check(OfferEngine(fullBytes, stateFrame) && StateCount(fullBytes) == 0 && Pending(fullBytes) == byteLimit, "Lossy State work must drop rather than exceed the byte budget.");
    Check(!OfferEngine(fullBytes, Frame(seq + 1)) && Pending(fullBytes) == byteLimit, "A reliable frame beyond the byte budget must retire the stream without changing accounting.");
    Check(Failure(fullBytes, 0)?.Contains("MiB") == true && TakeEngine(fullBytes) == null, "A byte-exhausted stream must report a persistent recoverable failure and stop applying work.");
}

var fullCount = Engine();
for (uint seq = 1; seq < frameLimit; seq++) Check(OfferEngine(fullCount, Frame(seq)), "Ready frame admission below the count limit must succeed.");
Check(OfferEngine(fullCount, Frame((uint)frameLimit + 1)), "One early frame may fill the remaining count budget.");
Check(FrameCount(fullCount, "Ready") == frameLimit - 1 && FrameCount(fullCount, "Buffered") == 1, "The count check must include ready and early frames together.");
Check(!OfferEngine(fullCount, Frame((uint)frameLimit)), "Even a gap-closing frame must not exceed the resident frame count.");
Check(Failure(fullCount, 0)?.Contains("world frames") == true, "Count exhaustion must report a persistent recoverable failure.");

var stateCount = Engine();
for (int i = 0; i <= 4096; i++) Check(OfferEngine(stateCount, stateFrame), "Lossy State count pressure must not retire the ordered world stream.");
Check(StateCount(stateCount) == 4096 && Pending(stateCount) == 4096L * stateFrame.Length, "The State count limit must leave both queue and byte accounting bounded.");
while (TakeEngine(stateCount, state: true) != null) { }
Check(Pending(stateCount) == 0 && Failure(stateCount, long.MaxValue) == null, "Draining State must restore its budget without creating a world gap.");

var readyOnly = Engine();
Check(OfferEngine(readyOnly, Frame(1)) && Failure(readyOnly, long.MaxValue) == null, "Ready scene consumer lag must never become a missing-world-sequence failure.");
var gap = Engine();
Check(OfferEngine(gap, Frame(2)), "A later frame must be retained for gap recovery.");
long gapSince = (long)sequencerType.GetField("_gapSinceMs", flags)!.GetValue(framesField.GetValue(gap))!;
Check(Failure(gap, gapSince + gapTimeout - 1) == null, "The compiled guest path must allow the entire cross-lane Bulk grace period.");
Check(Failure(gap, gapSince + gapTimeout)?.Contains("#1") == true, "The compiled guest watchdog must expose a permanent gap without more offers.");
Check(!OfferEngine(gap, Frame(1)) && TakeEngine(gap) == null, "An expired ordered stream must remain retired until a new session, without skipping or applying dependent frames.");
Console.WriteLine("PASS compiled guest engine admission: ready+early+State bytes, exact debit/release, byte/count limits, lossy State pressure, no false consumer-lag gap and persistent timed gap failure. No game/world/network started.");

static void Check(bool ok, string message) { if (!ok) throw new InvalidOperationException(message); }

public class CallSpy : DispatchProxy
{
    public Func<MethodInfo, object?[]?, object?> Call = null!;
    protected override object? Invoke(MethodInfo? method, object?[]? values) => Call(method!, values);
}
