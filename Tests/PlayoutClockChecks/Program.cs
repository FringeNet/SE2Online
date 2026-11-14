using Y51_SE2Online.Sync;

foreach (double hz in new[] { 20d, 29.97, 30d, 45d, 50d, 55d, 59.94, 60d, 120d })
{
    var result = Run($"steady {hz:0.##} Hz, 45 ms RTT", Ticks(hz, 60), Samples(60));
    Check(result.Jumps == 0, "steady playback must not periodically jump to catch the wall clock");
    Check(Math.Abs(result.PlayedSeconds - result.WallSeconds) < 0.1, "steady playback must run at the server's elapsed-time speed");
    Check(result.MaxStep <= PlayoutClock.Second / hz * (1 + PlayoutClock.MaxNudge) + 1, "steady playback must stay inside its normal step plus correction");
    Check(Math.Abs(result.Rate / PlayoutClock.Frame - 1) < 0.001, "client tick frequency must not change the estimated server wall rate");
}

var paired = Run("paired 30 Hz render / 60 Hz simulation ticks", Ticks(60, 60).Select((_, i) => i / 2 * 1000d / 30), Samples(60));
Check(paired.Jumps == 0 && paired.StepVariation < PlayoutClock.Frame * 0.001, "paired ticks must not alternate physical body velocities between double speed and zero");
var irregular = Run("irregular client pacing at 60 ticks/s", IrregularTicks(), Samples(60));
Check(irregular.Jumps == 0 && irregular.StepVariation < PlayoutClock.Frame * 0.001, "short pacing jitter must not oscillate the published physics step");
foreach (double hz in new[] { 30d, 120d })
{
    var paced = Run($"{hz:0} Hz client pacing with 5% jitter", JitteredTicks(hz), Samples(60));
    Check(paced.Jumps == 0 && Math.Abs(paced.PlayedSeconds - paced.WallSeconds) < 0.1, "pacing jitter near a refresh-rate boundary must not reintroduce playback drift");
    Check(paced.StepVariation < PlayoutClock.Second / hz * 0.06, "published step must smooth rather than amplify small pacing jitter");
}

var catchup = Ticks(60, 60).Select(t => t >= 10000 && t < 10700 ? 10700 : t >= 25000 && t < 25500 ? 25500 : t);
var recovery = Run("client stalls and back-to-back catch-up ticks", catchup, Samples(60));
Check(recovery.Jumps <= 2, "client stalls may require one catch-up each, not repeated jumps after recovery");
Check(Math.Abs(recovery.FinalLagMs - 100) < 50, "client stall recovery must settle near the configured buffer");

var packets = Samples(60).Select(s => s.ArrivalMs >= 10000 && s.ArrivalMs < 10500 ? s with { ArrivalMs = 10500 } : s).OrderBy(s => s.ArrivalMs);
var pause = Run("500 ms packet pause and burst", Ticks(60, 60), packets);
Check(pause.Jumps <= 2 && pause.LateJumps == 0 && Math.Abs(pause.FinalLagMs - 100) < 50, "a packet pause must recover without periodic catch-up jumps");

var stalledServer = Run("700 ms server stall", Ticks(60, 60), Samples(60, serverStall: true));
Check(stalledServer.Jumps <= 1 && Math.Abs(stalledServer.FinalLagMs - 100) < 50, "a stopped server clock must resume without persistent drift");

var jittered = Samples(60).Select((s, i) => s with { ArrivalMs = s.ArrivalMs + new[] { -10, 12, -3, 15, 0 }[i % 5] }).OrderBy(s => s.ArrivalMs);
var jitter = Run("reordered arrivals with 25 ms jitter", Ticks(60, 60), jittered);
Check(jitter.Jumps == 0 && Math.Abs(jitter.FinalLagMs - 100) < 75, "small reordered arrivals must remain covered by the buffer");

var capped = Run("30 Hz physical cap, then uncapped recovery", Ticks(30, 150), Samples(150), capUntilMs: 15000);
Check(capped.Jumps == 0, "a physical cap must not release a delayed catch-up jump when lifted");
Check(Math.Abs(capped.FinalLagMs - 100) < 100, "playback must eventually catch up smoothly after the physical cap is lifted");

var legacy = Run("legacy tick-counted 60 Hz", Ticks(60, 30), Samples(30), wall: false);
Check(legacy.Jumps == 0 && legacy.MaxStep <= PlayoutClock.Frame * 1.1 + 1, "legacy 60 Hz playback must retain its bounded behavior");
var reset = new PlayoutClock { StepCap = PlayoutClock.Frame * 0.75 };
reset.Arrive(PlayoutClock.Second); reset.Advance(1000);
Check(reset.Step <= reset.StepCap, "initial playback must honor the configured physical cap");
reset.Reset();
Check(!reset.Primed && reset.Step == PlayoutClock.Frame, "reset must discard the last session's elapsed step");
reset.StepCap = double.PositiveInfinity;
reset.Arrive(PlayoutClock.Frame); reset.Advance(10000);
reset.Arrive(PlayoutClock.Frame * 2); reset.Advance(10000 + 1000d / 60);
Check(Math.Abs(reset.Step - PlayoutClock.Frame) < 1, "a new session must start at its own cadence after a capped session");
Console.WriteLine("PASS elapsed-time playback, bounded cursors, physical cap, stalls, packet recovery and legacy 60 Hz.");

static IEnumerable<double> Ticks(double hz, int seconds) => Enumerable.Range(0, (int)(hz * seconds)).Select(i => i * 1000 / hz);

static IEnumerable<double> IrregularTicks()
{
    double now = 0;
    var gaps = new[] { 8d, 1000d / 30 - 8, 12d, 1000d / 30 - 12 };
    for (int i = 0; i < 3600; i++) { yield return now; now += gaps[i % gaps.Length]; }
}

static IEnumerable<double> JitteredTicks(double hz)
{
    double now = 0;
    var jitter = new[] { -0.05, 0.02, 0.05, -0.04, 0.03, -0.02, 0.01, 0 };
    for (int i = 0; i < hz * 60; i++) { yield return now; now += 1000 / hz * (1 + jitter[i % jitter.Length]); }
}

static IEnumerable<Sample> Samples(int seconds, bool serverStall = false)
{
    int gameTick = 0;
    for (int tick = 1; tick <= seconds * 60; tick++)
    {
        double wall = tick * 1000d / 60;
        if (serverStall && wall >= 10000 && wall < 10700) continue;
        yield return new Sample(wall + 22.5, ++gameTick * PlayoutClock.Frame);
    }
}

static Result Run(string name, IEnumerable<double> ticks, IEnumerable<Sample> samples, double capUntilMs = 0, bool wall = true)
{
    var clock = new PlayoutClock { MinimumDelay = 6 * PlayoutClock.Frame, Wall = wall };
    using var arrivals = samples.GetEnumerator();
    bool haveSample = arrivals.MoveNext();
    double previous = double.NaN, warmPlay = 0, warmWall = 0, lastWall = 0, maxStep = 0, minPublished = double.PositiveInfinity, maxPublished = 0;
    long warmJumps = 0, lateJumps = 0;
    bool warmed = false;
    foreach (double now in ticks)
    {
        clock.StepCap = now < capUntilMs ? PlayoutClock.Frame * 1.02 : double.PositiveInfinity;
        while (haveSample && arrivals.Current.ArrivalMs <= now)
        {
            clock.Arrive(arrivals.Current.ServerTime);
            clock.Heartbeat();
            haveSample = arrivals.MoveNext();
        }
        double play = clock.Advance(wall ? now : double.NaN);
        if (!clock.Primed) continue;
        Check(double.IsFinite(play) && double.IsFinite(clock.Step) && clock.Step >= 0, name + ": playback step must be finite and nonnegative");
        if (!double.IsNaN(previous))
        {
            Check(play >= previous, name + ": playback must not move backwards");
            if (now < capUntilMs) Check(play - previous <= clock.StepCap + 1, name + ": every physical step must honor the cap even with seconds of delay");
            if (warmed)
            {
                maxStep = Math.Max(maxStep, play - previous);
                minPublished = Math.Min(minPublished, clock.Step); maxPublished = Math.Max(maxPublished, clock.Step);
                if (now > 30000 && clock.LastTick.Jumped) lateJumps++;
            }
        }
        Check(play <= clock.Newest + PlayoutClock.Second / 2 + 1, name + ": playback must stay bounded when the stream stops");
        Check(clock.Step <= clock.StepCap, name + ": published step must retain the physical velocity cap");
        previous = play;
        if (!warmed && now >= 5000)
        {
            warmed = true; warmPlay = play; warmWall = now; warmJumps = clock.Jumps;
        }
        lastWall = now;
    }
    var result = new Result(clock.Jumps - warmJumps, maxStep, (clock.Play - warmPlay) / PlayoutClock.Second,
        (lastWall - warmWall) / 1000, (clock.Newest - clock.Play) / 10000, clock.Rate, maxPublished - minPublished, lateJumps);
    Console.WriteLine($"{name}: {result.Jumps} jumps, max step {result.MaxStep / 10000:0.0} ms, final buffer {result.FinalLagMs:0.0} ms");
    return result;
}

static void Check(bool ok, string message) { if (!ok) throw new InvalidOperationException(message); }
readonly record struct Sample(double ArrivalMs, long ServerTime);
readonly record struct Result(long Jumps, double MaxStep, double PlayedSeconds, double WallSeconds, double FinalLagMs, double Rate, double StepVariation, long LateJumps);
