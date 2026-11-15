using Y51_SE2Online.Sync;

Check(!LinkGrace.Backlogged(0, long.MaxValue), "Unavailable queue-time sentinel must not extend a dead peer's grace.");
Check(!LinkGrace.Backlogged(0, 2_000_000, true), "A multi-lane connection's global queue time is not a backlog measurement.");
Check(LinkGrace.Backlogged(0, 2_000_000), "A valid single-lane queue delay must retain slow-link grace.");
Check(LinkGrace.Backlogged(128 * 1024, long.MaxValue, true), "Measured reliable bytes must retain grace even when queue time is unavailable.");
Check(!LinkGrace.Backlogged(0, -1) && !LinkGrace.Backlogged(0, 0), "Empty/unknown queue measurements must keep normal liveness timeout.");
Check(LinkGrace.Bad(300, 0, long.MaxValue), "High ping remains an independent diagnostic signal.");
Console.WriteLine("PASS valid backlog grace, unavailable queue times and multi-lane measurements.");

static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }
