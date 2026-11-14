using System.Numerics;
using Y51_SE2Online.Sync;

internal static class NonPacedChecks
{
    internal static void Run()
    {
        var input = new OwnInput(new Vector3(0, 0, -1), new Vector3(0.01f, -0.0075f, 0.5f), false, false, false);
        var normal = OwnInputs.Decode(OwnInputs.Encode(1, 1, new[] { input }))!.Value;
        Check(!normal.Paced, "new clients must not advertise tick skipping");
        var legacy = OwnInputs.Decode(OwnInputs.Encode(1, 1, new[] { input }, paced: true))!.Value;
        Check(legacy.Paced && legacy.NewestFirst[0] == input, "legacy pacing flags must remain wire compatible");

        var buffer = new OwnInputBuffer(adaptive: true);
        uint sent = 0, ack = 0;
        int deepest = 0;
        double pitch = 0, yaw = 0, roll = 0;
        for (int ms = 0, nextInput = 0, nextServer = 0; ms < 60_000; ms++)
        {
            while (nextInput * 1000.0 / 60 <= ms)
            {
                buffer.Offer(++sent, input);
                nextInput++;
            }
            while (nextServer * 1000.0 / 55 <= ms)
            {
                Take();
                deepest = Math.Max(deepest, buffer.Depth);
                nextServer++;
            }
        }
        Check(deepest <= OwnInputBuffer.FastCap, "60 Hz inputs against a 55 Hz server must keep the queue bounded without client skips");
        Check(buffer.Merged > 0 && buffer.Guessed == 0, "server merging must handle the sustained input surplus");
        while (buffer.Depth > 0) Take();
        Check(ack == sent && buffer.Real == sent, "every real input must be consumed");
        Check(Math.Abs(pitch - input.Rotation.X * (double)sent) < 1e-5 && Math.Abs(yaw - input.Rotation.Y * (double)sent) < 1e-5 && roll == input.Rotation.Z * (double)sent,
            "merging must conserve mouse turns and roll during sustained rate mismatch");

        void Take()
        {
            var tick = buffer.Take(resting: false, speed: 6, paced: false)!.Value;
            ack = tick.Ack;
            pitch += tick.Input.Rotation.X;
            yaw += tick.Input.Rotation.Y;
            roll += tick.Input.Rotation.Z;
        }
    }

    private static void Check(bool ok, string message)
    {
        if (!ok) throw new InvalidOperationException(message);
    }
}
