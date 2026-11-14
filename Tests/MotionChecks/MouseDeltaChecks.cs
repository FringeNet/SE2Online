using System.Numerics;
using Y51_SE2Online.Sync;

internal static class MouseDeltaChecks
{
    internal static void Run()
    {
        var held = new OwnInput(new Vector3(0, 0, -1), new Vector3(0.2f, -0.1f, 1), true, true, true, OwnToggles.Crouch);
        var buffer = new OwnInputBuffer();
        buffer.Offer(1, held);
        Take(buffer);
        var guessed = Take(buffer);
        Check(guessed.Rotation == new Vector3(0, 0, 1), "a missing input must not repeat mouse pitch/yaw, but must hold roll");
        Check(guessed.Movement == held.Movement && guessed.Sprint && guessed.Boost, "held movement, sprint and boost must survive a short input gap");
        Check(!guessed.Jump && guessed.Toggles == OwnToggles.None, "a guessed input must not repeat one-shot actions");
        buffer.Offer(2, held with { Rotation = new Vector3(0, 0, 1), Jump = false, Toggles = OwnToggles.None });
        buffer.Offer(3, default);
        var recovered = Take(buffer);
        Check(recovered.Rotation == Vector3.Zero && recovered.Movement == Vector3.Zero, "late zero mouse input must not reverse the old turn, and current releases must stop movement/roll");

        buffer = new OwnInputBuffer();
        buffer.Offer(1, held);
        var total = Take(buffer).Rotation + Take(buffer).Rotation;
        var late = held with { Rotation = new Vector3(0.3f, 0.15f, 0), Jump = false, Toggles = OwnToggles.None };
        var current = late with { Rotation = new Vector3(-0.05f, 0.02f, 0), Movement = Vector3.Zero };
        buffer.Offer(2, late);
        buffer.Offer(2, late);
        buffer.Offer(3, current);
        total += Take(buffer).Rotation;
        Near(total, held.Rotation + late.Rotation + current.Rotation, "late real mouse turns and held-roll release must be compensated exactly once");
        buffer.Offer(4, default);
        Check(Take(buffer).Rotation == Vector3.Zero, "late compensation must not leak into the following input");

        buffer = new OwnInputBuffer();
        buffer.Offer(1, held);
        total = Take(buffer).Rotation;
        for (int i = 0; i < 3; i++) total += Take(buffer).Rotation;
        for (uint seq = 2; seq <= 4; seq++) buffer.Offer(seq, held);
        for (int i = 0; i < 3; i++) total += Take(buffer).Rotation;
        Check(buffer.Rewound == 3, "a longer input gap must exercise the existing rewind path");
        Near(total, held.Rotation * 4, "rewinding guesses must conserve actual mouse turns and held roll");

        buffer = new OwnInputBuffer();
        for (uint seq = 1; seq <= 18; seq++) buffer.Offer(seq, held);
        total = Vector3.Zero;
        for (int i = 0; i < 8; i++) total += Take(buffer).Rotation;
        Check(buffer.Merged == 1, "a backed-up input queue must exercise merging");
        Near(total, held.Rotation * 9, "merged real inputs must preserve mouse turns and held-roll rotation");

        buffer = new OwnInputBuffer();
        buffer.Offer(1, held);
        Take(buffer);
        for (int i = 0; i < OwnInputBuffer.GuessTicks; i++) Take(buffer);
        var expired = Take(buffer);
        Check(expired.Movement == Vector3.Zero && expired.Rotation == Vector3.Zero, "prolonged input loss must still release movement and roll");
    }

    private static OwnInput Take(OwnInputBuffer buffer) => buffer.Take(resting: false)!.Value.Input;
    private static void Near(Vector3 actual, Vector3 expected, string message) => Check(Vector3.Distance(actual, expected) < 1e-5f, message);
    private static void Check(bool ok, string message)
    {
        if (!ok) throw new InvalidOperationException(message);
    }
}
