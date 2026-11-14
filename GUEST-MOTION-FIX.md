# Guest motion test build

This candidate addresses guest input correction and playback timing, and returns VSync to the game's normal display setting. It includes the separately tested control-panel fix; it has not yet been tested in a live multiplayer session.

## Changes

- Missing input packets no longer repeat the last mouse pitch/yaw delta. Held movement and keyboard roll still survive short gaps; late inputs still recover their real rotation exactly once. This part runs on the host.
- A grounded turn that missed its physics position guard expires at the end of that tick and on leaving the session, instead of turning the guest later with zero mouse input.
- Guests no longer offer to skip simulation ticks. The old skip saved a completed pose, then restored it over the next tick's fresh movement/look and associated that old pose with the new input. The existing server queue and merge handling now manage rate differences. Legacy guests and their pacing flags remain supported.
- Remote playback advances according to recent elapsed time instead of assuming every client update covers 1/60 second. The existing four-update average avoids alternating double-speed/zero-speed playback when updates arrive in pairs. Physics velocity scaling and the fast-deck limit use the native physics timestep. Stall recovery, monotonic playback and physical step limits remain in place.
- The VSync override and disabled menu selector are removed. The game uses the saved preference; this does not automatically turn VSync off. The old `-y51online:vsync:free` flag is unnecessary.

The wire format and protocol version are unchanged. Update **both host and guest** to exercise all changes: the host prevents guessed mouse turns, while the guest owns playback, tick skipping and its display setting. Mixed versions can connect, but only receive the fixes on the updated side.

## Checks

With .NET 9, from the repository root:

```powershell
dotnet run --project Tests/MotionChecks -c Release
dotnet run --project Tests/PlayoutClockChecks -c Release
dotnet build Y51_SE2Online/Y51_SE2Online.csproj -c Release /p:SE2Bin="<game Game2 directory>"
dotnet run --project Tests/GroundYawChecks -c Release -- "<game Game2 directory>" "<built Y51_SE2Online.dll>"
dotnet run --project Tests/ControlPanelGroupChecks -c Release -- "<game Game2 directory>" "<built Y51_SE2Online.dll>"
```

The first two checks link the production source directly and need no game installation. They cover lost/late/duplicate inputs, held keys, rewind and merging; sustained 60 Hz inputs against a 55 Hz host; legacy pacing flags; playback at 20–120 client updates/s with 45 ms simulated RTT; fractional rates, uneven/paired updates, packet reordering, stalls, recovery and physical caps. They are deterministic simulations, not measurements of the game.

## Live comparison

1. Close the game on both PCs. Back up each installed mod DLL/PDB, then replace them with the candidate DLL/PDB. Keep the existing settings, launch arguments and other mods.
2. Restart and join the same world. Compare standing turns, walking with frequent direction changes, jetpack movement, and standing on a moving ship. Release the mouse while continuing to move and check for unwanted turns.
3. On the guest, compare VSync On and Off using the normal display menu. Keep the world, route and other display settings the same. Prefer the setting that feels better on that display; VSync Off can introduce tearing, and the original rendering jitter workaround has not been validated as unnecessary.
4. Compare host and guest logs for input queue/merge counts, guessed inputs, correction errors, playback catch-ups and simulation rate. Network ping alone does not measure these delays.
5. To roll back, close the game and restore the backed-up DLL/PDB. Restore the previous VSync preference if changed during comparison.

## Limits

A slow host can still merge inputs and produce corrections; removing client skips trades the known pose/input mismatch for that existing queue behavior. Physical speed limits intentionally take priority over catching up immediately on fast ships. The disabled-by-default `-y51online:catchupfreeze:on` experiment still restores poses and is outside this change; leave it absent for this comparison.

The earlier clock-rate discrepancies remain an investigation finding. Live host/guest testing is needed before claiming the reported sluggishness and camera motion are fully resolved, or establishing which conditional defects were active during play. No AreaRepairBoost code or authority checks change.
