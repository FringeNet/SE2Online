# Playout clock checks

```powershell
dotnet run --project Tests/PlayoutClockChecks -c Release
```

These source-linked checks need only .NET 9. They cover 45 ms RTT at 20/29.97/30/45/50/55/59.94/60/120 simulation ticks/s, pacing jitter at 30/120 Hz, paired ticks, client/server stalls, reordered packets, physical speed caps, reset and legacy 60 Hz behavior. No game or network session starts.

Steady playback covers roughly 15–240 ticks/s; stall/catch-up ticks keep nominal pacing. A long packet outage may require catch-up jumps during recovery. Native body velocity and fast-deck caps remain physical limits; live movement and deck physics still require game testing.
