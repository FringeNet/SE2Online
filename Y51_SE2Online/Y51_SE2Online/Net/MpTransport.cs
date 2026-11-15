using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Y51_SE2Online.Core;

namespace Y51_SE2Online.Net;

public sealed class MpTransport : ITransport, IDisposable
{
	private sealed class BulkOut
	{
		public long QueuedMs;
		public int CostBytes;
		public byte Type;
		public int Length;

		public uint TransferId;

		public ReadOnlyMemory<byte> Data;

		public uint Count;

		public uint Next;

		/// <summary>A big message being deflated on a worker; it goes (and everything queued after it waits) once done.</summary>
		public System.Threading.Tasks.Task<ReadOnlyMemory<byte>?>? Packing;
	}

	private sealed class ControlOut(byte[] frame, long queuedMs, int costBytes)
	{
		public readonly byte[] Frame = frame;
		public readonly long QueuedMs = queuedMs;
		public readonly int CostBytes = costBytes;
	}

	private sealed class PeerState
	{
		public uint Conn;

		public ulong InstanceId;

		public PeerId Id;

		public bool Ready;

		public bool Closed;

		public readonly object SendLock = new();
		public readonly Queue<OutRequest> Out = new();
		public readonly Queue<ControlOut> Control = new();
		public uint NextControlSeq;
		public long QueuedBytes;
		public long ControlQueuedBytes;
		public int QueuedMessages;
		public string? PendingClose;
		public int PendingCloseCode;
		public bool FlushBeforeClose;
		public long RejectedState;

		public long LinkConnectedMs;

		public readonly uint[] TxSeq = new uint[3];

		public readonly ReliableSequenceChecker ControlRx = new();

		public readonly ReliableSequenceChecker BulkRx = new();

		public readonly ReceiveWindow StateRx = new();

		public readonly AckTracker StateAcks = new();

		public Reassembler Reassembler;

		public readonly Queue<BulkOut> Bulk = new();

		public long BulkQueuedBytes;

		public uint NextTransferId = 1u;

		public List<byte[]>? Early;

		public int EarlyBytes;

		public int HelloResends;

		public bool PingLateNoted;

		public bool StateAckDirty;

		public long LastStateSentMs;

		public long LastPingSentMs;

		public uint NextPingId = 1u;

		public double RttSum;

		public readonly PeerStats Stats = new();
	}

	private readonly struct OutRequest
	{
		public readonly Channel Channel;

		public readonly ReadOnlyMemory<byte> Payload;

		public readonly ulong Token;

		public readonly long QueuedMs;
		public readonly int CostBytes;
		public readonly byte Type;
		public readonly int Length;

		public OutRequest(Channel channel, ReadOnlyMemory<byte> payload, ulong token, long queuedMs, int costBytes, byte type, int length)
		{
			Channel = channel;
			Payload = payload;
			Token = token;
			QueuedMs = queuedMs;
			CostBytes = costBytes;
			Type = type;
			Length = length;
		}
	}

	private readonly IPacketLink _link;

	private readonly TransportOptions _o;

	private readonly IMpLog _log;

	private readonly Func<long> _clock;

	private readonly Dictionary<uint, PeerState> _byConn = new();

	private readonly Dictionary<PeerId, PeerState> _byPeer = new();

	// Producer threads capture a connection object, never a PeerId that could later name its replacement.
	private readonly ConcurrentDictionary<PeerId, PeerState> _sendPeers = new();

	private readonly List<LinkEvent> _events = new();

	private readonly List<LinkMessage> _messages = new();

	private readonly List<ulong> _scratchTokens = new();

	private PeerId[] _peers = Array.Empty<PeerId>();

	private long _nextToken;

	private volatile bool _disposed;

	public IReadOnlyList<PeerId> Peers => Volatile.Read(in _peers);

	public IMpLog Log => _log;

	public event Action<PeerId>? PeerConnected;

	public event Action<PeerId, DisconnectInfo>? PeerDisconnected;

	public event Action<PeerId, Channel, byte[]>? MessageReceived;

	public event Action<PeerId, IReadOnlyList<ulong>>? StateAcked;

	public event Action<PeerId, IReadOnlyList<ulong>>? StateLost;

	public Func<PeerId, bool>? BulkGate { get; set; }

	/// <summary>Messages accepted by the link and delivered to handlers, by message type (uncompressed sizes).</summary>
	public MessageTally Sent { get; } = new();

	public MessageTally Received { get; } = new();

	public MpTransport(IPacketLink link, TransportOptions options, IMpLog log, Func<long>? clock = null)
	{
		_link = link;
		_o = options;
		_log = log;
		_clock = clock ?? new Func<long>(MpClock.NowMs);
		if (_o.PingTimeoutMs <= 0 || _o.MaxQueuedControlBytes <= 0 || _o.MaxQueuedSendMessages <= 0)
		{
			throw new ArgumentException("send queue limits and PingTimeoutMs must be positive");
		}
		int fragmentFrame = Frame.HeaderSize + FragmentHeader.Size + _o.FragmentPayload;
		if (fragmentFrame > link.MaxMessageSize)
		{
			throw new ArgumentException($"fragment frame {fragmentFrame} B exceeds link limit {link.MaxMessageSize} B");
		}
	}

	// Thread-safe: Control is copied; Bulk/State callers keep their memory unchanged until dispatched/accepted.
	public ulong Send(PeerId peer, Channel channel, ReadOnlyMemory<byte> payload)
	{
		if ((uint)channel > (uint)Channel.State) throw new ArgumentOutOfRangeException(nameof(channel));
		int maxDirect = MaxDirectPayload(channel);
		if (channel != Channel.Bulk && payload.Length > maxDirect)
		{
			_log.WarnOnce($"oversize-{channel}", $"{channel} message of {payload.Length} B exceeds {maxDirect} B; use Channel.Bulk");
			return 0;
		}
		int maxBulk = _o.Role == 0 ? _o.MaxBulkMessageBytes : _o.MaxBulkFromClientBytes;
		if (channel == Channel.Bulk && payload.Length > maxBulk)
		{
			_log.Warn($"bulk message (type {(payload.Length > 0 ? payload.Span[0] : 0)}) of {payload.Length} B exceeds the {maxBulk} B the {(_o.Role == 0 ? "client" : "host")} accepts; not sent");
			return 0;
		}
		if (!_sendPeers.TryGetValue(peer, out var p)) return 0;
		lock (p.SendLock)
		{
			if (_disposed || p.Closed || !p.Ready || p.PendingClose != null) return 0;
			int cost = payload.Length + Frame.HeaderSize + (channel == Channel.State ? StatePrefix.Size : 0);
			if (!Reserve(p, channel, cost)) return 0;
			// Freeze the small reliable lane; bulk keeps the existing caller-owned lifetime without copying whole worlds.
			if (channel == Channel.Control) payload = payload.ToArray();
			int length = payload.Length;
			byte type = length > 0 ? payload.Span[0] : (byte)0;
			if (channel == Channel.Bulk && BulkCompression.IsEnvelope(payload.Span))
			{
				type = payload.Span[5];
				length = BulkCompression.DeclaredLength(payload.Span);
			}
			else if (channel == Channel.Bulk && payload.Length < BulkCompression.PackLaterBytes && BulkCompression.Pack(payload.Span, MaxInflatedOut) is { } packed)
			{
				payload = packed;
			}
			ulong token = (ulong)Interlocked.Increment(ref _nextToken);
			p.Out.Enqueue(new OutRequest(channel, payload, token, _clock(), cost, type, length));
			Core.LatencyTrace.Mark(Core.LatencyTrace.Queued, payload.Span, (int)channel, peer.Value);
			return token;
		}
	}

	public void Disconnect(PeerId peer, int code, string reason)
	{
		if (!_sendPeers.TryGetValue(peer, out var p)) return;
		lock (p.SendLock)
		{
			if (!p.Closed && p.PendingClose == null)
			{
				p.PendingClose = reason;
				p.PendingCloseCode = code;
				p.FlushBeforeClose = true;
			}
		}
	}

	// Also counts ingress, so a stalled Poll cannot hide an unbounded producer backlog.
	private bool Reserve(PeerState p, Channel channel, int bytes)
	{
		long maxBytes = 2L * (_o.Role == 0 ? _o.MaxBulkMessageBytes : _o.MaxBulkFromClientBytes) + _o.MaxQueuedControlBytes;
		if (p.QueuedMessages >= _o.MaxQueuedSendMessages || p.QueuedBytes + bytes > maxBytes ||
			(channel == Channel.Control && p.ControlQueuedBytes + bytes > _o.MaxQueuedControlBytes))
		{
			p.PendingClose = $"send backlog limit exceeded ({p.QueuedMessages} messages, {p.QueuedBytes} B; {channel} needs {bytes} B)";
			p.PendingCloseCode = DisconnectCodes.PingTimeout;
			return false;
		}
		p.QueuedMessages++;
		p.QueuedBytes += bytes;
		if (channel == Channel.Control) p.ControlQueuedBytes += bytes;
		return true;
	}

	private static void Release(PeerState p, Channel channel, int bytes)
	{
		lock (p.SendLock)
		{
			if (p.Closed) return;
			p.QueuedMessages--;
			p.QueuedBytes -= bytes;
			if (channel == Channel.Control) p.ControlQueuedBytes -= bytes;
		}
	}

	/// <summary>The largest message the other side inflates (<see cref="BulkCompression.Pack"/>'s limit).</summary>
	public int MaxInflatedOut => _o.Role == 0 ? _o.MaxInflatedFromHostBytes : _o.MaxInflatedFromClientBytes;

	public int MaxDirectPayload(Channel channel)
	{
		int max = _link.MaxMessageSize - Frame.HeaderSize;
		if (channel != Channel.State)
		{
			return max;
		}
		return max - StatePrefix.Size;
	}

	public bool TryGetStats(PeerId peer, out PeerStats stats)
	{
		if (_byPeer.TryGetValue(peer, out var p))
		{
			FillStats(p);
			stats = p.Stats.Clone();
			return true;
		}
		stats = null;
		return false;
	}

	public void Poll()
	{
		if (_disposed)
		{
			return;
		}
		long now = _clock();
		long started = System.Diagnostics.Stopwatch.GetTimestamp();
		_events.Clear();
		_messages.Clear();
		_link.Pump(_events, _messages);
		double pumpMs = System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds;
		// Connects first so a message arriving in the same pump finds its peer; disconnects last so
		// the peer's final messages are still delivered.
		foreach (var linkEvent in _events)
		{
			var e = linkEvent;
			if (e.Kind == LinkEventKind.Connected)
			{
				OnLinkEvent(in e, now);
			}
		}
		foreach (var message in _messages)
		{
			OnLinkMessage(message, now);
		}
		foreach (var linkEvent in _events)
		{
			var e = linkEvent;
			if (e.Kind != LinkEventKind.Connected)
			{
				OnLinkEvent(in e, now);
			}
		}
		double handleMs = System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds - pumpMs;
		int dispatched = 0;
		long dispatchedBytes = 0;
		foreach (var p in ToArray())
		{
			for (int i = 0; i < 256; i++)
			{
				OutRequest request;
				lock (p.SendLock)
				{
					if (p.Closed || (p.PendingClose != null && !p.FlushBeforeClose) || !p.Out.TryDequeue(out request)) break;
				}
				dispatched++;
				dispatchedBytes += request.Payload.Length;
				Dispatch(p, in request, now);
			}
		}
		double dispatchMs = System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds - pumpMs - handleMs;
		_lastPoll = (pumpMs, _messages.Count, handleMs, dispatched, dispatchedBytes, dispatchMs);
		if (_byConn.Count == 0)
		{
			return;
		}
		foreach (var p in ToArray())
		{
			if (!p.Closed)
			{
				Service(p, now);
			}
		}
	}

	private (double PumpMs, int In, double HandleMs, int Sends, long SendBytes, double DispatchMs) _lastPoll;

	/// <summary>Where the last Poll spent its time, for slow-loop warnings.</summary>
	public string LastPollDetail => $"pump {_lastPoll.PumpMs:0} ms, {_lastPoll.In} msgs in handled in {_lastPoll.HandleMs:0} ms, {_lastPoll.Sends} sends ({_lastPoll.SendBytes} B) in {_lastPoll.DispatchMs:0} ms";

	private PeerState[] ToArray()
	{
		var peers = new PeerState[_byConn.Count];
		_byConn.Values.CopyTo(peers, 0);
		return peers;
	}

	private void Service(PeerState p, long now)
	{
		string? close;
		int code;
		long oldest = now;
		lock (p.SendLock)
		{
			close = p.PendingClose;
			code = p.PendingCloseCode;
			if (p.Out.TryPeek(out var request)) oldest = Math.Min(oldest, request.QueuedMs);
		}
		if (p.Control.TryPeek(out var control)) oldest = Math.Min(oldest, control.QueuedMs);
		if (p.Bulk.TryPeek(out var bulk)) oldest = Math.Min(oldest, bulk.QueuedMs);
		if (now - oldest > _o.PingTimeoutMs * 10L)
		{
			Close(p, DisconnectCodes.PingTimeout, $"send backlog exceeded maximum age (oldest {now - oldest} ms)", byPeer: false, "timeout", notifyPeer: true);
			return;
		}
		if (close != null)
		{
			// Preserve preceding notices over bounded polls while accepted; a refusal must never delay closing.
			bool accepting = p.FlushBeforeClose && PumpControl(p);
			if (p.Closed) return;
			if (accepting && (p.Control.Count > 0 || p.Out.Count > 0)) return;
			Close(p, code, close, byPeer: false, "local", notifyPeer: true);
			return;
		}
		if (!p.Ready)
		{
			PumpControl(p);
			if (now - p.LinkConnectedMs > _o.HelloTimeoutMs)
			{
				Close(p, DisconnectCodes.HelloTimeout, $"no Hello within {_o.HelloTimeoutMs} ms", byPeer: false, "timeout", notifyPeer: true);
			}
			return;
		}
		if (p.StateAckDirty && now - p.LastStateSentMs >= _o.StateAckDelayMs)
		{
			SendStateFrame(p, FrameKind.Ack, default, 0, now);
		}
		if (now - p.LastPingSentMs >= _o.PingIntervalMs)
		{
			// Our Hello was seemingly never taken (the other side answers nothing until it has it): send it again.
			if (p.Stats.PongsReceived == 0 && p.Stats.PingsSent >= 3 * (p.HelloResends + 1) && p.HelloResends < 3)
			{
				p.HelloResends++;
				_log.Warn($"peer {p.Id}: no answer to {p.Stats.PingsSent} pings; sending our Hello again ({p.HelloResends}/3)");
				SendHello(p);
			}
			p.LastPingSentMs = now;
			var ping = new WireWriter(16).U32(p.NextPingId++).I64(_clock());
			SendFrame(p, Channel.Control, FrameKind.Ping, ping.ToArray(), reliable: true);
		}
		long lastHeardMs = p.Stats.LastPongMs > 0 ? p.Stats.LastPongMs : p.Stats.ConnectedMs;
		if (now - lastHeardMs > _o.PingTimeoutMs)
		{
			bool alive = now - lastHeardMs <= _o.PingTimeoutMs * 10L && _link.IsSessionAlive(p.Conn);
			if (!alive)
			{
				Close(p, DisconnectCodes.PingTimeout, $"no Pong for {now - lastHeardMs} ms", byPeer: false, "timeout", notifyPeer: true);
				return;
			}
			if (!p.PingLateNoted)
			{
				p.PingLateNoted = true;
				_log.Warn($"peer {p.Id}: no Pong for {now - lastHeardMs} ms, but the connection itself is up (a long queue ahead of our pings); waiting up to {_o.PingTimeoutMs * 10L / 1000} s");
			}
		}
		else p.PingLateNoted = false;
		_scratchTokens.Clear();
		p.StateAcks.Expire(now, _o.StateAckTimeoutMs, null, _scratchTokens);
		if (_scratchTokens.Count > 0)
		{
			Raise(StateLost, p.Id, _scratchTokens.ToArray());
		}
		if (p.Reassembler.Expire(now) > 0)
		{
			Close(p, DisconnectCodes.PingTimeout, $"reliable reassembly failed: {p.Reassembler.LastError}", byPeer: false, "timeout", notifyPeer: true);
			return;
		}
		PumpControl(p);
		if (!p.Closed && p.Control.Count == 0) PumpBulk(p);
	}

	// False means native refusal, rather than merely reaching this poll's effort budget.
	private bool PumpControl(PeerState p)
	{
		int bytes = 0;
		for (int i = 0; i < 256 && bytes < 1048576 && !p.Closed && p.Control.TryPeek(out var item); i++)
		{
			var result = _link.Send(p.Conn, Channel.Control, reliable: true, item.Frame);
			if (result == LinkSendResult.Busy) { p.Stats.SendBusy++; return false; }
			if (result != LinkSendResult.Ok)
			{
				Close(p, DisconnectCodes.PingTimeout, "reliable Control send failed", byPeer: false, "link-send", notifyPeer: false);
				return false;
			}
			p.Control.Dequeue();
			Release(p, Channel.Control, item.CostBytes);
			Frame.TryRead(item.Frame, out var header, out var payload, out _);
			AcceptedFrame(p, Channel.Control, header.Seq, item.Frame.Length);
			if (header.Kind == FrameKind.Data) AcceptedMessage(p, Channel.Control, payload);
			else if (header.Kind == FrameKind.Ping) p.Stats.PingsSent++;
			bytes += item.Frame.Length;
		}
		return true;
	}

	// Feeds bulk fragments only while the link's reliable backlog is under the high-water mark,
	// so a large transfer cannot starve Control and State traffic.
	private void PumpBulk(PeerState p)
	{
		int bytes = 0;
		for (int i = 0; i < 256 && bytes < 1048576 && !p.Closed && p.Bulk.Count > 0 && _link.GetPendingReliableBytes(p.Conn) < (_link.HasLanePriorities(p.Conn) ? _o.BulkHighWaterLanesBytes : _o.BulkHighWaterBytes); i++)
		{
			var bulk = p.Bulk.Peek();
			if (bulk.Packing != null && !Packed(p, bulk)) return;
			var chunk = Fragmenter.Slice(bulk.Data, bulk.Next, (uint)_o.FragmentPayload);
			var fragment = new FragmentHeader(bulk.TransferId, bulk.Next, bulk.Count, (uint)bulk.Data.Length, (uint)_o.FragmentPayload);
			byte[] frame = new byte[Frame.HeaderSize + FragmentHeader.Size + chunk.Length];
			uint seq = p.TxSeq[(int)Channel.Bulk] + 1;
			Frame.Write(frame, new FrameHeader(Channel.Bulk, FrameKind.Fragment, seq, 0));
			fragment.Write(frame.AsSpan(Frame.HeaderSize));
			chunk.Span.CopyTo(frame.AsSpan(Frame.HeaderSize + FragmentHeader.Size));
			switch (_link.Send(p.Conn, Channel.Bulk, reliable: true, frame))
			{
			case LinkSendResult.Busy:
				p.Stats.SendBusy++;
				return;
			case LinkSendResult.Failed:
				Close(p, DisconnectCodes.PingTimeout, "reliable Bulk send failed", byPeer: false, "link-send", notifyPeer: false);
				return;
			}
			p.TxSeq[(int)Channel.Bulk] = seq;
			ref var stats = ref p.Stats.Channels[(int)Channel.Bulk];
			stats.FramesSent++;
			stats.BytesSent += frame.Length;
			bytes += frame.Length;
			if (++bulk.Next == bulk.Count)
			{
				p.Bulk.Dequeue();
				p.BulkQueuedBytes -= bulk.Data.Length;
				Release(p, Channel.Bulk, bulk.CostBytes);
				stats.MessagesSent++;
				if (bulk.Length > 0) Sent.Add(bulk.Type, bulk.Length);
				Core.LatencyTrace.Mark(Core.LatencyTrace.Sent, bulk.Data.Span, (int)Channel.Bulk, p.Id.Value);
			}
		}
	}

	// The head of a player's bulk queue once its worker is done: the envelope, or the message as it was if deflating did not
	// pay or failed. Order holds because only the head is ever sent.
	private bool Packed(PeerState p, BulkOut bulk)
	{
		if (!bulk.Packing!.IsCompleted) return false;
		ReadOnlyMemory<byte>? packed = null;
		if (bulk.Packing.IsCompletedSuccessfully) packed = bulk.Packing.Result;
		else _log.WarnOnce("bulk-pack-failed", $"peer {p.Id}: deflating a {bulk.Data.Length} B bulk message failed, it goes as it is ({bulk.Packing.Exception?.GetBaseException().Message})");
		bulk.Packing = null;
		if (packed is { } envelope)
		{
			p.BulkQueuedBytes += envelope.Length - bulk.Data.Length;
			bulk.Data = envelope;
		}
		bulk.Count = Fragmenter.CountFor((uint)bulk.Data.Length, (uint)_o.FragmentPayload);
		return true;
	}

	private static readonly System.Threading.SemaphoreSlim PackGate = new(2);

	private void Dispatch(PeerState p, in OutRequest req, long now)
	{
		switch (req.Channel)
		{
		case Channel.Control:
			QueueControl(p, FrameKind.Data, req.Payload.Span, req.QueuedMs, req.CostBytes);
			break;
		case Channel.State:
			SendStateFrame(p, FrameKind.Data, req.Payload.Span, req.Token, now);
			Release(p, Channel.State, req.CostBytes);
			break;
		case Channel.Bulk:
			if (p.PendingClose != null && p.FlushBeforeClose)
			{
				Release(p, Channel.Bulk, req.CostBytes);
				break;
			}
			var data = req.Payload;
			var bulk = new BulkOut { QueuedMs = req.QueuedMs, CostBytes = req.CostBytes, Type = req.Type, Length = req.Length, TransferId = p.NextTransferId++, Data = data, Count = Fragmenter.CountFor((uint)data.Length, (uint)_o.FragmentPayload) };
			// A big message (a world's initial batch: ~0.3 s for 52 MiB) is deflated on a worker; pings and acks of every
			// player keep flowing meanwhile (0.17.4 deflated it here, on the network thread).
			if (data.Length >= BulkCompression.PackLaterBytes && !BulkCompression.IsEnvelope(data.Span))
			{
				int max = MaxInflatedOut;
				bulk.Packing = System.Threading.Tasks.Task.Run(async () =>
				{
					// Players rejoining a restarted server together: two worlds deflate at a time, not every one at once.
					await PackGate.WaitAsync().ConfigureAwait(false);
					try { return Volatile.Read(ref p.Closed) || _disposed ? null : BulkCompression.PackLarge(data.Span, max); }
					finally { PackGate.Release(); }
				});
				// A pack whose player left is never looked at again; its fault is observed here, not by the game's crash handler.
				bulk.Packing.ContinueWith(t => _ = t.Exception, System.Threading.Tasks.TaskContinuationOptions.OnlyOnFaulted | System.Threading.Tasks.TaskContinuationOptions.ExecuteSynchronously);
			}
			p.Bulk.Enqueue(bulk);
			p.BulkQueuedBytes += data.Length;
			Core.LatencyTrace.Mark(Core.LatencyTrace.BulkQueued, req.Payload.Span, (int)Channel.Bulk, p.Id.Value);
			break;
		}
	}

	// Every State frame piggybacks our receive window so the peer can resolve its in-flight tokens.
	private void SendStateFrame(PeerState p, FrameKind kind, ReadOnlySpan<byte> payload, ulong token, long now)
	{
		byte[] body = new byte[StatePrefix.Size + payload.Length];
		StatePrefix.Write(body, p.StateRx.Latest, p.StateRx.Bits);
		payload.CopyTo(body.AsSpan(StatePrefix.Size));
		uint seq = SendFrame(p, Channel.State, kind, body, reliable: false);
		if (kind == FrameKind.Data && token != 0 && seq != 0)
		{
			p.StateAcks.Register(seq, token, now);
		}
		if (seq != 0)
		{
			if (kind == FrameKind.Data) AcceptedMessage(p, Channel.State, payload);
			p.StateAckDirty = false;
			p.LastStateSentMs = now;
		}
		else if (token != 0)
		{
			p.RejectedState++;
			Raise(StateLost, p.Id, new[] { token });
		}
	}

	private void QueueControl(PeerState p, FrameKind kind, ReadOnlySpan<byte> payload, long queuedMs, int costBytes)
	{
		byte[] frame = Frame.Build(new FrameHeader(Channel.Control, kind, ++p.NextControlSeq, 0), payload);
		p.Control.Enqueue(new ControlOut(frame, queuedMs, costBytes));
	}

	// Returns the frame's sequence number, or 0 if the link refused it.
	private uint SendFrame(PeerState p, Channel ch, FrameKind kind, ReadOnlySpan<byte> payload, bool reliable)
	{
		if (ch == Channel.Control && kind != FrameKind.Bye)
		{
			lock (p.SendLock)
			{
				if (p.Closed || p.PendingClose != null || !Reserve(p, ch, Frame.HeaderSize + payload.Length)) return 0;
			}
			QueueControl(p, kind, payload, _clock(), Frame.HeaderSize + payload.Length);
			return 0;
		}
		uint seq = p.TxSeq[(uint)ch] + 1;
		byte[] frame = new byte[Frame.HeaderSize + payload.Length];
		Frame.Write(frame, new FrameHeader(ch, kind, seq, 0));
		payload.CopyTo(frame.AsSpan(Frame.HeaderSize));
		var result = _link.Send(p.Conn, ch, reliable, frame);
		if (result != LinkSendResult.Ok)
		{
			if (result == LinkSendResult.Busy)
			{
				p.Stats.SendBusy++;
			}
			_log.WarnOnce($"send-{result}-{p.Id}-{ch}", $"peer {p.Id}: link refused {ch}/{kind} frame ({result})");
			return 0;
		}
		AcceptedFrame(p, ch, seq, frame.Length);
		return seq;
	}

	private static void AcceptedFrame(PeerState p, Channel ch, uint seq, int bytes)
	{
		p.TxSeq[(uint)ch] = seq;
		ref var stats = ref p.Stats.Channels[(uint)ch];
		stats.FramesSent++;
		stats.BytesSent += bytes;
	}

	private void AcceptedMessage(PeerState p, Channel ch, ReadOnlySpan<byte> payload)
	{
		p.Stats.Channels[(int)ch].MessagesSent++;
		if (payload.Length > 0) Sent.Add(payload[0], payload.Length);
		Core.LatencyTrace.Mark(Core.LatencyTrace.Sent, payload, (int)ch, p.Id.Value);
	}

	private void OnLinkEvent(in LinkEvent e, long now)
	{
		PeerState closing;
		if (e.Kind == LinkEventKind.Connected)
		{
			if (!_byConn.ContainsKey(e.Conn))
			{
				var peerId = new PeerId(e.RemoteId);
				if (_byPeer.TryGetValue(peerId, out var old))
				{
					_log.Warn($"peer {peerId} reconnected on a new connection; dropping the old one");
					Close(old, DisconnectCodes.Bye, "replaced by new connection", byPeer: false, "local", notifyPeer: false);
				}
				var p = new PeerState
				{
					Conn = e.Conn,
					Id = peerId,
					LinkConnectedMs = now,
					Reassembler = _o.Role == 0 ? new Reassembler(_o.MaxBulkFromClientBytes, 8, 2L * _o.MaxBulkFromClientBytes, (int)Math.Min(int.MaxValue, _o.PingTimeoutMs * 10L)) : new Reassembler(_o.MaxBulkMessageBytes, 8, 536870912L, (int)Math.Min(int.MaxValue, _o.PingTimeoutMs * 10L))
				};
				p.Stats.Peer = peerId;
				p.Stats.ConnectedMs = now;
				_byConn[e.Conn] = p;
				_byPeer[peerId] = p;
				_sendPeers[peerId] = p;
				SendHello(p);
				_log.Info($"link up to {peerId} (conn {e.Conn}), Hello sent");
			}
		}
		else if (_byConn.TryGetValue(e.Conn, out closing))
		{
			Close(closing, e.Code, e.Reason, e.ByPeer, "link", notifyPeer: false);
		}
	}

	private void SendHello(PeerState p)
	{
		var hello = new WireWriter(128)
			.U16(Frame.ProtocolVersion)
			.U8(_o.Role)
			.U64(_o.LocalId)
			.Str(_o.PluginVersion)
			.Str(_o.GameVersion)
			.Str(_o.LocalName)
			.U64(_o.InstanceId);
		SendFrame(p, Channel.Control, FrameKind.Hello, hello.ToArray(), reliable: true);
	}

	private void OnLinkMessage(in LinkMessage m, long now)
	{
		if (!_byConn.TryGetValue(m.Conn, out var p) || p.Closed)
		{
			return;
		}
		if (!Frame.TryRead(m.Data, out var header, out var payload, out string error))
		{
			ProtocolError(p, error);
		}
		else if (!p.Ready && header.Kind != FrameKind.Hello && header.Kind != FrameKind.Bye)
		{
			// Frames can overtake the Hello across lanes; hold a few until the handshake completes.
			p.Early ??= new List<byte[]>();
			p.EarlyBytes += m.Data.Length;
			if (p.Early.Count >= _o.MaxEarlyFrames || p.EarlyBytes > _o.MaxEarlyBytes)
			{
				_log.Warn($"peer {p.Id}: protocol error: more than {_o.MaxEarlyFrames} frames or {_o.MaxEarlyBytes} B before Hello; disconnecting");
				Close(p, DisconnectCodes.ProtocolError, "too much data before Hello", byPeer: false, "local", notifyPeer: true);
			}
			else
			{
				p.Early.Add(m.Data);
			}
		}
		else
		{
			HandleFrameSafe(p, in header, payload, m.Data.Length, now);
		}
	}

	// One peer's malformed frame must not reach Poll's caller: that would stop networking for everyone.
	private void HandleFrameSafe(PeerState p, in FrameHeader h, ReadOnlySpan<byte> payload, int frameBytes, long now)
	{
		try
		{
			HandleFrame(p, in h, payload, frameBytes, now);
		}
		catch (Exception ex)
		{
			p.Stats.ProtocolErrors++;
			_log.Warn($"peer {p.Id}: protocol error: malformed {h.Channel}/{h.Kind} frame ({frameBytes} B): {ex.GetType().Name}: {ex.Message}; disconnecting");
			Close(p, DisconnectCodes.ProtocolError, $"malformed {h.Kind} frame", byPeer: false, "local", notifyPeer: true);
		}
	}

	private void HandleFrame(PeerState p, in FrameHeader h, ReadOnlySpan<byte> payload, int frameBytes, long now)
	{
		ref var channelStats = ref p.Stats.Channels[(uint)h.Channel];
		channelStats.FramesReceived++;
		channelStats.BytesReceived += frameBytes;
		if (h.Channel == Channel.State)
		{
			HandleState(p, in h, payload, now);
			return;
		}
		var check = (h.Channel == Channel.Control ? p.ControlRx : p.BulkRx).Observe(h.Seq);
		if (check != ReliableCheck.Ok)
		{
			ProtocolError(p, $"{h.Channel} sequence {check} at #{h.Seq}");
			if (check == ReliableCheck.Duplicate)
			{
				return;
			}
		}
		switch (h.Kind)
		{
		case FrameKind.Hello:
			HandleHello(p, payload);
			break;
		case FrameKind.Ping:
		{
			var pong = new WireWriter(payload.Length);
			pong.Bytes(payload);
			SendFrame(p, Channel.Control, FrameKind.Pong, pong.ToArray(), reliable: true);
			break;
		}
		case FrameKind.Pong:
		{
			// Ping payload echoed back: [u32 ping id][i64 our send time]
			if (payload.Length < 12)
			{
				ProtocolError(p, "short Pong");
				break;
			}
			var reader = new WireReader(payload);
			reader.U32();
			long sentMs = reader.I64();
			double rtt = Math.Max(0L, _clock() - sentMs);
			var stats = p.Stats;
			stats.PongsReceived++;
			stats.LastPongMs = now;
			stats.LastRttMs = rtt;
			stats.MinRttMs = Math.Min(stats.MinRttMs, rtt);
			stats.MaxRttMs = Math.Max(stats.MaxRttMs, rtt);
			p.RttSum += rtt;
			stats.AvgRttMs = p.RttSum / stats.PongsReceived;
			break;
		}
		case FrameKind.Bye:
		{
			int code = DisconnectCodes.Bye;
			string reason = "bye";
			if (payload.Length >= 6)
			{
				var reader = new WireReader(payload);
				code = reader.I32();
				reason = reader.Str();
			}
			Close(p, code, reason, byPeer: true, "bye", notifyPeer: false);
			break;
		}
		case FrameKind.Data:
			channelStats.MessagesReceived++;
			Raise(p, h.Channel, payload.ToArray());
			break;
		case FrameKind.Fragment:
		{
			if (h.Channel != Channel.Bulk || !FragmentHeader.TryRead(payload, out var fragment, out var chunk))
			{
				ProtocolError(p, "malformed fragment");
				break;
			}
			if (fragment.TotalLength > _o.MaxBulkBytesBeforeAdmit && BulkGate is { } gate && !gate(p.Id))
			{
				_log.Warn($"peer {p.Id}: protocol error: {fragment.TotalLength} B bulk transfer before the join was accepted; disconnecting");
				Close(p, DisconnectCodes.ProtocolError, "bulk data before the join was accepted", byPeer: false, "local", notifyPeer: true);
				break;
			}
			byte[] complete;
			switch (p.Reassembler.Add(in fragment, chunk, now, out complete))
			{
			case ReassemblyResult.Rejected:
				// A lost fragment breaks the reliable stream for good, and honest peers never send one.
				_log.Warn($"peer {p.Id}: protocol error: fragment rejected: {p.Reassembler.LastError}; disconnecting");
				Close(p, DisconnectCodes.ProtocolError, "fragment rejected", byPeer: false, "local", notifyPeer: true);
				break;
			case ReassemblyResult.Complete:
				channelStats.MessagesReceived++;
				Raise(p, Channel.Bulk, complete);
				break;
			}
			break;
		}
		default:
			ProtocolError(p, $"unexpected {h.Kind} on {h.Channel}");
			break;
		}
	}

	private void HandleState(PeerState p, in FrameHeader h, ReadOnlySpan<byte> payload, long now)
	{
		if (!StatePrefix.TryRead(payload, out var ackSeq, out var ackBits, out var rest))
		{
			ProtocolError(p, "short State frame");
			return;
		}
		if (ackSeq != 0 || ackBits != 0)
		{
			_scratchTokens.Clear();
			p.StateAcks.ProcessAck(ackSeq, ackBits, _scratchTokens);
			if (_scratchTokens.Count > 0)
			{
				Raise(StateAcked, p.Id, _scratchTokens.ToArray());
			}
			_scratchTokens.Clear();
			p.StateAcks.Expire(now, _o.StateAckTimeoutMs, ackSeq, _scratchTokens);
			if (_scratchTokens.Count > 0)
			{
				Raise(StateLost, p.Id, _scratchTokens.ToArray());
			}
		}
		if (h.Kind == FrameKind.Ack)
		{
			return;
		}
		if (h.Kind != FrameKind.Data)
		{
			ProtocolError(p, $"unexpected {h.Kind} on State");
			return;
		}
		var result = p.StateRx.Observe(h.Seq);
		if (result != ReceiveResult.Duplicate && result != ReceiveResult.TooOld)
		{
			p.StateAckDirty = true;
			p.Stats.Channels[(int)Channel.State].MessagesReceived++;
			Raise(p, Channel.State, rest.ToArray());
		}
	}

	private void HandleHello(PeerState p, ReadOnlySpan<byte> payload)
	{
		if (p.Ready)
		{
			// A peer that heard nothing back repeats its Hello (see Service); the same game instance is not an error.
			if (payload.Length < 8 || new WireReader(payload[^8..]).U64() != p.InstanceId || p.InstanceId == 0)
			{
				ProtocolError(p, "second Hello");
			}
			return;
		}
		ushort protocol;
		byte role;
		ulong claimedId;
		string pluginVersion;
		string gameVersion;
		string name;
		try
		{
			var reader = new WireReader(payload);
			protocol = reader.U16();
			role = reader.U8();
			claimedId = reader.U64();
			pluginVersion = reader.Str();
			gameVersion = reader.Str();
			name = reader.Str();
			// Older builds end the Hello here.
			p.InstanceId = reader.Remaining >= 8 ? reader.U64() : 0;
		}
		catch (Exception ex) when (ex is ArgumentOutOfRangeException or IndexOutOfRangeException)
		{
			_log.Warn($"peer {p.Id}: protocol error: malformed Hello ({payload.Length} B); disconnecting");
			Close(p, DisconnectCodes.ProtocolError, "malformed Hello", byPeer: false, "local", notifyPeer: true);
			return;
		}
		if (protocol != Frame.ProtocolVersion)
		{
			string message = $"version mismatch: peer runs Space Engineers 2 Online {pluginVersion} (protocol {protocol}), this PC runs {_o.PluginVersion} (protocol {2}); " + "both players need the same plugin build";
			_log.Error($"peer {p.Id} '{name}': {message}");
			Close(p, DisconnectCodes.VersionMismatch, message, byPeer: false, "local", notifyPeer: true);
			return;
		}
		if (p.InstanceId != 0)
		{
			foreach (PeerState other in _byConn.Values.ToList())
			{
				if (other != p && !other.Closed && other.InstanceId == p.InstanceId && PeerAlias.SteamOf(other.Id.Value) == PeerAlias.SteamOf(p.Id.Value))
				{
					_log.Warn($"peer {other.Id} reconnected from the same game instance as {p.Id}; dropping the old connection");
					Close(other, DisconnectCodes.Bye, "replaced by new connection", byPeer: false, "local", notifyPeer: false);
				}
			}
		}
		if (claimedId != 0 && claimedId != PeerAlias.SteamOf(p.Id.Value) && _link.AuthenticatesPeers)
		{
			_log.Warn($"peer {p.Id}: Hello claims id {claimedId}");
		}
		p.Stats.RemoteSteamId = claimedId;
		if (role == _o.Role)
		{
			_log.Warn($"peer {p.Id}: both sides are {(role == 0 ? "host" : "client")}");
		}
		if (pluginVersion != _o.PluginVersion)
		{
			_log.Warn($"peer {p.Id}: plugin version {pluginVersion} differs from ours ({_o.PluginVersion})");
		}
		if (gameVersion != _o.GameVersion)
		{
			_log.Warn($"peer {p.Id}: game version {gameVersion} differs from ours ({_o.GameVersion})");
		}
		lock (p.SendLock) p.Ready = true;
		p.Stats.Ready = true;
		p.Stats.RemoteName = name;
		p.Stats.RemotePluginVersion = pluginVersion;
		p.Stats.RemoteGameVersion = gameVersion;
		RefreshPeers();
		_log.Info($"handshake OK with {p.Id} '{name}' ({(role == 0 ? "host" : "client")}, plugin {pluginVersion}, game {gameVersion})");
		try
		{
			PeerConnected?.Invoke(p.Id);
		}
		catch (Exception ex)
		{
			_log.Error($"PeerConnected handler threw: {ex}");
		}
		if (p.Early == null)
		{
			return;
		}
		var early = p.Early;
		p.Early = null;
		long now = _clock();
		foreach (byte[] frame in early)
		{
			if (p.Closed)
			{
				break;
			}
			if (Frame.TryRead(frame, out var header, out var framePayload, out string _))
			{
				HandleFrameSafe(p, in header, framePayload, frame.Length, now);
			}
		}
	}

	private void CompressionError(PeerState p, string error)
	{
		p.Stats.ProtocolErrors++;
		_log.Warn($"peer {p.Id}: protocol error: {error}; disconnecting");
		Close(p, DisconnectCodes.ProtocolError, "bad compressed message", byPeer: false, "local", notifyPeer: true);
	}

	private void ProtocolError(PeerState p, string error)
	{
		p.Stats.ProtocolErrors++;
		_log.Warn($"peer {p.Id}: protocol error: {error}");
		if (p.Stats.ProtocolErrors >= _o.MaxProtocolErrors && !p.Closed)
		{
			Close(p, DisconnectCodes.ProtocolError, "too many protocol errors (last: " + error + ")", byPeer: false, "local", notifyPeer: true);
		}
	}

	private void Close(PeerState p, int code, string reason, bool byPeer, string source, bool notifyPeer)
	{
		if (p.Closed)
		{
			return;
		}
		lock (p.SendLock)
		{
			p.Closed = true;
			p.Out.Clear();
			p.QueuedBytes = p.ControlQueuedBytes = p.QueuedMessages = 0;
		}
		((ICollection<KeyValuePair<PeerId, PeerState>>)_sendPeers).Remove(new(p.Id, p));
		p.Control.Clear();
		p.Bulk.Clear();
		p.BulkQueuedBytes = 0;
		bool byeAccepted = false;
		if (notifyPeer)
		{
			var bye = new WireWriter(32 + reason.Length).I32(code).Str(reason);
			byeAccepted = SendFrame(p, Channel.Control, FrameKind.Bye, bye.ToArray(), reliable: true) != 0;
		}
		if (source != "link")
		{
			// Linger only when a Bye was queued, so it reaches the peer before the link closes.
			_link.Close(p.Conn, code, reason, byeAccepted);
		}
		_byConn.Remove(p.Conn);
		if (_byPeer.TryGetValue(p.Id, out var current) && current == p)
		{
			_byPeer.Remove(p.Id);
		}
		bool wasReady = p.Ready;
		RefreshPeers();
		var info = new DisconnectInfo(code, reason, byPeer, source);
		_log.Info($"peer {p.Id} disconnected: {info}");
		if (!wasReady)
		{
			return;
		}
		try
		{
			PeerDisconnected?.Invoke(p.Id, info);
		}
		catch (Exception ex)
		{
			_log.Error($"PeerDisconnected handler threw: {ex}");
		}
	}

	// Peers is read from other threads, so it is swapped as a fresh array rather than mutated.
	private void RefreshPeers()
	{
		var ready = new List<PeerId>();
		foreach (var p in _byPeer.Values)
		{
			if (p.Ready && !p.Closed)
			{
				ready.Add(p.Id);
			}
		}
		Volatile.Write(ref _peers, ready.ToArray());
	}

	private void FillStats(PeerState p)
	{
		var stats = p.Stats;
		stats.BulkQueuedMessages = p.Bulk.Count;
		stats.BulkQueuedBytes = p.BulkQueuedBytes;
		lock (p.SendLock)
		{
			stats.SendQueuedMessages = p.QueuedMessages;
			stats.SendQueuedBytes = p.QueuedBytes;
			stats.ControlQueuedBytes = p.ControlQueuedBytes;
		}
		stats.ReassemblyInProgress = p.Reassembler.InProgress;
		stats.ReassemblyBytes = p.Reassembler.PendingBytes;
		stats.ReassemblyTotalBytes = p.Reassembler.PendingTotalBytes;
		stats.SeqGaps = p.ControlRx.Gaps + p.BulkRx.Gaps;
		stats.SeqDuplicates = p.ControlRx.Duplicates + p.BulkRx.Duplicates;
		stats.StateLate = p.StateRx.Late;
		stats.StateMissing = p.StateRx.Missing;
		stats.StateDuplicates = p.StateRx.Duplicates;
		stats.StateAcked = p.StateAcks.Acked;
		stats.StateLost = p.StateAcks.Failed + p.RejectedState;
		stats.StateInFlight = p.StateAcks.InFlight;
		stats.LinkPendingBytes = _link.GetPendingReliableBytes(p.Conn);
	}

	public bool TryGetIncomingBulkProgress(PeerId peer, uint transferId, out int received, out int count)
	{
		received = count = 0;
		long startedMs;
		if (_byPeer.TryGetValue(peer, out var p))
		{
			return p.Reassembler.TryGetProgress(transferId, out received, out count, out startedMs);
		}
		return false;
	}

	private void Raise(PeerState p, Channel ch, byte[] data)
	{
		if (data.Length > 0 && data[0] == BulkCompression.Marker)
		{
			int declared = BulkCompression.DeclaredLength(data);
			// Honest senders never produce a bad envelope, so the first one ends the connection, either direction.
			if (ch != Channel.Bulk || (declared > _o.MaxBulkBytesBeforeAdmit && BulkGate is { } gate && !gate(p.Id)))
			{
				CompressionError(p, "compressed message where none is allowed");
				return;
			}
			if (BulkCompression.Unpack(data, _o.Role == 0 ? _o.MaxInflatedFromClientBytes : _o.MaxInflatedFromHostBytes) is not { } message)
			{
				CompressionError(p, $"malformed compressed message ({data.Length} B declaring {declared} B)");
				return;
			}
			data = message;
		}
		try
		{
			Core.LatencyTrace.Mark(Core.LatencyTrace.Received, data, (int)ch, p.Id.Value);
			if (data.Length > 0) Received.Add(data[0], data.Length);
			MessageReceived?.Invoke(p.Id, ch, data);
		}
		catch (Exception ex)
		{
			p.Stats.ProtocolErrors++;
			_log.Error($"peer {p.Id}: MessageReceived handler threw on a {data.Length} B {ch} message, disconnecting: {ex}");
			Close(p, DisconnectCodes.ProtocolError, "message could not be handled", byPeer: false, "local", notifyPeer: true);
		}
	}

	private void Raise(Action<PeerId, IReadOnlyList<ulong>>? ev, PeerId peer, ulong[] tokens)
	{
		try
		{
			ev?.Invoke(peer, tokens);
		}
		catch (Exception ex)
		{
			_log.Error($"State ack handler threw: {ex}");
		}
	}

	public void DisconnectAll(int code, string reason)
	{
		foreach (var p in ToArray())
		{
			Close(p, code, reason, byPeer: false, "local", notifyPeer: true);
		}
	}

	public void Dispose()
	{
		if (!_disposed)
		{
			_disposed = true;
			DisconnectAll(DisconnectCodes.Shutdown, "shutting down");
		}
	}
}
