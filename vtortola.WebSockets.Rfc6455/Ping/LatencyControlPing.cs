using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace vtortola.WebSockets.Rfc6455
{
    partial class WebSocketConnectionRfc6455
    {
        private sealed class LatencyControlPing : PingHandler
        {
            private readonly ArraySegment<byte> pingBuffer;
            private readonly TimeSpan pingTimeout;
            private readonly WebSocketConnectionRfc6455 connection;
            private readonly Stopwatch lastPong;

            public LatencyControlPing(WebSocketConnectionRfc6455 connection)
            {
                if (connection == null) throw new ArgumentNullException(nameof(connection));

                this.pingTimeout = connection.options.PingTimeout < TimeSpan.Zero ? TimeSpan.MaxValue : connection.options.PingTimeout;
                this.pingBuffer = connection.outPingBuffer;
                this.connection = connection;
                this.lastPong = new Stopwatch();

            }

            public override async Task PingAsync()
            {
                if (this.connection.IsClosed)
                    return;

                if (this.lastPong.Elapsed > this.pingTimeout)
                {
                    await this.connection.CloseAsync(WebSocketCloseReasons.GoingAway).ConfigureAwait(false);
                    return;
                }

                if (this.lastPong.IsRunning)
                {
                    return; // no pong is returned from last ping
                }

                ((ulong)Stopwatch.GetTimestamp()).ToBytes(this.pingBuffer.Array, this.pingBuffer.Offset);
                var messageType = (WebSocketMessageType)WebSocketFrameOption.Ping;

                var pingFrame = this.connection.PrepareFrame(this.pingBuffer, 8, true, false, messageType, WebSocketExtensionFlags.None);
                if (await this.connection.SendFrameAsync(pingFrame, TimeSpan.Zero, SendOptions.NoErrors, CancellationToken.None).ConfigureAwait(false))
                    this.lastPong.Restart();
            }
            /// <inheritdoc />
            public override void NotifyActivity()
            {

            }
            public override void NotifyPong(ArraySegment<byte> pongBuffer)
            {
                this.lastPong.Stop();

                var timeDelta = TimestampToTimeSpan(Stopwatch.GetTimestamp() - BitConverter.ToInt64(pongBuffer.Array, pongBuffer.Offset));
                this.connection.latency = TimeSpan.FromMilliseconds(Math.Max(0, timeDelta.TotalMilliseconds / 2));
            }
        }
    }
}
