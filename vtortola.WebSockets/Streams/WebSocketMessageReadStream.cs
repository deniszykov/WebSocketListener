using System;
using System.Threading;
using System.Threading.Tasks;

namespace vtortola.WebSockets
{
    public abstract class WebSocketMessageReadStream : WebSocketMessageStream
    {
        public abstract WebSocketMessageType MessageType { get; }
        public abstract WebSocketExtensionFlags Flags { get; }
        public sealed override bool CanRead => true;

        /// <inheritdoc />
        [Obsolete("Writing to the read stream is not allowed", true)]
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }
    }
}
