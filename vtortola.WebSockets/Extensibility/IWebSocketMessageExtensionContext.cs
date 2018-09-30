using JetBrains.Annotations;

namespace vtortola.WebSockets
{
    public interface IWebSocketMessageExtensionContext
    {
        [NotNull]
        WebSocketMessageReadStream ExtendReader([NotNull] WebSocketMessageReadStream message);

        [NotNull]
        WebSocketMessageWriteStream ExtendWriter([NotNull] WebSocketMessageWriteStream message);
    }
}
