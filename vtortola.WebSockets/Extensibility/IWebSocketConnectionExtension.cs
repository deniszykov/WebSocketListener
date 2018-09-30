using System.Threading.Tasks;
using JetBrains.Annotations;
using vtortola.WebSockets.Transports;

namespace vtortola.WebSockets
{
    public interface IWebSocketConnectionExtension
    {
        [ItemNotNull, NotNull]
        Task<NetworkConnection> ExtendConnectionAsync([NotNull] NetworkConnection networkConnection);

        [NotNull]
        IWebSocketConnectionExtension Clone();
    }
}
