using Foundatio.Messaging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.FoundatioMessagingEx.Kafka
{
    public class KafkaMessageBusAdapter : IMessageBus
    {
        Aix.KafkaMessageBus.IKafkaMessageBus _messageBus;
        public KafkaMessageBusAdapter(Aix.KafkaMessageBus.IKafkaMessageBus messageBus)
        {
            _messageBus = messageBus;
        }
        public void Dispose()
        {

        }

        public async Task PublishAsync(Type messageType, object message, TimeSpan? delay = null, CancellationToken cancellationToken = default)
        {
            if (delay.HasValue && delay.Value > TimeSpan.Zero) throw new NotImplementedException("未实现延迟任务");
            await _messageBus.PublishAsync(messageType, message);
        }

        public async Task SubscribeAsync<T>(Func<T, CancellationToken, Task> handler, CancellationToken cancellationToken = default) where T : class
        {
            await SubscribeAsync<T>(handler,null, cancellationToken);
        }

        public async Task SubscribeAsync<T>(Func<T, CancellationToken, Task> handler, KafkaMessageBus.Model.SubscribeOptions subscribeOptions, CancellationToken cancellationToken = default) where T : class
        {
            await _messageBus.SubscribeAsync<T>((message) =>
            {
                return handler(message, cancellationToken);
            }, subscribeOptions, cancellationToken);
        }
    }
}
