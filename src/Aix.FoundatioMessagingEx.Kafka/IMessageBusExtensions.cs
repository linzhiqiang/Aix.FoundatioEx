using Foundatio.Messaging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.FoundatioMessagingEx.Kafka
{
    public static class IMessageBusExtensions
    {
        /// <summary>
        /// 订阅 支持传递自定义参数 弥补Foundatio.Messaging.IMessageBus接口，增加MessageBusContext参数
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="messageBus"></param>
        /// <param name="handler"></param>
        /// <param name="context"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task SubscribeAsync<T>(this IMessageBus messageBus, Func<T, Task> handler, KafkaMessageBus.Model.SubscribeOptions subscribeOptions = null, CancellationToken cancellationToken = default(CancellationToken)) where T : class
        {
            //if (messageBus is InMemoryMessageBus)
            //{
            //    return messageBus.SubscribeAsync<T>(handler, cancellationToken);
            //}
            if (messageBus is KafkaMessageBusAdapter)
            {
                return (messageBus as KafkaMessageBusAdapter).SubscribeAsync<T>((message, token) =>
                {
                    return handler(message);
                }, subscribeOptions, cancellationToken);
            }
            else
            {
                return messageBus.SubscribeAsync<T>(handler, cancellationToken);
            }
        }
    }
}
