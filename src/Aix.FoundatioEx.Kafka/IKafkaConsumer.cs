using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.FoundatioEx.Kafka
{
    internal interface IKafkaConsumer<TKey, TValue> : IDisposable
    {
        Task Subscribe(string topic, string groupId, CancellationToken cancellationToken);

        event Func<ConsumeResult<TKey, TValue>, Task> OnMessage;
        void Close();

    }
  
}
