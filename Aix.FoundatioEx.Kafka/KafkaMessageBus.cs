using Aix.FoundatioEx.Kafka.Utils;
using Confluent.Kafka;
using Foundatio.AsyncEx;
using Foundatio.Messaging;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.FoundatioEx.Kafka
{
    /// <summary>
    /// kafka版MessageBus，每个类型创建一个topic, 支持手工提交offset
    /// </summary>
    public class KafkaMessageBus : IMessageBus
    {
        #region 属性 构造
        private IServiceProvider _serviceProvider;
        private ILogger<KafkaMessageBus> _logger;
        private KafkaMessageBusOptions _kafkaOptions;
        IKafkaProducer<Null, MessageBusData> _producer = null;
        List<IKafkaConsumer<Null, MessageBusData>> _consumerList = new List<IKafkaConsumer<Null, MessageBusData>>();

        ConcurrentDictionary<string, List<SubscriberInfo>> _subscriberDict = new ConcurrentDictionary<string, List<SubscriberInfo>>();
        HashSet<string> _subscriberTopicSet = new HashSet<string>();
        public KafkaMessageBus(IServiceProvider serviceProvider, ILogger<KafkaMessageBus> logger, KafkaMessageBusOptions kafkaOptions)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _kafkaOptions = kafkaOptions;

            if ((kafkaOptions.ClientMode & ClientMode.Producer) == ClientMode.Producer)
            {
                this._producer = new KafkaProducer<Null, MessageBusData>(serviceProvider);
            }
            if ((kafkaOptions.ClientMode & ClientMode.Consumer) == ClientMode.Consumer)
            {
                //消费者连接订阅时再创建
            }
        }

        #endregion

        public async Task PublishAsync(Type messageType, object message, TimeSpan? delay = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (delay != null && delay.Value.TotalMilliseconds > 0) throw new Exception("不支持延时任务");

            var data = new MessageBusData { Type = GetHandlerKey(messageType), Data = _kafkaOptions.Serializer.Serialize(message) };
            await _producer.ProduceAsync(GetTopic(messageType), new Message<Null, MessageBusData> { Value = data });
        }

        public async Task SubscribeAsync<T>(Func<T, CancellationToken, Task> handler, CancellationToken cancellationToken = default(CancellationToken)) where T : class
        {
            string handlerKey = GetHandlerKey(typeof(T)); //handler缓存key
            var subscriber = new SubscriberInfo
            {
                Type = typeof(T),
                CancellationToken = cancellationToken,
                Action = (message, token) =>
                {
                    var realObj = _kafkaOptions.Serializer.Deserialize<T>(message);
                    return handler(realObj, cancellationToken);
                }
            };

            lock (typeof(T))
            {
                if (_subscriberDict.ContainsKey(handlerKey))
                {
                    _subscriberDict[handlerKey].Add(subscriber);
                }
                else
                {
                    _subscriberDict.TryAdd(handlerKey, new List<SubscriberInfo> { subscriber });

                }
            }

            await SubscribeKafka(typeof(T), cancellationToken);
        }


        public void Dispose()
        {
            _logger.LogInformation("KafkaMessageBus 释放...");
            if (_producer != null)
            {
                With.NoException(_logger, () => { _producer.Dispose(); }, "关闭生产者");
            }

            foreach (var item in _consumerList)
            {
                item.Close();
            }
        }

        #region private 

        private Task SubscribeKafka(Type type, CancellationToken cancellationToken)
        {
            var topic = GetTopic(type);
            if (_subscriberTopicSet.Contains(topic)) return Task.CompletedTask; //同一主题订阅一次即可
            lock (_subscriberTopicSet)
            {
                if (_subscriberTopicSet.Contains(topic)) return Task.CompletedTask;

                _subscriberTopicSet.Add(topic);
            }

            Task.Run(async () =>
            {
                for (int i = 0; i < _kafkaOptions.ConsumerThreadCount; i++)
                {
                    var consumer = new KafkaConsumer<Null, MessageBusData>(_serviceProvider);
                    consumer.OnMessage += Handler;
                    _consumerList.Add(consumer);
                    await consumer.Subscribe(topic, cancellationToken);
                }
            });

            return Task.CompletedTask;
        }


        private async Task Handler(ConsumeResult<Null, MessageBusData> consumeResult)
        {
            string handlerKey = consumeResult.Value.Type;
            var hasHandler = _subscriberDict.TryGetValue(handlerKey, out List<SubscriberInfo> list);
            if (!hasHandler || list == null) return;
            foreach (var item in list)
            {
                if (item.CancellationToken != null && item.CancellationToken.IsCancellationRequested)
                    continue;

                if (item.CancellationToken != null && item.CancellationToken.IsCancellationRequested) return;
                await With.NoException(_logger, async () =>
                 {
                     await item.Action(consumeResult.Value.Data, item.CancellationToken);
                 }, $"消费数据{consumeResult.Value.Type}");
            }
        }

        private string GetHandlerKey(Type type)
        {
            return type.FullName;
        }

        private string GetTopic(Type type)
        {
            if (this._kafkaOptions.TopicMode == TopicMode.Single)
            {
                return GetTopic(this._kafkaOptions.Topic);
            }
            return GetTopic(type.Name);
        }
        private string GetTopic(string name)
        {
            return $"{_kafkaOptions.TopicPrefix ?? string.Empty}{name}";
        }

        #endregion
    }


}
