using Aix.FoundatioEx.Kafka.Utils;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace Aix.FoundatioEx.Kafka
{
    /// <summary>
    /// kafka消费者
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    internal class KafkaConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
    {
        private IServiceProvider _serviceProvider;
        private ILogger<KafkaConsumer<TKey, TValue>> _logger;
        private KafkaMessageBusOptions _kafkaOptions;


        IConsumer<TKey, TValue> _consumer = null;
        /// <summary>
        /// 存储每个分区的最大offset，针对手工提交 
        /// </summary>
        private ConcurrentDictionary<TopicPartition, TopicPartitionOffset> _offsetDict = new ConcurrentDictionary<TopicPartition, TopicPartitionOffset>();
        private volatile bool _isStart = false;
        private int Count = 0;

        public event Func<ConsumeResult<TKey, TValue>, Task> OnMessage;
        public KafkaConsumer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = serviceProvider.GetService<ILogger<KafkaConsumer<TKey, TValue>>>();
            _kafkaOptions = serviceProvider.GetService<KafkaMessageBusOptions>();
        }

        public Task Subscribe(string topic, CancellationToken cancellationToken)
        {
            Task.Run(async () =>
            {
                _isStart = true;
                this._consumer = this.CreateConsumer();
                this._consumer.Subscribe(topic);
                await StartPoll(cancellationToken);
            });

            return Task.CompletedTask;
        }

        public void Close()
        {
            this._isStart = false;
            _logger.LogInformation("Kafka关闭消费者");
            With.NoException(_logger, () =>
            {
                if (EnableAutoCommit() == false)
                {
                    this._consumer.Commit();
                }
            }, "关闭消费者时提交偏移量");

            With.NoException(_logger, () => { this._consumer?.Close(); }, "关闭消费者");
        }

        public void Dispose()
        {
            this.Close();
        }

        #region private

        private Task StartPoll(CancellationToken cancellationToken)
        {
            Task.Factory.StartNew(async () =>
            {
                _logger.LogInformation("开始消费数据...");
                try
                {

                    while (_isStart && !cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            await Consume();
                        }
                        catch (ConsumeException ex)
                        {
                            _logger.LogError($"消费拉取消息ConsumeException, {ex.Message}, {ex.StackTrace}");
                        }
                        catch (KafkaException ex)
                        {
                            _logger.LogError($"消费拉取消息KafkaException, {ex.Message}, {ex.StackTrace}");
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError($"消费拉取消息系统异常, {ex.Message}, {ex.StackTrace}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError($"消费异常退出消费循环, {ex.Message}, {ex.StackTrace}");
                }
                finally
                {
                    _logger.LogInformation("退出消费循环 关闭消费者...");
                    this.Close();
                }
            });

            return Task.CompletedTask;
        }

        private async Task Consume()
        {
            var result = this._consumer.Consume(TimeSpan.FromSeconds(1));
            if (result == null || result.IsPartitionEOF || result.Value == null)
            {
                return;
            }
            Count++;
            //消费数据
            await Handler(result);

            //处理手动提交
            ManualCommitOffset(result); //采用后提交（至少一次）,消费前提交（至多一次）
        }


        /// <summary>
        /// 手工提交offset
        /// </summary>
        /// <param name="result"></param>
        private void ManualCommitOffset(ConsumeResult<TKey, TValue> result)
        {
            //处理手动提交
            if (EnableAutoCommit() == false)
            {
                var topicPartition = result.TopicPartition;
                var topicPartitionOffset = new TopicPartitionOffset(topicPartition, result.Offset + 1);
                AddToOffsetDict(topicPartition, topicPartitionOffset); //加入offset缓存 

                if (Count % _kafkaOptions.ManualCommitBatch == 0)
                {
                    _offsetDict.TryGetValue(topicPartition, out TopicPartitionOffset maxOffset); //取出最大的offset提交，可能并发当前的不是最大的
                    With.NoException(_logger, () =>
                    {
                        this._consumer.Commit(new[] { maxOffset });
                    }, "手动提交偏移量");
                }
            }
        }

        private async Task Handler(ConsumeResult<TKey, TValue> consumeResult)
        {
            if (OnMessage != null)
            {
                await With.NoException(_logger, async () =>
                {
                    await OnMessage(consumeResult);
                }, "kafka消费失败");
            }
            else
            {
                _logger.LogWarning("kafka没有注册消费事件");
            }
        }
        private void AddToOffsetDict(TopicPartition topicPartition, TopicPartitionOffset TopicPartitionOffset)
        {
            _offsetDict.AddOrUpdate(topicPartition, TopicPartitionOffset, (key, oldValue) =>
            {
                return TopicPartitionOffset.Offset > oldValue.Offset ? TopicPartitionOffset : oldValue;
            });
        }

        /// <summary>
        /// 创建消费者对象
        /// </summary>
        /// <returns></returns>
        private IConsumer<TKey, TValue> CreateConsumer()
        {
            if (_kafkaOptions.ConsumerConfig == null) _kafkaOptions.ConsumerConfig = new ConsumerConfig();

            if (string.IsNullOrEmpty(_kafkaOptions.ConsumerConfig.BootstrapServers))
            {
                _kafkaOptions.ConsumerConfig.BootstrapServers = _kafkaOptions.BootstrapServers;

            }
            if (string.IsNullOrEmpty(_kafkaOptions.ConsumerConfig.BootstrapServers))
            {
                throw new Exception("请配置BootstrapServers参数");
            }
            if (string.IsNullOrEmpty(_kafkaOptions.ConsumerConfig.GroupId))
            {
                _kafkaOptions.ConsumerConfig.GroupId = "kafka-messagebus-group";
            }

            var consumer = new ConsumerBuilder<TKey, TValue>(_kafkaOptions.ConsumerConfig)
                  .SetErrorHandler((producer, error) =>
                  {
                      if (error.IsFatal || error.IsBrokerError)
                      {
                          string errorInfo = $"Code:{error.Code}, Reason:{error.Reason}, IsFatal={error.IsFatal}, IsLocalError:{error.IsLocalError}, IsBrokerError:{error.IsBrokerError}";
                          _logger.LogError($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}Kafka消费者出错：{errorInfo}");
                      }
                  })
                  .SetPartitionsRevokedHandler((c, partitions) =>
                  {
                      //方法会在再均衡开始之前和消费者停止读取消息之后被调用。如果在这里提交偏移量，下一个接管partition的消费者就知道该从哪里开始读取了。
                      //Console.WriteLine($"Revoking assignment: [{string.Join(", ", partitions)}]");
                      if (EnableAutoCommit() == false)
                      {
                          //只提交当前消费者分配的分区
                          With.NoException(_logger, () =>
                          {
                              c.Commit(_offsetDict.Values.Where(x => partitions.Exists(current => current.Topic == x.Topic && current.Partition == x.Partition)));
                              _logger.LogInformation("Kafka再均衡提交");
                              _offsetDict.Clear();
                          }, "Kafka再均衡提交");

                      }
                  })
                  .SetPartitionsAssignedHandler((c, partitions) =>
                  {
                      if (EnableAutoCommit() == false)
                      {
                          _offsetDict.Clear();
                      }
                      _logger.LogInformation($"MemberId:{c.MemberId}分配的分区：Assigned partitions: [{string.Join(", ", partitions)}]");
                  })
                .SetValueDeserializer(new ConfluentKafkaSerializerAdapter<TValue>(_kafkaOptions.Serializer))
                .Build();

            return consumer;
        }

        /// <summary>
        /// 是否是自动提交
        /// </summary>
        /// <returns></returns>
        private bool EnableAutoCommit()
        {
            var enableAutoCommit = this._kafkaOptions.ConsumerConfig.EnableAutoCommit;
            return !enableAutoCommit.HasValue || enableAutoCommit.Value == true;
        }

        #endregion
    }
}
