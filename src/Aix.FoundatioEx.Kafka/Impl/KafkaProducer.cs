using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Confluent.Kafka;
using System.Threading.Tasks;
using Aix.FoundatioEx.Kafka.Utils;

namespace Aix.FoundatioEx.Kafka
{
    /// <summary>
    /// kafka生产者实现
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    internal class KafkaProducer<TKey, TValue> : IKafkaProducer<TKey, TValue>
    {
        private IServiceProvider _serviceProvider;
        private ILogger<KafkaProducer<TKey, TValue>> _logger;
        private KafkaMessageBusOptions _kafkaOptions;
        private static TimeSpan DefaultTimeout = TimeSpan.FromSeconds(5);

        IProducer<TKey, TValue> _producer = null;
        public KafkaProducer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = serviceProvider.GetService<ILogger<KafkaProducer<TKey, TValue>>>();
            _kafkaOptions = serviceProvider.GetService<KafkaMessageBusOptions>();

            this.CreateProducer();
        }
        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message)
        {
            return this._producer.ProduceAsync(topic, message);
        }

        /// <summary>
        /// 事务版本   貌似没必要 要求 配置TransactionalId，Acks必须等于all
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        public async Task<DeliveryResult<TKey, TValue>> ProduceAsync2(string topic, Message<TKey, TValue> message)
        {
            DeliveryResult<TKey, TValue> result = null;
            try
            {
                _producer.InitTransactions(DefaultTimeout);
                _producer.BeginTransaction();
                result = await this._producer.ProduceAsync(topic, message);
                _producer.CommitTransaction(DefaultTimeout);
            }
            catch (Exception)
            {
                _producer.AbortTransaction(DefaultTimeout);
                throw;
            }
            return result;
        }


        public void Dispose()
        {
            _logger.LogInformation("Kafka关闭生产者");
            if (this._producer != null)
            {
                With.NoException(_logger, () => { this._producer.Dispose(); }, "Kafka关闭生产者");
            }
        }

        #region private

        private void CreateProducer()
        {
            if (this._producer != null) return;

            lock (this)
            {
                if (this._producer != null) return;

                if (_kafkaOptions.ProducerConfig == null) _kafkaOptions.ProducerConfig = new ProducerConfig();
                if (string.IsNullOrEmpty(_kafkaOptions.ProducerConfig.BootstrapServers))
                {
                    _kafkaOptions.ProducerConfig.BootstrapServers = _kafkaOptions.BootstrapServers;
                }
                if (string.IsNullOrEmpty(_kafkaOptions.ProducerConfig.BootstrapServers))
                {
                    throw new Exception("kafka BootstrapServers参数");
                }
                var builder = new ProducerBuilder<TKey, TValue>(_kafkaOptions.ProducerConfig)
                .SetErrorHandler((p, error) =>
                {
                    if (error.IsFatal)
                    {
                        string errorInfo = $"{error.Code}-{error.Reason}, IsFatal={error.IsFatal}, IsLocalError:{error.IsLocalError}, IsBrokerError:{error.IsBrokerError}";
                        _logger.LogError($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}Kafka生产者出错：{errorInfo}");
                    }
                })
               .SetValueSerializer(new ConfluentKafkaSerializerAdapter<TValue>(_kafkaOptions.Serializer));

                //以下是内置的
                //if (typeof(TKey) == typeof(Null)) builder.SetKeySerializer((ISerializer<TKey>)Confluent.Kafka.Serializers.Null);
                //if (typeof(TKey) == typeof(string)) builder.SetKeySerializer((ISerializer<TKey>)Confluent.Kafka.Serializers.Utf8);
                //if (typeof(TKey) == typeof(int)) builder.SetKeySerializer((ISerializer<TKey>)Confluent.Kafka.Serializers.Int32);
                //if (typeof(TKey) == typeof(long)) builder.SetKeySerializer((ISerializer<TKey>)Confluent.Kafka.Serializers.Int64);
                //if (typeof(TKey) == typeof(float)) builder.SetKeySerializer((ISerializer<TKey>)Confluent.Kafka.Serializers.Single);
                //if (typeof(TKey) == typeof(double)) builder.SetKeySerializer((ISerializer<TKey>)Confluent.Kafka.Serializers.Double);
                //if (typeof(TKey) == typeof(byte[])) builder.SetKeySerializer((ISerializer<TKey>)Confluent.Kafka.Serializers.ByteArray);
                //if (typeof(TKey) == typeof(object)) builder.SetKeySerializer(new ConfluentKafkaSerializerAdapter<TKey>(_kafkaOptions.Serializer));

                this._producer = builder.Build();
            }
        }

        #endregion


    }
}
