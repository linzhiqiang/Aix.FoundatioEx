using Confluent.Kafka;
using Foundatio.Serializer;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.FoundatioEx.Kafka
{
    public class KafkaMessageBusOptions
    {
        public KafkaMessageBusOptions()
        {
            // this.TopicPrefix = "kafka-messagebus-";
            this.Serializer = new Aix.FoundatioEx.Kafka.MessagePackSerializer();
            this.DefaultConsumerThreadCount = 4;
            this.ManualCommitBatch = 100;
            this.ManualCommitIntervalSecond = 0;
        }

        public ProducerConfig ProducerConfig { get; set; }

        public ConsumerConfig ConsumerConfig { get; set; }

        public string BootstrapServers { get; set; }

        /// <summary>
        /// 多topic时的前缀，为了防止重复，建议用项目名称
        /// </summary>
        public string TopicPrefix { get; set; }

        /// <summary>
        /// 自定义序列化，默认为MessagePack
        /// </summary>
        public IKafkaSerializer Serializer { get; set; }

        /// <summary>
        /// 每个类型的消费线程数 默认4个
        /// </summary>
        public int DefaultConsumerThreadCount { get; set; }

        /// <summary>
        /// EnableAutoCommit=false时每多少个消息提交一次 默认100条消息提交一次，重要业务建议手工提交
        /// </summary>
        public int ManualCommitBatch { get; set; }

        /// <summary>
        /// EnableAutoCommit=false时 每多少秒提交一次 默认0秒不开启  ManualCommitBatch和ManualCommitIntervalSecond是或的关系
        /// </summary>
        public int ManualCommitIntervalSecond { get; set; }
    }
}
