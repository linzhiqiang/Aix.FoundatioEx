using Confluent.Kafka;
using Foundatio.Serializer;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.FoundatioEx.Kafka
{
    [Flags]
    public enum ClientMode
    {
        Producer = 1,
        Consumer = 2,
        Both = 3
    }

    public enum TopicMode
    {
        /// <summary>
        /// 多个topic 每种类型对应一个topic ，默认为多topic
        /// </summary>
        multiple = 0,

        /// <summary>
        /// 所有类型对应一个topic
        /// </summary>
        Single = 1
    }

    public class KafkaMessageBusOptions
    {
        public KafkaMessageBusOptions()
        {
            this.TopicPrefix = "kafka";
            this.ClientMode = ClientMode.Producer;
            this.Serializer = new DefaultKafkaSerializer();
            this.ConsumerThreadCount = 4;
            this.TopicMode = TopicMode.Single;
            this.ManualCommitBatch = 10;
        }

        public ProducerConfig ProducerConfig { get; set; }

        public ConsumerConfig ConsumerConfig { get; set; }

        public string BootstrapServers { get; set; }

        /// <summary>
        /// 多topic时的前缀，为了防止重复，建议用项目名称
        /// </summary>
        public string TopicPrefix { get; set; }

        /// <summary>
        /// 针对单topic模式可以指定topic，覆盖系统的TopicPrefix参数也无效
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        /// 客户端模式，是生产者还是消费者
        /// </summary>
        public ClientMode ClientMode { get; set; }

        /// <summary>
        /// 自定义序列化，默认为MessagePack
        /// </summary>
        public IKafkaSerializer Serializer { get; set; }

        /// <summary>
        /// 每个类型的消费线程数 默认4个
        /// </summary>
        public int ConsumerThreadCount { get; set; }

        /// <summary>
        /// 不同类型消息是单个topic 还是多topic,默认值是Single
        /// </summary>
        public TopicMode TopicMode { get; set; }

        /// <summary>
        /// EnableAutoCommit=false时每多少个消息提交一次 默认10条消息提交一次
        /// </summary>
        public int ManualCommitBatch { get; set; }
    }
}
