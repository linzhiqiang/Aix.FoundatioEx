using Aix.FoundatioEx.Kafka.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaTester
{
    /// <summary>
    /// 具体业务消息
    /// </summary>
    [TopicAttribute(Name = "relic-queue-task")]
    public class KafkaMessage
    {
        [RouteKeyAttribute]
        public string MessageId { get; set; }
        public string Content { get; set; }

        public DateTime CreateTime { get; set; }
    }

    /// <summary>
    /// 表示不同的消息
    /// </summary>
    [TopicAttribute(Name = "KafkaMessage2")]
    public class KafkaMessage2
    {
        public string MessageId { get; set; }
        public string Content { get; set; }

        public DateTime CreateTime { get; set; }
    }
}
