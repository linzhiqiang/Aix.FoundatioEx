using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaTester
{
    public class KafkaMessage
    {
        public string MessageId { get; set; }
        public string Content { get; set; }

        public DateTime CreateTime { get; set; }
    }

    /// <summary>
    /// 表示不同的消息
    /// </summary>
    public class KafkaMessage2
    {
        public string MessageId { get; set; }
        public string Content { get; set; }

        public DateTime CreateTime { get; set; }
    }
}
