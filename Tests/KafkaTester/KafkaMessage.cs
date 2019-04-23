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
}
