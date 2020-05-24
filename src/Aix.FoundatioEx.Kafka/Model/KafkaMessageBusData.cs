using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.FoundatioEx.Kafka.Model
{
    public class KafkaMessageBusData
    {
        public string Topic { get; set; }
        public byte[] Data { get; set; }
    }
}
