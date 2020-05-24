using Aix.FoundatioEx.Kafka.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.FoundatioEx.Kafka
{
    internal static class Helper
    {
        public static string GetTopic(KafkaMessageBusOptions options, Type type)
        {
            string topicName = type.Name;

            var topicAttr = TopicAttribute.GetTopicAttribute(type);
            if (topicAttr != null && !string.IsNullOrEmpty(topicAttr.Name))
            {
                topicName = topicAttr.Name;
            }

            return $"{options.TopicPrefix ?? ""}{topicName}";
        }
    }
}
