using Confluent.Kafka;
using System;

namespace Aix.FoundatioEx.Kafka
{
    /// <summary>
    /// kafka序列化适配  转化为Confluent.Kafka的api要求
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class ConfluentKafkaSerializerAdapter<T> : Confluent.Kafka.ISerializer<T>, Confluent.Kafka.IDeserializer<T>
    {
        private IKafkaSerializer _kafkaSerializer;
        public ConfluentKafkaSerializerAdapter(IKafkaSerializer kafkaSerializer)
        {
            _kafkaSerializer = kafkaSerializer;
        }
        public byte[] Serialize(T data, SerializationContext context)
        {
            return _kafkaSerializer.Serialize<T>(data);
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (!isNull)
            {
                return _kafkaSerializer.Deserialize<T>(data.ToArray());
            }
            return default(T);
        }

    }


}
