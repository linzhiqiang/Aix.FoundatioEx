using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Aix.FoundatioEx.Kafka
{
    public interface ISerializer
    {
        byte[] Serialize<T>(T data);

        T Deserialize<T>(byte[] bytes);
    }

    /// <summary>
    /// 默认采用MessagePackSerializer
    /// </summary>
    internal class MessagePackSerializer : ISerializer
    {
        private Foundatio.Serializer.ISerializer _serializer = new Foundatio.Serializer.MessagePackSerializer();
        private static byte[] EmptyBytes = new byte[0];
        public T Deserialize<T>(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0) return default(T);

            return (T)_serializer.Deserialize(new MemoryStream(bytes), typeof(T));
        }

        public byte[] Serialize<T>(T data)
        {
            if (data == null) return EmptyBytes;

            var stream = new MemoryStream();
            _serializer.Serialize(data, stream);
            return stream.ToArray();
        }
    }
}
