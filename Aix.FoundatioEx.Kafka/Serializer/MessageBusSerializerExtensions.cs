using Foundatio.Serializer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Aix.FoundatioEx.Kafka
{
    public static class MessageBusSerializerExtensions
    {
        //public static T Deserialize<T>(this ISerializer serializer, ReadOnlySpan<byte> data, bool isNull)
        //{
        //    if (!isNull)
        //    {
        //        return (T)serializer.Deserialize(new MemoryStream(data.ToArray()), typeof(T));
        //    }

        //    return default(T);
        //}
    }
}
