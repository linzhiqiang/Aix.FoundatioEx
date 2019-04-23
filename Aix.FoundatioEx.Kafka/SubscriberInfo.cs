using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.FoundatioEx.Kafka
{
    /// <summary>
    /// 订阅信息包装类
    /// </summary>
    internal class SubscriberInfo
    {
        //public string Id { get; private set; } = Guid.NewGuid().ToString();
        public CancellationToken CancellationToken { get; set; }

        public Type Type { get; set; }

        public Func<byte[], CancellationToken, Task> Action { get; set; }
    }
}
