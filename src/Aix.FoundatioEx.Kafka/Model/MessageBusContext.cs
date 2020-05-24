using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.FoundatioEx.Kafka
{
    public class MessageBusContext
    {
        public static MessageBusContext Default = new MessageBusContext();

        private IDictionary<string, string> _config;

        /// <summary>
        /// 具体实现需要的个性配置 如kafka实现，redis实现，rabbitmq实现
        /// </summary>
        public IDictionary<string, string> Config
        {
            get
            {
                if (_config == null) _config = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
                return _config;
            }
        }
    }
}
