using Aix.FoundatioEx.Kafka;
using KafkaTester.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaTester
{
    public class Startup
    {
        internal static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
        {
            var options = CmdOptions.Options;
            services.AddSingleton(options);
            var kafkaMessageBusOptions = context.Configuration.GetSection("kafka").Get<KafkaMessageBusOptions>();
            services.AddKafkaMessageBus(kafkaMessageBusOptions);

            if ((options.Mode & ClientMode.Consumer) > 0)
            {
                services.AddHostedService<MessageBusConsumeService>();
            }
            if ((options.Mode & ClientMode.Producer) > 0)
            {
                services.AddHostedService<MessageBusProduerService>();
            }
        }
    }
}
