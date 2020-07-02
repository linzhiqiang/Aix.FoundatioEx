
using Aix.FoundatioMessagingEx.Kafka;
using Aix.KafkaMessageBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace AixFoundatioMessagingExKafkaSample
{
    public class Startup
    {
        internal static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
        {
            var options = CmdOptions.Options;
            services.AddSingleton(options);
            var kafkaMessageBusOptions = context.Configuration.GetSection("kafka").Get<KafkaMessageBusOptions>();
            services.AddFoundatioKafkaMessageBus(kafkaMessageBusOptions);

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
