using Foundatio.Messaging;
using Microsoft.Extensions.DependencyInjection;

namespace Aix.FoundatioEx.Kafka
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKafkaMessageBus(this IServiceCollection services, KafkaMessageBusOptions options)
        {

            services
               .AddSingleton<KafkaMessageBusOptions>(options)
               .AddSingleton<IMessageBus, KafkaMessageBus>();

            return services;
        }
    }
}
