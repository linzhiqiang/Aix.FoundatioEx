using Aix.KafkaMessageBus;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;
using Foundatio.Messaging;

namespace Aix.FoundatioEx.Kafka
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddFoundatioKafkaMessageBus(this IServiceCollection services, KafkaMessageBusOptions options)
        {
            services.AddSingleton<KafkaMessageBusOptions>(options);
            Aix.KafkaMessageBus.ServiceCollectionExtensions.AddKafkaMessageBus(services, options);
            services.AddSingleton<Foundatio.Messaging.IMessageBus, KafkaMessageBusAdapter>();
            return services;
        }
    }
}
