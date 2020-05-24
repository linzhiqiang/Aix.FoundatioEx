using Aix.FoundatioEx.Kafka;
using Foundatio.Messaging;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaTester
{
    public class MessageBusConsumeService : IHostedService
    {
        private ILogger<MessageBusConsumeService> _logger;
        public IMessageBus _messageBus;

        private int Count = 0;
        public MessageBusConsumeService(ILogger<MessageBusConsumeService> logger, IMessageBus messageBus)
        {
            _logger = logger;
            _messageBus = messageBus;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Task.Run(async () =>
            {
                List<Task> taskList = new List<Task>(); //多个订阅者
                taskList.Add(Consume(cancellationToken));
                await Task.WhenAll(taskList.ToArray());
            });

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        private async Task Consume(CancellationToken cancellationToken)
        {
            MessageBusContext context = new MessageBusContext();
            context.Config.Add(MessageBusContextConstant.GroupId, "group1"); //kafka消费者组(只有kafka使用)
            context.Config.Add(MessageBusContextConstant.ConsumerThreadCount, "4");//该订阅的消费线程数，若是kafka注意和分区数匹配
            await _messageBus.SubscribeAsync<KafkaMessage>(async (message) =>
            {
                var current = Interlocked.Increment(ref Count);
                //if (current % 10000 == 0)
                _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}消费数据：MessageId={message.MessageId},Content={message.Content},count={current}");
                //await Task.Delay(100);
                await Task.CompletedTask;
            }, context);


            await _messageBus.SubscribeAsync<KafkaMessage2>(async (message) =>
            {
                var current = Interlocked.Increment(ref Count);
                //if (current % 10000 == 0)
                _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}消费数据：MessageId={message.MessageId},Content={message.Content},count={current}");
                //await Task.Delay(100);
                await Task.CompletedTask;
            }, cancellationToken);
        }
    }
}
