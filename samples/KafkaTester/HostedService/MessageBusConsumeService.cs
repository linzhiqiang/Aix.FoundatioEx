using Foundatio.Messaging;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
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

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            List<Task> taskList = new List<Task>(); //多个订阅者
            taskList.Add(Consume(cancellationToken));
            await Task.WhenAll(taskList.ToArray());

        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        private async Task Consume(CancellationToken cancellationToken)
        {
            //SubscribeOptions subscribeOptions = new SubscribeOptions();
            //subscribeOptions.GroupId = "group1";
            //subscribeOptions.ConsumerThreadCount = 4;
           
            await _messageBus.SubscribeAsync<KafkaMessage>(async (message) =>
            {
                var current = Interlocked.Increment(ref Count);
                //if (current % 10000 == 0)
                _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}消费数据：MessageId={message.MessageId},Content={message.Content},count={current}");
                //await Task.Delay(100);
                await Task.CompletedTask;
            });


            //await _messageBus.SubscribeAsync<KafkaMessage2>(async (message) =>
            //{
            //    var current = Interlocked.Increment(ref Count);
            //    //if (current % 10000 == 0)
            //    _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}消费数据：MessageId={message.MessageId},Content={message.Content},count={current}");
            //    //await Task.Delay(100);
            //    await Task.CompletedTask;
            //}, cancellationToken);
        }
    }
}
