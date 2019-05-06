using Foundatio.Messaging;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaTester
{
    public class MessageBusProduerService : IHostedService
    {
        private ILogger<MessageBusProduerService> _logger;
        public IMessageBus _messageBus;
        CmdOptions _cmdOptions;
        public MessageBusProduerService(ILogger<MessageBusProduerService> logger, IMessageBus messageBus, CmdOptions cmdOptions)
        {
            _logger = logger;
            _messageBus = messageBus;
            _cmdOptions = cmdOptions;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Task.Run(() =>
            {
                return Test(cancellationToken);
            });

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        private async Task Test(CancellationToken cancellationToken)
        {
            int producerCount = _cmdOptions.Count > 0 ? _cmdOptions.Count : 1;
            var duration = Stopwatch.StartNew();
            for (int i = 0; i < producerCount; i++)
            {
                if (cancellationToken.IsCancellationRequested) break;

                var messageData = new KafkaMessage { MessageId = i.ToString(), Content = $"我是内容_{i}", CreateTime = DateTime.Now };
                _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}生产数据：MessageId={messageData.MessageId}");
                await _messageBus.PublishAsync(messageData);
            }
            duration.Stop();

           var totalSecond= duration.ElapsedMilliseconds /1000;//执行任务的时间

            _logger.LogInformation($"生产效率={producerCount*1.0/ totalSecond}");
        }
    }
}
