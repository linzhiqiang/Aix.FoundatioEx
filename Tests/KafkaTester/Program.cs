using Microsoft.Extensions.Hosting;
using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Foundatio.Messaging;
using Confluent.Kafka;
using Aix.FoundatioEx.Kafka;
using CommandLine;
using Microsoft.Extensions.Configuration;

namespace KafkaTester
{
    /*
   dotnet run -m 1 -q 100  //生产者测试
   dotnet run -m 2  //消费者测试
   dotnet run -m 3 -q 100 //生产者消费者一起测试
    */

    /// <summary>
    /// 组件 commandlineparser
    /// </summary>
    public class CmdOptions
    {
        [Option('m', "mode", Required = false, Default =1, HelpText = "1=生产者测试，2=消费者测试,3=同时测试")]
        public ClientMode Mode { get; set; }

        [Option('q', "quantity", Required = false, Default =1, HelpText = "测试生产数量")]
        public int Count { get; set; }
    }
    class Program
    {
        static void Main(string[] args)
        {
            Parser parser = new Parser((setting) =>
            {
                setting.CaseSensitive = false;
            });
            parser.ParseArguments<CmdOptions>(args).WithParsed(Run);
        }
        static void Run(CmdOptions options)
        {
            var host = new HostBuilder()
                .ConfigureAppConfiguration((hostContext, config) =>
                 {
                     config.AddJsonFile("appsettings.json", optional: true);
                 })
                .ConfigureLogging((context, factory) =>
                {
                    factory.AddConsole();
                })
                .ConfigureServices((context, services) =>
                {
                    services.AddSingleton<CmdOptions>(options);

                   var kafkaMessageBusOptions=  context.Configuration.GetSection("kafka").Get<KafkaMessageBusOptions>();
                    kafkaMessageBusOptions.ClientMode = options.Mode;//这里方便测试，以命令行参数为准
                    services.AddKafkaMessageBus(kafkaMessageBusOptions);

                   // AddKafkaMessageBus(services);
                    if ((options.Mode & ClientMode.Consumer) > 0)
                    {
                        services.AddHostedService<MessageBusConsumeService>();
                    }
                    if ((options.Mode & ClientMode.Producer) > 0)
                    {
                        services.AddHostedService<MessageBusProduerService>();
                    }
                });
            host.RunConsoleAsync().Wait();
            Console.WriteLine("服务已退出");
        }

        private static void AddKafkaMessageBus(IServiceCollection services)
        {
            var bootstrapServers = "192.168.111.132:9092,192.168.111.132:9093,192.168.111.132:9094";// com 虚拟机
                                                                                                    // bootstrapServers = "192.168.72.130:9092,192.168.72.130:9093,192.168.72.130:9094";//home 虚拟机
            var options = new KafkaMessageBusOptions
            {
                ClientMode = ClientMode.Both,
                TopicPrefix = "KafkaDemo", //项目名称
                //TopicMode = TopicMode.Single,
                ConsumerThreadCount = 4,
                ManualCommitBatch = 10,
                ProducerConfig = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers,
                    Acks = Acks.Leader,
                    Partitioner = Partitioner.ConsistentRandom
                },
                ConsumerConfig = new ConsumerConfig
                {
                    GroupId = "demo-messagebus",
                    BootstrapServers = bootstrapServers,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = false, //手动提交，避免数据丢失。但是数据重复问题避免不了，需要业务上做处理
                    AutoCommitIntervalMs = 5000, //自动提交偏移量间隔 ，每5秒同步一次,当再均衡时，如果有消费者一直没有poll，会等到所有的消费者都poll之后才再均衡处理
                    CancellationDelayMaxMs = 1000 //poll等待时间，如果自己 consumer.Consume(TimeSpan.FromSeconds(1));写的时间就不用这个配置了

                }
            };



            services.AddKafkaMessageBus(options);
        }
    }
}
