using Microsoft.Extensions.Hosting;
using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Foundatio.Messaging;
using Confluent.Kafka;
using Aix.FoundatioEx.Kafka;
using CommandLine;

namespace KafkaTester
{
    /*
   dotnet run -m p -q 100  //生产者测试
   dotnet run -m c  //消费者测试
   dotnet run -m a -q 100 //生产者消费者一起测试
    */

    /// <summary>
    /// 组件 commandlineparser
    /// </summary>
    public class CmdOptions
    {
        [Option('m', "mode", Required = false, HelpText = "p=生产者测试，c=消费者测试,a=同时测试")]
        public string Mode { get; set; }

        [Option('q', "quantity", Required = false, HelpText = "测试生产数量")]
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
            //Parser.Default.ParseArguments<CmdOptions>(args).WithParsed(Run);
            parser.ParseArguments<CmdOptions>(args).WithParsed(Run);
        }
        static void Run(CmdOptions options)
        {
            var host = new HostBuilder()
                .ConfigureAppConfiguration((hostContext, config) =>
                 {
                 })
                .ConfigureLogging((context, factory) =>
                {
                    factory.AddConsole();
                })
                .ConfigureServices((context, services) =>
                {
                    services.AddSingleton<CmdOptions>(options);
                    AddKafkaMessageBus(services);
                    if (string.IsNullOrEmpty(options.Mode) || options.Mode == "c" || options.Mode == "a")
                    {
                        services.AddHostedService<MessageBusConsumeService>();
                    }
                    if (string.IsNullOrEmpty(options.Mode) || options.Mode == "p" || options.Mode == "a")
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
                KafkaMessageBusMode = KafkaMessageBusMode.Both,
                TopicPrefix = "KafkaDemo", //项目名称
                //TopicMode = TopicMode.Single,
                ConsumerThreadCount = 4,
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
