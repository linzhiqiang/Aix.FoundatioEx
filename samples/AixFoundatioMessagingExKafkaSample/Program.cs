using CommandLine;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;

namespace AixFoundatioMessagingExKafkaSample
{
    /*
   dotnet run -m 1 -q 100  //生产者测试
   dotnet run -m 2  //消费者测试
   dotnet run -m 3 -q 100 //生产者消费者一起测试
    */
    class Program
    {
        public static void Main(string[] args)
        {
            Parser parser = new Parser((setting) =>
            {
                setting.CaseSensitive = false;
            });
            parser.ParseArguments<CmdOptions>(args).WithParsed((options) =>
            {
                CmdOptions.Options = options;
                CreateHostBuilder(args, options).Build().Run();
                Console.WriteLine("服务已退出");
            });

        }

        public static IHostBuilder CreateHostBuilder(string[] args, CmdOptions options)
        {
            return Host.CreateDefaultBuilder(args)
            .ConfigureHostConfiguration(configurationBuilder =>
            {

            })
           .ConfigureAppConfiguration((hostBulderContext, configurationBuilder) =>
           {
           })
            .ConfigureLogging((hostBulderContext, loggingBuilder) =>
            {
                loggingBuilder.SetMinimumLevel(LogLevel.Information);
                //系统也默认加载了默认的log
                loggingBuilder.ClearProviders();
                loggingBuilder.AddConsole();
            })
            .ConfigureServices(Startup.ConfigureServices);

        }
    }

}
