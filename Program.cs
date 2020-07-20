using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Mycenae.Models;
using Mycenae.Tasks;
using NLog;
using NLog.Extensions.Logging;
using System;
using Logger = NLog.Logger;

namespace Mycenae
{
    public class Program
    {
        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private static IServiceProvider _serviceProvider;

        public static void Main()
        {
            try
            {
                Setup();

                var migrationTask = GetMigrationTaskInstance();
                migrationTask.Execute();
            }
            catch (Exception ex)
            {
                _logger.Error(ex);
            }
            finally
            {
                _logger.Info("Ending application...");
            }
        }

        private static void Setup()
        {
            _serviceProvider = new ServiceCollection()
                .AddTransient<Extraction>()
                .AddTransient<Insertion>()
                .AddTransient<EndToEnd>()
                .AddLogging(builder =>
                {
                    builder.ClearProviders();
                    builder.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Debug);
                    builder.AddNLog("NLog.config");
                })
                .BuildServiceProvider();
        }

        private static MigrationTask GetMigrationTaskInstance()
        {
            return Settings.Values.TaskToExecute switch
            {
                TaskToExecute.Extraction => _serviceProvider.GetRequiredService<Extraction>(),
                TaskToExecute.Insertion => _serviceProvider.GetRequiredService<Insertion>(),
                TaskToExecute.EndToEnd => _serviceProvider.GetRequiredService<EndToEnd>(),
                _ => throw new ArgumentException($"Config {nameof(Settings.Values.TaskToExecute)} is not properly specified.")
            };
        }
    }
}