using Mycenae.Models;
using NLog;
using System;
using Logger = NLog.Logger;

namespace Mycenae
{
    public class Program
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        public static void Main()
        {
            try
            {
                var migrationTask = GetMigrationTaskInstance();
                migrationTask.Execute();
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
            }
            finally
            {
                Logger.Info("Ending application...");
            }
        }

        private static MigrationTask GetMigrationTaskInstance()
        {
            _ = Enum.TryParse(Config.TaskToPerform.Value, true, out TaskToPerform task);

            return task switch
            {
                TaskToPerform.Extraction => new Extraction(),
                TaskToPerform.Insertion => new Insertion(),
                TaskToPerform.EndToEnd => new EndToEnd(),
                _ => throw new ArgumentException($"Config {Config.TaskToPerform.Path} is either unspecified or misspecified.")
            };
        }
    }
}