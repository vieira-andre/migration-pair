using migration_pair.Models;
using NLog;
using System;
using Logger = NLog.Logger;

namespace migration_pair
{
    public class Program
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        public static void Main()
        {
            try
            {
                if (!Enum.TryParse(Config.TaskToPerform.Value, true, out TaskToPerform task))
                    Logger.Error($"Config entry {Config.TaskToPerform.Path} is either unspecified or misspecified.");

                var migrationTask = GetMigrationTaskInstance(task);
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

        private static MigrationTask GetMigrationTaskInstance(TaskToPerform task)
        {
            return task switch
            {
                TaskToPerform.Extraction => new Extraction(),
                TaskToPerform.Insertion => new Insertion(),
                TaskToPerform.EndToEnd => new EndToEnd(),
                _ => throw new ArgumentException("Invalid value for migration task.")
            };
        }
    }
}