using NLog;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace migration_pair.Helpers
{
    internal static class StopwatchManager
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        internal static Stopwatch Start()
        {
            return Stopwatch.StartNew();
        }

        internal static void StopAndLog(this Stopwatch stopwatch, [CallerMemberName]string methodMeasured = null)
        {
            stopwatch.Stop();
            Logger.Info($"Time elapsed ({methodMeasured}): {stopwatch.Elapsed.Hours}h:{stopwatch.Elapsed.Minutes}m:{stopwatch.Elapsed.Seconds}s:{stopwatch.Elapsed.Milliseconds}ms.");
        }
    }
}
