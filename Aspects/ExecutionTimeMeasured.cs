using AspectInjector.Broker;
using NLog;
using System;
using System.Diagnostics;

namespace Mycenae.Aspects
{
    [Aspect(Scope.PerInstance)]
    [Injection(typeof(ExecutionTimeMeasured))]
    public class ExecutionTimeMeasured : Attribute
    {
        private readonly ILogger _logger = LogManager.GetCurrentClassLogger();
        private Stopwatch _stopwatch;

        [Advice(Kind.Before)]
        public void StartMeasurement()
        {
            _stopwatch = Stopwatch.StartNew();
        }

        [Advice(Kind.After)]
        public void FinishMeasurement([Argument(Source.Name)] string name)
        {
            _stopwatch.Stop();

            _logger.Info($"Time elapsed ({name}): {_stopwatch.Elapsed.Hours}h:{_stopwatch.Elapsed.Minutes}m:" +
                $"{_stopwatch.Elapsed.Seconds}s:{_stopwatch.Elapsed.Milliseconds}ms");
        }
    }
}
