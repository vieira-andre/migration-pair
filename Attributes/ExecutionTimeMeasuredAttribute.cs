using MethodDecorator.Fody.Interfaces;
using NLog;
using System;
using System.Diagnostics;
using System.Reflection;

namespace Mycenae.Attributes
{
    [AttributeUsage(AttributeTargets.Method)]
    public class ExecutionTimeMeasuredAttribute : Attribute, IMethodDecorator
    {
        private readonly ILogger _logger = LogManager.GetCurrentClassLogger();
        private Stopwatch _stopwatch;

        public void Init(object instance, MethodBase method, object[] args)
        {
        }

        public void OnEntry()
        {
            _stopwatch = Stopwatch.StartNew();
        }

        public void OnException(Exception exception)
        {
        }

        public void OnExit()
        {
            _stopwatch.Stop();

            _logger.Info($"Time elapsed: {_stopwatch.Elapsed.Hours}h:{_stopwatch.Elapsed.Minutes}m:{_stopwatch.Elapsed.Seconds}s:{_stopwatch.Elapsed.Milliseconds}ms");
        }
    }
}
