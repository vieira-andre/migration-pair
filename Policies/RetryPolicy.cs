using Cassandra;
using System.Threading.Tasks;

namespace migration_pair.Policies
{
    internal class RetryPolicy : IRetryPolicy
    {
        public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
        {
            throw new System.NotImplementedException();
        }

        public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            throw new System.NotImplementedException();
        }

        public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            Task.Delay(300).Wait();

            return RetryDecision.Retry(cl);
        }
    }
}