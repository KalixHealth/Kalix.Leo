using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene
{
    internal static class TaskExtension
    {
        /// <summary>
        /// This is a dangerous way to wait, as it may cause deadlock, not a problem since our library uses .ConfigureAwait(false)
        /// </summary>
        public static void WaitAndWrap(this Task task)
        {
            if (!task.IsCompleted)
            {
                try
                {
                    task.Wait();
                }
                catch (AggregateException e)
                {
                    throw e.InnerException;
                }
            }
        }

        /// <summary>
        /// This is a dangerous way to wait, as it may cause deadlock, not a problem since our library uses .ConfigureAwait(false)
        /// </summary>
        public static T ResultAndWrap<T>(this Task<T> task)
        {
            try
            {
                return task.Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
    }
}
