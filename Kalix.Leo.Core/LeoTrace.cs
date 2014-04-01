using System;

namespace Kalix.Leo
{
    /// <summary>
    /// Easy class to add tracing throughout the leo engine
    /// </summary>
    public static class LeoTrace
    {
        /// <summary>
        /// This action should be set if you want the trace to do anything
        /// </summary>
        public static Action<string> TraceAction { get; set; }

        /// <summary>
        /// Will write to the trace action if it exists, otherwise nothing
        /// </summary>
        /// <param name="trace">The message to write</param>
        public static void WriteLine(string trace)
        {
            if(TraceAction != null)
            {
                TraceAction(trace);
            }
        }
    }
}
