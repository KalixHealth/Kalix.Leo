using System;

namespace Kalix.Leo
{
    public static class LeoTrace
    {
        public static Action<string> TraceAction { get; set; }

        public static void WriteLine(string trace)
        {
            if(TraceAction != null)
            {
                TraceAction(trace);
            }
        }
    }
}
