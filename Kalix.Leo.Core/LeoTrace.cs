using System;

namespace Kalix.Leo;

/// <summary>
/// Easy class to add tracing throughout the leo engine
/// </summary>
public static class LeoTrace
{
    private static Action<string> _writeLineAction = s => { };

    /// <summary>
    /// Will write to the trace action if it exists, otherwise nothing
    /// </summary>
    public static Action<string> WriteLine
    {
        get { return _writeLineAction; }
        set { _writeLineAction = value ?? (s => { }); }
    }
}