using System;

namespace Kalix.Leo.Storage;

/// <summary>
/// Exception that occurs when trying to write to storage when locked
/// </summary>
public class LockException : Exception
{
    public LockException(string message) : base(message)
    {
    }
}