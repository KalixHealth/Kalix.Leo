using System;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    /// <summary>
    /// Handy extensions for observables
    /// </summary>
    public static class ObservableExtension
    {
        /// <summary>
        /// Use this method to easily tell the sync context when awaiting the observable 
        /// </summary>
        /// <param name="obs">Observable to await</param>
        /// <returns>The equivalent task of just calling await</returns>
        public static async Task<T> ToTask<T>(this IObservable<T> obs)
        {
#pragma warning disable ConfigureAwaitChecker // CAC001
            return await obs;
#pragma warning restore ConfigureAwaitChecker // CAC001
        }
    }
}
