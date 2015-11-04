using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Kalix.Leo.Core.Tests.AsyncEnumerable
{
    [TestFixture]
    public class AsyncEnumerableExTests
    {
        [Test]
        public void TimerFunctionWaits()
        {
            var stopwatch = Stopwatch.StartNew();
            var time = TimeSpan.FromSeconds(0.3);
            var enumerable = AsyncEnumerableEx.CreateTimer(time).GetEnumerator();
            enumerable.MoveNext().Wait();
            enumerable.MoveNext().Wait();
            enumerable.MoveNext().Wait();
            stopwatch.Stop();

            Assert.GreaterOrEqual(stopwatch.Elapsed, time + time + time);
        }

        [Test]
        public void CanTimerWaitSuccessfullyCancels()
        {
            bool finished = false;
            var time = TimeSpan.FromSeconds(0.3);
            var enumerable = AsyncEnumerableEx.CreateTimer(time).TakeUntilDisposed(TimeSpan.FromSeconds(0.8), t => finished = true);

            Thread.Sleep(1000);

            Assert.AreEqual(true, finished);
            Assert.AreEqual(true, enumerable.RunningTask.IsCompleted);
            Assert.AreEqual(true, enumerable.RunningTask.IsCanceled);
        }
    }
}
