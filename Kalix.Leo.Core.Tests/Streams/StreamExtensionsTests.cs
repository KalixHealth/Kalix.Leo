using NUnit.Framework;
using System;
using System.IO;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Kalix.Leo.Core.Tests.Streams
{
    [TestFixture]
    public class StreamExtensionsTests
    {
        [Test]
        [ExpectedException]
        public void ErrorInObservablePassesErrorUp()
        {
            var subject = new Subject<byte[]>();
            var stream = new MemoryStream();
            var task = StreamExtensions.WriteToStream(subject, stream);

            subject.OnError(new Exception("Something went wrong"));

            task.Wait();
        }
    }
}
