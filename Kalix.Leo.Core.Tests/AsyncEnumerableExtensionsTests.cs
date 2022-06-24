using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Core.Tests;

[TestFixture]
public class AsyncEnumerableExtensionsTests
{
    [Test]
    public async Task CombineWithFirstPriorityShouldResolveFirst()
    {
        var set1 = new[] { 1, 2, 3 }.ToAsyncEnumerable();
        var set2 = new[] { 4, 5, 6 }.ToAsyncEnumerable();

        var combined = await new[] { set1, set2 }
            .Combine(AsyncEnumberableCombineType.FirstHasPriority, 1)
            .ToListAsync();

        Assert.That(combined, Is.EquivalentTo(new[] { 1, 2, 3, 4, 5, 6 }));
    }

    [Test]
    public async Task CombineWithEvenPriorityShouldResolveSplit()
    {
        var set1 = new[] { 1, 2, 3 }.ToAsyncEnumerable();
        var set2 = new[] { 4, 5, 6 }.ToAsyncEnumerable();

        var combined = await new[] { set1, set2 }
            .Combine(AsyncEnumberableCombineType.Even, 1)
            .ToListAsync();

        Assert.That(combined, Is.EquivalentTo(new[] { 1, 4, 2, 5, 3, 6 }));
    }
}