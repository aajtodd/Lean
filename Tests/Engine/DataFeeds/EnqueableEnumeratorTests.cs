using System;
using System.Collections.Generic;
using NUnit.Framework;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.Tests.Engine.DataFeeds
{
    [TestFixture]
    public class EnqueableEnumeratorTests
    {
        [Test]
        public void PassesTicksStraightThrough()
        {
            var enumerator = new EnqueableEnumerator<Tick>();

            // add some ticks
            var currentTime = new DateTime(2015, 10, 08);

            // returns true even if no data present until stop is called
            Assert.IsTrue(enumerator.MoveNext());
            Assert.IsNull(enumerator.Current);

            var tick1 = new Tick(currentTime, "SPY", 199.55m, 199, 200) {Quantity = 10};
            enumerator.Enqueue(tick1);
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual(tick1, enumerator.Current);

            Assert.IsTrue(enumerator.MoveNext());
            Assert.IsNull(enumerator.Current);

            var tick2 = new Tick(currentTime, "SPY", 199.56m, 199.21m, 200.02m) {Quantity = 5};
            enumerator.Enqueue(tick2);
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual(tick2, enumerator.Current);

            enumerator.Stop();

            Assert.IsFalse(enumerator.MoveNext());
            Assert.IsNull(enumerator.Current);
        }
    }
}