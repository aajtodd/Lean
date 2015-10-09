using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Authentication.ExtendedProtection.Configuration;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.Results;
using QuantConnect.Packets;

namespace QuantConnect.Tests.Engine.DataFeeds
{

    [TestFixture]//, Ignore("These tests depend on a remote server")]
    public class NewLiveTradingDataFeedTests
    {
        [Test]
        public void EmitsData()
        {
            var algorithm = new AlgorithmStub(forex: new List<string> { "EURUSD" });

            // job is used to send into DataQueueHandler
            var job = new LiveNodePacket();
            // result handler is used due to dependency in SubscriptionDataReader
            var resultHandler = new ConsoleResultHandler(); // new ResultHandlerStub();

            var lastTime = DateTime.MinValue;
            //var timeProvider = new ManualTimeProvider(new DateTime(2015, 10, 08));
            var timeProvider = new RealTimeProvider();
            var dataQueueHandler = new FuncDataQueueHandler(fdqh =>
            {
                var time = timeProvider.GetUtcNow().ConvertFromUtc(TimeZones.EasternStandard);
                if (time == lastTime) return Enumerable.Empty<BaseData>();
                lastTime = time;
                 return Enumerable.Range(0, 9).Select(x => new Tick(time.AddMilliseconds(x*100), "EURUSD", 1.3m, 1.2m, 1.3m));
            });

            var feed = new NewTestableLiveTradingDataFeed(dataQueueHandler, timeProvider);
            feed.Initialize(algorithm, job, resultHandler);

            var feedThreadStarted = new ManualResetEvent(false);
            Task.Factory.StartNew(() =>
            {
                feedThreadStarted.Set();
                feed.Run();
            });

            // wait for feed.Run to actually begin
            feedThreadStarted.WaitOne();



            ConsumeBridge(feed, TimeSpan.FromSeconds(10), true, ts =>
            {
                if (ts.Slice.Count != 0)
                {
                    Console.WriteLine("HasData: " + ts.Slice.Bars["EURUSD"].EndTime);
                    Console.WriteLine();
                }
            });

            Thread.Sleep(5000);
        }


        [Test]
        public void SubscribesToOnlyEquityAndForex()
        {
            // Current implementation only sends equity/forex subscriptions to the queue handler,
            // new impl sends all, the restriction shouldn't live in the feed, but rather in the
            // queue handler impl

            var algorithm = new AlgorithmStub(equities: new List<string> { "SPY" }, forex: new List<string> { "EURUSD" });
            algorithm.AddData<ForexRemoteFileBaseData>("RemoteFile");

            FuncDataQueueHandler dataQueueHandler;
            var feed = RunDataFeed(algorithm, out dataQueueHandler);

            Assert.IsTrue(dataQueueHandler.Subscriptions.Contains(new FuncDataQueueHandler.Subscription("SPY", SecurityType.Equity)));
            Assert.IsTrue(dataQueueHandler.Subscriptions.Contains(new FuncDataQueueHandler.Subscription("EURUSD", SecurityType.Forex)));
            Assert.IsTrue(dataQueueHandler.Subscriptions.Contains(new FuncDataQueueHandler.Subscription("REMOTEFILE", SecurityType.Base)));
            Assert.AreEqual(3, dataQueueHandler.Subscriptions.Count);
        }

        [Test]
        public void UnsubscribesFromOnlyEquityAndForex()
        {
            var algorithm = new AlgorithmStub(equities: new List<string> { "SPY" }, forex: new List<string> { "EURUSD" });
            algorithm.AddData<ForexRemoteFileBaseData>("RemoteFile");

            FuncDataQueueHandler dataQueueHandler;
            var feed = RunDataFeed(algorithm, out dataQueueHandler);

            feed.RemoveSubscription(algorithm.Securities["SPY"]);

            Assert.AreEqual(2, dataQueueHandler.Subscriptions.Count);
            Assert.IsFalse(dataQueueHandler.Subscriptions.Contains(new FuncDataQueueHandler.Subscription("SPY", SecurityType.Equity)));
            Assert.IsTrue(dataQueueHandler.Subscriptions.Contains(new FuncDataQueueHandler.Subscription("EURUSD", SecurityType.Forex)));
            Assert.IsTrue(dataQueueHandler.Subscriptions.Contains(new FuncDataQueueHandler.Subscription("RemoteFile", SecurityType.Base)));
        }

        [Test]
        public void EmitsForexDataWithRoundedUtcTimes()
        {
            var algorithm = new AlgorithmStub(forex: new List<string> { "EURUSD" });

            var feed = RunDataFeed(algorithm);

            var emittedData = false;
            ConsumeBridge(feed, TimeSpan.FromSeconds(10), ts =>
            {
                emittedData = true;
                Console.WriteLine(ts.Time);
                Assert.AreEqual(DateTime.UtcNow.RoundDown(Time.OneSecond), ts.Time);
                Assert.AreEqual(1, ts.Slice.Bars.Count);
            });

            Assert.IsTrue(emittedData);
        }

        [Test]
        public void HandlesRemoteUpdatingFiles()
        {
            var resolution = Resolution.Second;
            var algorithm = new AlgorithmStub();
            algorithm.AddData<ForexRemoteFileBaseData>("EURUSD", resolution);

            var feed = RunDataFeed(algorithm);

            int count = 0;
            bool receivedData = false;
            var timeZone = algorithm.Securities["EURUSD"].Exchange.TimeZone;
            var stopwatch = Stopwatch.StartNew();
            Console.WriteLine("start: " + DateTime.UtcNow.ToString("o"));
            ConsumeBridge(feed, TimeSpan.FromSeconds(20), ts =>
            {
                // because this is a remote file we may skip data points while the newest
                // version of the file is downloading [internet speed] and also we decide
                // not to emit old data
                if (ts.Slice.ContainsKey("EURUSD"))
                {
                    stopwatch.Stop();
                    count++;
                    receivedData = true;
                    var data = (ForexRemoteFileBaseData)ts.Slice["EURUSD"];
                    var time = data.EndTime.ConvertToUtc(timeZone);
                    Console.WriteLine("Data time: " + time.ConvertFromUtc(TimeZones.NewYork) + Environment.NewLine);
                    // make sure within 2 seconds
                    var delta = ts.Time.Subtract(time);
                    Assert.IsTrue(delta <= TimeSpan.FromSeconds(2), delta.ToString());
                }
            });

            Console.WriteLine("end: " + DateTime.UtcNow.ToString("o"));
            Console.WriteLine("Spool up time: " + stopwatch.Elapsed);

            // even though we're doing 20 seconds, give a little
            // leeway for slow internet traffic
            Assert.That(count, Is.GreaterThan(17));
            Assert.IsTrue(receivedData);
        }

        [Test]
        public void HandlesRestApi()
        {
            var resolution = Resolution.Second;
            var algorithm = new AlgorithmStub();
            algorithm.AddData<ForexRestApiBaseData>("EURUSD", resolution);

            var feed = RunDataFeed(algorithm);

            int count = 0;
            bool receivedData = false;
            var timeZone = algorithm.Securities["EURUSD"].Exchange.TimeZone;
            ForexRestApiBaseData last = null;
            ConsumeBridge(feed, TimeSpan.FromSeconds(6), ts =>
            {
                count++;
                receivedData = true;
                var data = (ForexRestApiBaseData)ts.Slice["EURUSD"];
                var time = data.EndTime.ConvertToUtc(timeZone);
                Console.WriteLine("Data time: " + time.ConvertFromUtc(TimeZones.NewYork) + Environment.NewLine);
                // make sure within 1 seconds
                var delta = ts.Time.Subtract(time);
                Assert.IsTrue(delta <= resolution.ToTimeSpan(), delta.ToString());

                if (last != null)
                {
                    Assert.AreEqual(last.EndTime, data.EndTime.Subtract(resolution.ToTimeSpan()));
                }
                last = data;
            });
            // even though we're doing 6 seconds, give a little
            // leeway for slow internet traffic
            Assert.That(count, Is.GreaterThan(3));
            Assert.IsTrue(receivedData);
        }

        [Test]
        public void HandlesCoarseFundamentalData()
        {
            var algorithm = new AlgorithmStub();
            algorithm.SetUniverse(coarse => coarse.Select(x => x.Symbol));

            var lck = new object();
            CoarseFundamentalList list = null;
            var timer = new Timer(state =>
            {
                lock (state)
                {
                    Console.WriteLine("timer.Elapsed");
                    list = new CoarseFundamentalList
                    {
                        Symbol = "qc-universe-coarse-usa",
                        Data =
                        {
                            new CoarseFundamental {Symbol = "SPY"},
                            new CoarseFundamental {Symbol = "IBM"},
                            new CoarseFundamental {Symbol = "MSFT"},
                            new CoarseFundamental {Symbol = "AAPL"},
                            new CoarseFundamental {Symbol = "GOOG"}
                        }
                    };
                }
            }, lck, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(60));

            var feed = RunDataFeed(algorithm, fdqh =>
            {
                lock (lck)
                {
                    if (list != null)
                        try
                        {
                            return new List<BaseData> { list };
                        }
                        finally
                        {
                            list = null;
                        }
                }
                return Enumerable.Empty<BaseData>();
            });

            Assert.IsTrue(feed.Subscriptions.Any(x => x.IsUniverseSelectionSubscription));

            var firedUniverseSelection = false;
            feed.UniverseSelection += (sender, args) =>
            {
                firedUniverseSelection = true;
                Console.WriteLine("Fired universe selection");
            };


            ConsumeBridge(feed, TimeSpan.FromSeconds(2), ts =>
            {
            });

            Assert.IsTrue(firedUniverseSelection);
        }



        private IDataFeed RunDataFeed(IAlgorithm algorithm, Func<FuncDataQueueHandler, IEnumerable<BaseData>> getNextTicksFunction = null)
        {
            FuncDataQueueHandler dataQueueHandler;
            return RunDataFeed(algorithm, out dataQueueHandler, getNextTicksFunction);
        }

        private IDataFeed RunDataFeed(IAlgorithm algorithm,
            out FuncDataQueueHandler dataQueueHandler,
            Func<FuncDataQueueHandler, IEnumerable<BaseData>> getNextTicksFunction = null)
        {
            getNextTicksFunction = getNextTicksFunction ?? (fdqh => fdqh.Subscriptions.Select(x => new Tick(DateTime.Now, x.Symbol, 1, 2)));

            // job is used to send into DataQueueHandler
            var job = new LiveNodePacket();
            // result handler is used due to dependency in SubscriptionDataReader
            var resultHandler = new ConsoleResultHandler(); // new ResultHandlerStub();

            dataQueueHandler = new FuncDataQueueHandler(getNextTicksFunction);

            var feed = new NewTestableLiveTradingDataFeed(dataQueueHandler);
            feed.Initialize(algorithm, job, resultHandler);

            var feedThreadStarted = new ManualResetEvent(false);
            Task.Factory.StartNew(() =>
            {
                feedThreadStarted.Set();
                feed.Run();
            });

            // wait for feed.Run to actually begin
            feedThreadStarted.WaitOne();

            return feed;
        }

        private static void ConsumeBridge(IDataFeed feed, Action<TimeSlice> handler)
        {
            ConsumeBridge(feed, TimeSpan.FromSeconds(10), handler);
        }

        private static void ConsumeBridge(IDataFeed feed, TimeSpan timeout, Action<TimeSlice> handler)
        {
            ConsumeBridge(feed, timeout, false, handler);
        }

        private static void ConsumeBridge(IDataFeed feed, TimeSpan timeout, bool alwaysInvoke, Action<TimeSlice> handler)
        {
            bool startedReceivingata = false;
            var cancellationTokenSource = new CancellationTokenSource(timeout);
            foreach (var timeSlice in feed.Bridge.GetConsumingEnumerable(cancellationTokenSource.Token))
            {
                Console.WriteLine("TimeSlice.Time (EDT): " + timeSlice.Time.ConvertFromUtc(TimeZones.NewYork).ToString("o"));
                if (!startedReceivingata && timeSlice.Slice.Count != 0)
                {
                    startedReceivingata = true;
                    Console.WriteLine("Recieved data");
                }
                if (startedReceivingata || alwaysInvoke)
                {
                    handler(timeSlice);
                }
            }
        }

    }

    public class NewTestableLiveTradingDataFeed : NewLiveTradingDataFeed
    {
        private readonly ITimeProvider _timeProvider;
        private readonly IDataQueueHandler _dataQueueHandler;

        public NewTestableLiveTradingDataFeed(IDataQueueHandler dataQueueHandler, ITimeProvider timeProvider = null)
        {
            _dataQueueHandler = dataQueueHandler;
            _timeProvider = timeProvider ?? new RealTimeProvider();
        }

        protected override IDataQueueHandler GetDataQueueHandler()
        {
            return _dataQueueHandler;
        }

        protected override ITimeProvider GetTimeProvider()
        {
            return _timeProvider;
        }
    }
}
