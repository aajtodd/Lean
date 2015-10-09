using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.Results;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;

namespace QuantConnect.Lean.Engine.DataFeeds
{
    public class NewLiveTradingDataFeed : IDataFeed
    {
        private SecurityChanges _changes = SecurityChanges.None;

        private LiveNodePacket _job;
        private IAlgorithm _algorithm;
        private ITimeProvider _timeProvider;
        private IResultHandler _resultHandler;
        private IDataQueueHandler _dataQueueHandler;
        private DataQueueExchange _dataQueueExchange;
        private ConcurrentDictionary<SymbolSecurityType, NewLiveSubscription> _subscriptions;
        private CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        protected IAlgorithm Algorithm
        {
            get { return _algorithm; }
        }

        protected ITimeProvider TimeProvider
        {
            get { return _timeProvider; }
        }

        protected IResultHandler ResultHandler
        {
            get { return _resultHandler; }
        }

        protected IDataQueueHandler DataQueueHandler
        {
            get { return _dataQueueHandler; }
        }

        protected DataQueueExchange DataQueueExchange
        {
            get { return _dataQueueExchange; }
        }

        #region Implementation of IDataFeed

        public event EventHandler<UniverseSelectionEventArgs> UniverseSelection;

        public IEnumerable<Subscription> Subscriptions
        {
            get { return _subscriptions.Select(x => x.Value); }
        }

        public BusyBlockingCollection<TimeSlice> Bridge
        {
            get; private set;
        }

        public bool IsActive
        {
            get; private set;
        }

        public void Initialize(IAlgorithm algorithm, AlgorithmNodePacket job, IResultHandler resultHandler)
        {
            _cancellationTokenSource = new CancellationTokenSource();

            _algorithm = algorithm;
            _job = (LiveNodePacket) job;
            _resultHandler = resultHandler;
            _timeProvider = GetTimeProvider();
            _dataQueueHandler = GetDataQueueHandler();
            _dataQueueExchange = new DataQueueExchange(_dataQueueHandler);
            _subscriptions = new ConcurrentDictionary<SymbolSecurityType, NewLiveSubscription>();

            Bridge = new BusyBlockingCollection<TimeSlice>();

            // run the exchange
            _dataQueueExchange.BeginConsumeQueue();

            // add user defined subscriptions
            var start = _timeProvider.GetUtcNow();
            foreach (var kvp in _algorithm.Securities)
            {
                var security = kvp.Value;
                AddSubscription(security, start, Time.EndOfTime, true);
            }

            // add universe subscriptions
            foreach (var universe in _algorithm.Universes)
            {
                var subscription = CreateUniverseSubscription(universe, start, Time.EndOfTime);
                _subscriptions[new SymbolSecurityType(subscription)] = subscription;
            }
        }

        public void AddSubscription(Security security, DateTime utcStartTime, DateTime utcEndTime, bool isUserDefinedSubscription)
        {
            var subscription = CreateSubscription(security, utcStartTime, utcEndTime, isUserDefinedSubscription);
            _subscriptions[new SymbolSecurityType(subscription)] = subscription;

            _dataQueueHandler.Subscribe(_job, new Dictionary<SecurityType, List<string>>
            {
                {security.Type, new List<string> {security.Symbol}}
            });
            _changes += SecurityChanges.Added(security);
        }

        public void RemoveSubscription(Security security)
        {
            NewLiveSubscription subscription;
            _subscriptions.TryRemove(new SymbolSecurityType(security), out subscription);
            _dataQueueExchange.RemoveHandler(security.Symbol);

            _dataQueueHandler.Unsubscribe(_job, new Dictionary<SecurityType, List<string>>
            {
                {security.Type, new List<string> {security.Symbol}}
            });

            _changes += SecurityChanges.Removed(security);
        }

        public void Run()
        {
            IsActive = true;

            var nextEmit = DateTime.MinValue;
            var sleepIncrement = _subscriptions.Any(x => x.Value.Configuration.Resolution == Resolution.Tick)
                ? Time.OneMillisecond
                : Time.OneSecond;

            try
            {
                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    if (_changes != SecurityChanges.None)
                    {
                        sleepIncrement = _subscriptions.Any(x => x.Value.Configuration.Resolution == Resolution.Tick)
                            ? Time.OneMillisecond
                            : Time.OneSecond;
                    }

                    //perform sleeps to wake up on the second?
                    var frontier = _timeProvider.GetUtcNow();
                    var roundingIncrement = sleepIncrement;
                    var data = new List<KeyValuePair<Security, List<BaseData>>>();
                    foreach (var kvp in _subscriptions)
                    {
                        var subscription = kvp.Value;

                        var cache = new KeyValuePair<Security, List<BaseData>>(subscription.Security, new List<BaseData>());

                        // use subscription.Current if it wasn't pulled last loop, otherwise move next
                        Console.WriteLine("MoveNextTimeUtc: " + DateTime.UtcNow.ToString("o"));
                        while (!subscription.NeedsMoveNext || subscription.MoveNext())
                        {
                            // nothing to pull off
                            if (subscription.Current == null)
                            {
                                subscription.NeedsMoveNext = true;
                                break;
                            }
                            // TODO : need fast forward logic to skip old data
                            if (subscription.Current.EndTime.ConvertToUtc(subscription.Configuration.TimeZone) > frontier)
                            {
                                // the next item is in the future, save it for next iteration
                                subscription.NeedsMoveNext = false;
                                break;
                            }
                            cache.Value.Add(subscription.Current);
                            subscription.NeedsMoveNext = true;
                        }

                        if (cache.Value.Count > 0)
                        {
                            data.Add(cache);
                            if (subscription.Configuration.Resolution == Resolution.Tick)
                            {
                                roundingIncrement = Time.OneMillisecond;
                            }
                        }

                        // we have new universe data to select based on
                        if (subscription.IsUniverseSelectionSubscription && cache.Value.Count > 0)
                        {
                            var universe = subscription.Universe;

                            // always wait for other thread
                            if (!Bridge.Wait(Timeout.Infinite, _cancellationTokenSource.Token))
                            {
                                break;
                            }

                            OnUniverseSelection(universe, subscription.Configuration, frontier, cache.Value);
                        }
                    }

                    // check for cancellation
                    if (_cancellationTokenSource.IsCancellationRequested) return;

                    if (data.Count != 0 || frontier >= nextEmit)
                    {
                        if (data.Count != 0)
                        {
                            Console.WriteLine("Count: " + data.First().Value.Count);
                        }
                        // add our data to the bridge
                        var emitTime = frontier.RoundDown(roundingIncrement);
                        Bridge.Add(TimeSlice.Create(emitTime, _algorithm.TimeZone, _algorithm.Portfolio.CashBook, data, _changes));
                        nextEmit = emitTime + Time.OneSecond;
                    }

                    // reset our security changes
                    _changes = SecurityChanges.None;

                    // determine how long to pause
                    var time = _timeProvider.GetUtcNow();
                    var sleepTime = time.Add(sleepIncrement).RoundDown(sleepIncrement) - time;
                    Thread.Sleep((int) Math.Ceiling(Math.Max(sleepTime.TotalMilliseconds, 1)));
                }
            }
            catch (Exception err)
            {
                Log.Error(err);
            }
            IsActive = false;
        }

        public void Exit()
        {
            _cancellationTokenSource.Cancel();
        }

        #endregion

        protected virtual IDataQueueHandler GetDataQueueHandler()
        {
            return Composer.Instance.GetExportedValueByTypeName<IDataQueueHandler>(Config.Get("data-queue-handler", "LiveDataQueue"));
        }

        protected virtual ITimeProvider GetTimeProvider()
        {
            return new RealTimeProvider();
        }

        protected virtual NewLiveSubscription CreateSubscription(Security security, DateTime utcStartTime, DateTime utcEndTime, bool isUserDefinedSubscription)
        {
            var config = security.SubscriptionDataConfig;
            var localStartTime = utcStartTime.ConvertFromUtc(config.TimeZone);
            var localEndTime = utcEndTime.ConvertFromUtc(config.TimeZone);

            IEnumerator<BaseData> enumerator;
            NewLiveSubscription subscription = null;
            if (config.IsCustomData)
            {
                var tradeableDates = Time.EachTradeableDay(security, localStartTime, localEndTime);
                enumerator = new SubscriptionDataReader(config, localStartTime, localEndTime, _resultHandler, tradeableDates, true, false);
            }
            else if (config.Resolution != Resolution.Tick)
            {
                var aggregator = new TickAggregatingEnumerator(config.Increment, config.TimeZone, _timeProvider);
                _dataQueueExchange.SetHandler(config.Symbol, data =>
                {
                    aggregator.ProcessTick((Tick)data);
                    if (subscription != null)
                    {
                        subscription.SetRealtimePrice(data.Value);
                    }
                });
                enumerator = aggregator;
            }
            else
            {
                var tickEnumerator = new EnqueableEnumerator<Tick>();
                _dataQueueExchange.SetHandler(config.Symbol, data =>
                {
                    tickEnumerator.Enqueue((Tick) data);
                    if (subscription != null)
                    {
                        subscription.SetRealtimePrice(data.Value);
                    }
                });
                enumerator = tickEnumerator;
            }

            if (config.FillDataForward)
            {
                // TODO : Properly resolve fill forward resolution like in FileSystemDataFeed (make considerations for universe-only)
                enumerator = new LiveFillForwardEnumerator(_timeProvider, enumerator, security.Exchange, config.Increment, config.ExtendedMarketHours, localEndTime, config.Increment);
            }

            enumerator = new SubscriptionFilterEnumerator(enumerator, security, localEndTime);

            subscription = new NewLiveSubscription(security, enumerator, utcStartTime, utcEndTime, isUserDefinedSubscription);

            subscription.MoveNext();
            subscription.NeedsMoveNext = subscription.Current == null;

            return subscription;
        }

        /// <summary>
        /// Creates a new subscription for universe selection
        /// </summary>
        /// <param name="universe">The universe to add a subscription for</param>
        /// <param name="startTimeUtc">The start time of the subscription in utc</param>
        /// <param name="endTimeUtc">The end time of the subscription in utc</param>
        protected virtual NewLiveSubscription CreateUniverseSubscription(
            IUniverse universe,
            DateTime startTimeUtc,
            DateTime endTimeUtc
            )
        {
            // grab the relevant exchange hours
            var config = universe.Configuration;

            var exchangeHours = SecurityExchangeHoursProvider.FromDataFolder()
                .GetExchangeHours(config.Market, null, config.SecurityType);

            // create a canonical security object
            var security = new Security(exchangeHours, config, universe.SubscriptionSettings.Leverage);

            IEnumerator<BaseData> enumerator;
            if (config.Type == typeof (CoarseFundamental))
            {
                var enqueable = new EnqueableEnumerator<BaseData>();
                _dataQueueExchange.SetHandler(config.Symbol, dto =>
                {
                    var coarseList = dto as CoarseFundamentalList;
                    if (coarseList != null)
                    {
                        foreach (var coarse in coarseList.Data)
                        {
                            enqueable.Enqueue(coarse);
                        }
                    }
                });
                enumerator = enqueable;
            }
            else
            {
                var localStartTime = startTimeUtc.ConvertFromUtc(config.TimeZone);
                var localEndTime = endTimeUtc.ConvertFromUtc(config.TimeZone);

                // define our data enumerator
                var tradeableDates = Time.EachTradeableDay(security, localStartTime, localEndTime);
                enumerator = new SubscriptionDataReader(config, localStartTime, localEndTime, _resultHandler, tradeableDates, true);
            }

            // create the subscription
            var subscription = new NewLiveSubscription(universe, security, enumerator, startTimeUtc, endTimeUtc);

            return subscription;
        }

        protected virtual void OnUniverseSelection(IUniverse universe, SubscriptionDataConfig config, DateTime dateTimeUtc, IReadOnlyList<BaseData> data)
        {
            var handler = UniverseSelection;
            if (handler != null) handler(this, new UniverseSelectionEventArgs(universe, config, dateTimeUtc, data));
        }
    }

    public class NewLiveSubscription : Subscription
    {
        public bool NeedsMoveNext { get; set; }
        public NewLiveSubscription(Security security, IEnumerator<BaseData> enumerator, DateTime utcStartTime, DateTime utcEndTime, bool isUserDefined)
            : base(security, enumerator, utcStartTime, utcEndTime, isUserDefined)
        {
            NeedsMoveNext = true;
        }

        public NewLiveSubscription(IUniverse universe, Security security, IEnumerator<BaseData> enumerator, DateTime utcStartTime, DateTime utcEndTime)
            : base(universe, security, enumerator, utcStartTime, utcEndTime)
        {
            NeedsMoveNext = true;
        }

        public void SetRealtimePrice(decimal marketPrice)
        {
            RealtimePrice = marketPrice;
        }
    }

    public interface ITimeProvider
    {
        DateTime GetUtcNow();
    }

    public sealed class RealTimeProvider : ITimeProvider
    {
        public DateTime GetUtcNow()
        {
            return DateTime.UtcNow;
        }
    }
}
