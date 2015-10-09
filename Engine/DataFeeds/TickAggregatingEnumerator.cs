/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using NodaTime;
using QuantConnect.Data;
using QuantConnect.Data.Market;

namespace QuantConnect.Lean.Engine.DataFeeds
{
    /// <summary>
    /// Aggregates ticks into trade bars ready to be time synced
    /// </summary>
    public class TickAggregatingEnumerator : IEnumerator<BaseData>
    {
        private readonly TimeSpan _barSize;
        private readonly DateTimeZone _timeZone;
        private readonly ITimeProvider _timeProvider;
        private readonly ConcurrentQueue<TradeBar> _singleItemQueue;

        /// <summary>
        /// Initializes a new instance of the <see cref="TickAggregatingEnumerator"/> class
        /// </summary>
        /// <param name="barSize">The trade bar size to produce</param>
        /// <param name="timeZone">The time zone the raw data is time stamped in</param>
        /// <param name="timeProvider">The source of 'real-time', if left null, the <see cref="RealTimeProvider"/> will be used</param>
        public TickAggregatingEnumerator(TimeSpan barSize, DateTimeZone timeZone, ITimeProvider timeProvider = null)
        {
            _barSize = barSize;
            _timeZone = timeZone;
            _singleItemQueue = new ConcurrentQueue<TradeBar>();
            _timeProvider = timeProvider ?? new RealTimeProvider();
        }
        /// <summary>
        /// Pushes the tick into this enumerator. This tick will be aggregated into a bar
        /// and emitted after the alotted time has passed
        /// </summary>
        /// <param name="tick">The new tick to be aggregated</param>
        public void ProcessTick(Tick tick)
        {
            TradeBar working;
            if (!_singleItemQueue.TryPeek(out working))// || time >= working.EndTime) this would give us multiple items in the queue.. checkout out Eric Lippert's deque
            {
                // the consumer took the working bar
                var marketPrice = tick.LastPrice;
                var barStartTime = _timeProvider.GetUtcNow().ConvertFromUtc(_timeZone).RoundDown(_barSize);
                working = new TradeBar(barStartTime, tick.Symbol, marketPrice, marketPrice, marketPrice, marketPrice, tick.Quantity, _barSize);
                _singleItemQueue.Enqueue(working);
            }
            else
            {
                // we're still within this bar size's time
                working.Update(tick.LastPrice, tick.BidPrice, tick.AskPrice, tick.Quantity);
            }
        }

        /// <summary>
        /// Advances the enumerator to the next element of the collection.
        /// </summary>
        /// <returns>
        /// true if the enumerator was successfully advanced to the next element; false if the enumerator has passed the end of the collection.
        /// </returns>
        public bool MoveNext()
        {
            TradeBar working;

            // check if there's a bar there and if its time to pull it off (i.e, done aggregation)
            if (_singleItemQueue.TryPeek(out working) && working.EndTime.ConvertToUtc(_timeZone) <= _timeProvider.GetUtcNow())
            {
                // working is good to go, set it to current
                Current = working;
                // remove working from the queue so we can start aggregating the next ba
                _singleItemQueue.TryDequeue(out working);
            }
            else
            {
                Current = null;
            }

            // IEnumerator contract dictates that we return true unless we're actually
            // finished with the 'collection' and since this is live, we're never finished
            return true;
        }

        /// <summary>
        /// Sets the enumerator to its initial position, which is before the first element in the collection.
        /// </summary>
        public void Reset()
        {
            _singleItemQueue.Clear();
        }

        /// <summary>
        /// Gets the element in the collection at the current position of the enumerator.
        /// </summary>
        /// <returns>
        /// The element in the collection at the current position of the enumerator.
        /// </returns>
        public BaseData Current
        {
            get; private set;
        }

        /// <summary>
        /// Gets the current element in the collection.
        /// </summary>
        /// <returns>
        /// The current element in the collection.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        object IEnumerator.Current
        {
            get { return Current; }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
        }
    }
}