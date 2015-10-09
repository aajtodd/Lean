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
 *
*/

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Logging;

namespace QuantConnect.Lean.Engine.DataFeeds
{
    /// <summary>
    /// Provides a wrapper on an <see cref="IDataQueueHandler"/> to pipe it's items
    /// into dedicated handlers by <see cref="Symbol"/>
    /// </summary>
    public class DataQueueExchange
    {
        private Func<Exception, bool> _isFatalError; 
        private readonly IDataQueueHandler _dataQueueHandler;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly ConcurrentDictionary<Symbol, Action<BaseData>> _handlers;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataQueueExchange"/>
        /// </summary>
        /// <param name="dataQueueHandler">The <see cref="IDataQueueHandler"/> to be wrapped</param>
        public DataQueueExchange(IDataQueueHandler dataQueueHandler)
        {
            _isFatalError = x => false;
            _dataQueueHandler = dataQueueHandler;
            _cancellationTokenSource = new CancellationTokenSource();
            _handlers = new ConcurrentDictionary<Symbol, Action<BaseData>>();
        }

        /// <summary>
        /// Sets the specified function as the error handler. This function
        /// returns true if it is a fatal error and queue consumption should
        /// cease.
        /// </summary>
        /// <param name="isFatalError">The error handling function to use when an
        /// error is encountered during queue consumption. Returns true if queue
        /// consumption should be stopped, returns false if queue consumption should
        /// continue</param>
        public void SetErrorHandler(Func<Exception, bool> isFatalError)
        {
            // default to false;
            _isFatalError = isFatalError ?? (x => false);
        }

        /// <summary>
        /// Sets the specified hander function to handle data for the symbol
        /// </summary>
        /// <param name="symbol">The symbol to attach this handler to</param>
        /// <param name="handler">The handler to use when this symbol's data is encountered</param>
        public void SetHandler(Symbol symbol, Action<BaseData> handler)
        {
            _handlers[symbol] = handler;
        }

        /// <summary>
        /// Removes the handler for the specified symbol
        /// </summary>
        /// <param name="symbol">The symbol to remove the handler for</param>
        /// <returns>True if the handler existed and was removed, false otherwise</returns>
        public bool RemoveHandler(Symbol symbol)
        {
            Action<BaseData> handler;
            return _handlers.TryRemove(symbol, out handler);
        }

        /// <summary>
        /// Begins consumption of the wrapped <see cref="IDataQueueHandler"/> on
        /// a separate thread
        /// </summary>
        public void BeginConsumeQueue()
        {
            // mark as long running to get dedicated thread
            Task.Factory.StartNew(ConsumeQueue, TaskCreationOptions.LongRunning);
        }

        /// <summary>
        /// Ends consumption of the wrapped <see cref="IDataQueueHandler"/>
        /// </summary>
        public void EndConsumeQueue()
        {
            _cancellationTokenSource.Cancel();
        }

        /// <summary> Entry point for queue consumption </summary>
        /// <remarks> This function only return after <see cref="EndConsumeQueue"/> is called </remarks>
        private void ConsumeQueue()
        {
            while (true)
            {
                if (_cancellationTokenSource.IsCancellationRequested)
                {
                    Log.Trace("DataQueueHandlerExchange.ConsumeQueue(): Exiting...");
                    return;
                }

                try
                {
                    bool handled = false;
                    foreach (var data in _dataQueueHandler.GetNextTicks())
                    {
                        Action<BaseData> handler;
                        if (_handlers.TryGetValue(data.Symbol, out handler))
                        {
                            handled = true;
                            handler.Invoke(data);
                        }
                    }
                    if (!handled)
                    {
                        Thread.Sleep(5);
                    }
                }
                catch (Exception err)
                {
                    Log.Error(err);
                    if (_isFatalError(err))
                    {
                        Log.Trace("DataQueueHandlerExchange.ConsumeQueue(): Fatal error encountered. Exiting...");
                        return;
                    }
                }
            }
        }
    }
}