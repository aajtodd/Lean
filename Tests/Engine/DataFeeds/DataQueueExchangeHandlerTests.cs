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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.Tests.Engine.DataFeeds
{
    [TestFixture]
    public class DataQueueExchangeHandlerTests
    {
        [Test]
        public void FiresCorrectHandlerBySymbol()
        {
            var dataQueue = new ConcurrentQueue<BaseData>();
            var exchange = CreateExchange(dataQueue);

            var firedHandler = false;
            var firedWrongHandler = false;
            exchange.SetHandler("SPY", spy =>
            {
                firedHandler = true;
            });
            exchange.SetHandler("EURUSD", eurusd =>
            {
                firedWrongHandler = true;
            });

            dataQueue.Enqueue(new Tick{Symbol = "SPY"});

            exchange.BeginConsumeQueue();

            Thread.Sleep(10);

            Assert.IsTrue(firedHandler);
            Assert.IsFalse(firedWrongHandler);
        }

        [Test]
        public void RemovesHandlerBySymbol()
        {
            var dataQueue = new ConcurrentQueue<BaseData>();
            var exchange = CreateExchange(dataQueue);

            var firedHandler = false;
            exchange.SetHandler("SPY", spy =>
            {
                firedHandler = true;
            });
            exchange.RemoveHandler("SPY");

            dataQueue.Enqueue(new Tick {Symbol = "SPY"});

            exchange.BeginConsumeQueue();

            Thread.Sleep(10);

            Assert.IsFalse(firedHandler);
        }

        [Test]
        public void EndsQueueConsumption()
        {
            var dataQueue = new ConcurrentQueue<BaseData>();
            var exchange = CreateExchange(dataQueue);
            
            Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    Thread.Sleep(1);
                    dataQueue.Enqueue(new Tick {Symbol = "SPY", Time = DateTime.UtcNow});
                }
            });

            BaseData last = null;
            exchange.SetHandler("SPY", spy =>
            {
                last = spy;
            });

            exchange.BeginConsumeQueue();

            Thread.Sleep(25);

            exchange.EndConsumeQueue();

            var endTime = DateTime.UtcNow;

            Assert.IsNotNull(last);
            Assert.IsTrue(last.Time <= endTime);
        }

        [Test]
        public void DefaultErrorHandlerDoesNotStopQueueConsumption()
        {
            var dataQueue = new ConcurrentQueue<BaseData>();
            var exchange = CreateExchange(dataQueue);

            Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    Thread.Sleep(1);
                    dataQueue.Enqueue(new Tick { Symbol = "SPY", Time = DateTime.UtcNow });
                }
            });

            var first = true;
            BaseData last = null;
            exchange.SetHandler("SPY", spy =>
            {
                if (first)
                {
                    first = false;
                    throw new Exception();
                }
                last = spy;
            });

            exchange.BeginConsumeQueue();

            Thread.Sleep(25);

            exchange.EndConsumeQueue();

            Assert.IsNotNull(last);
        }

        [Test]
        public void SetErrorHandlerExitsOnTrueReturn()
        {
            var dataQueue = new ConcurrentQueue<BaseData>();
            var exchange = CreateExchange(dataQueue);

            Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    Thread.Sleep(1);
                    dataQueue.Enqueue(new Tick { Symbol = "SPY", Time = DateTime.UtcNow });
                }
            });

            var first = true;
            BaseData last = null;
            exchange.SetHandler("SPY", spy =>
            {
                if (first)
                {
                    first = false;
                    throw new Exception();
                }
                last = spy;
            });

            exchange.SetErrorHandler(error => true);

            exchange.BeginConsumeQueue();

            Thread.Sleep(25);

            exchange.EndConsumeQueue();

            Assert.IsNull(last);
        }

        private static DataQueueExchange CreateExchange(ConcurrentQueue<BaseData> dataQueue)
        {
            var dataQueueHandler = new FuncDataQueueHandler(q =>
            {
                BaseData data;
                var list = new List<BaseData>();
                while (dataQueue.TryDequeue(out data)) list.Add(data);
                return list;
            });
            var exchange = new DataQueueExchange(dataQueueHandler);
            return exchange;
        }
    }
}
