using System;
using System.Collections.Generic;
using QuantConnect.Data;
using QuantConnect.Securities;

namespace QuantConnect.Lean.Engine.DataFeeds
{
    /// <summary>
    /// An implementation of the <see cref="FillForwardEnumerator"/> that uses an <see cref="ITimeProvider"/>
    /// to determine if a fill forward bar needs to be emitted
    /// </summary>
    public class LiveFillForwardEnumerator : FillForwardEnumerator
    {
        private readonly ITimeProvider _timeProvider;

        /// <summary>
        /// Initializes a new instance of the <see cref="LiveFillForwardEnumerator"/> class
        /// </summary>
        /// <param name="timeProvider">The source of time used to gauage when this enumerator should emit extra bars when
        /// null data is returned from the source enumerator</param>
        /// <param name="enumerator">The source enumerator to be filled forward</param>
        /// <param name="exchange">The exchange used to determine when to insert fill forward data</param>
        /// <param name="fillForwardResolution">The resolution we'd like to receive data on</param>
        /// <param name="isExtendedMarketHours">True to use the exchange's extended market hours, false to use the regular market hours</param>
        /// <param name="subscriptionEndTime">The end time of the subscrition, once passing this date the enumerator will stop</param>
        /// <param name="dataResolution">The source enumerator's data resolution</param>
        public LiveFillForwardEnumerator(ITimeProvider timeProvider, IEnumerator<BaseData> enumerator, SecurityExchange exchange, TimeSpan fillForwardResolution, bool isExtendedMarketHours, DateTime subscriptionEndTime, TimeSpan dataResolution)
            : base(enumerator, exchange, fillForwardResolution, isExtendedMarketHours, subscriptionEndTime, dataResolution)
        {
            _timeProvider = timeProvider;
        }

        /// <summary>
        /// Determines whether or not fill forward is required, and if true, will produce the new fill forward data
        /// </summary>
        /// <param name="previous">The last piece of data emitted by this enumerator</param>
        /// <param name="next">The next piece of data on the source enumerator, this may be null</param>
        /// <param name="fillForward">When this function returns true, this will have a non-null value, null when the function returns false</param>
        /// <returns>True when a new fill forward piece of data was produced and should be emitted by this enumerator</returns>
        protected override bool RequiresFillForwardData(BaseData previous, BaseData next, out BaseData fillForward)
        {
            fillForward = null;
            var nextExpectedDataPointTime = (previous.EndTime + FillForwardResolution);
            if (next != null)
            {
                // if not future data, just return the 'next'
                if (next.EndTime <= nextExpectedDataPointTime)
                {
                    return false;
                }
                // next is future data, fill forward in between
                var clone = previous.Clone(true);
                clone.Time = previous.Time + FillForwardResolution;
                fillForward = clone;
                return true;
            }

            // the underlying enumerator returned null, check to see if time has passed for fill fowarding
            var currentLocalTime = _timeProvider.GetUtcNow().ConvertFromUtc(Exchange.TimeZone);
            if (nextExpectedDataPointTime <= currentLocalTime)
            {
                var clone = previous.Clone(true);
                clone.Time = previous.Time + FillForwardResolution;
                fillForward = clone;
                return true;
            }

            return false;
        }
    }
}