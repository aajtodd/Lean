using System;
using NodaTime;
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.Tests.Engine.DataFeeds
{
    public class ManualTimeProvider : ITimeProvider
    {
        private DateTime _currentTime;

        private readonly DateTimeZone _setCurrentTimeTimeZone;

        public ManualTimeProvider(DateTimeZone setCurrentTimeTimeZone = null)
        {
            _setCurrentTimeTimeZone = setCurrentTimeTimeZone ?? TimeZones.Utc;
        }

        public ManualTimeProvider(DateTime currentTime, DateTimeZone setCurrentTimeTimeZone = null)
        {
            _setCurrentTimeTimeZone = setCurrentTimeTimeZone ?? TimeZones.Utc;
            _currentTime = currentTime.ConvertToUtc(_setCurrentTimeTimeZone);
        }

        public DateTime GetUtcNow()
        {
            return _currentTime;
        }

        public void SetCurrentTime(DateTime time)
        {
            _currentTime = time.ConvertToUtc(_setCurrentTimeTimeZone);
        }

        public void Advance(TimeSpan span)
        {
            _currentTime += span;
        }

        public void AdvanceSeconds(double seconds)
        {
            Advance(TimeSpan.FromSeconds(seconds));
        }
    }
}