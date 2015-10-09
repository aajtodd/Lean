using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace QuantConnect.Lean.Engine.DataFeeds
{
    /// <summary>
    /// An implementation of <see cref="IEnumerator{T}"/> that relies on the
    /// <see cref="Enqueue"/> method being called
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class EnqueableEnumerator<T> : IEnumerator<T>
    {
        private T _current;
        private bool _end;

        private readonly ConcurrentQueue<T> _queue = new ConcurrentQueue<T>();

        /// <summary>
        /// Enqueues the new data into this enumerator
        /// </summary>
        /// <param name="data">The data to be enqueued</param>
        public void Enqueue(T data)
        {
            if (_end) return;
            _queue.Enqueue(data);
        }

        /// <summary>
        /// Signals the enumerator to stop enumerating when the items currently
        /// held inside are gone. No more items will be added to this enumerator.
        /// </summary>
        public void Stop()
        {
            _end = true;
        }

        /// <summary>
        /// Advances the enumerator to the next element of the collection.
        /// </summary>
        /// <returns>
        /// true if the enumerator was successfully advanced to the next element; false if the enumerator has passed the end of the collection.
        /// </returns>
        /// <exception cref="T:System.InvalidOperationException">The collection was modified after the enumerator was created. </exception><filterpriority>2</filterpriority>
        public bool MoveNext()
        {
            if (!_queue.TryDequeue(out _current))
            {
                return !_end;
            }

            // even if we don't have data to return, we haven't technically
            // passed the end of the collection, so always return true until
            // the enumerator is explicitly disposed or ended
            return true;
        }

        /// <summary>
        /// Sets the enumerator to its initial position, which is before the first element in the collection.
        /// </summary>
        /// <exception cref="T:System.InvalidOperationException">The collection was modified after the enumerator was created. </exception><filterpriority>2</filterpriority>
        public void Reset()
        {
            _queue.Clear();
        }

        /// <summary>
        /// Gets the element in the collection at the current position of the enumerator.
        /// </summary>
        /// <returns>
        /// The element in the collection at the current position of the enumerator.
        /// </returns>
        public T Current
        {
            get { return _current; }
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
            Stop();
            _queue.Clear();
        }
    }
}