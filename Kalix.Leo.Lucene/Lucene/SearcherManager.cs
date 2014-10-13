using Kalix.Leo;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System;
using System.Threading;

// https://github.com/NielsKuhnel/NrtManager
namespace Lucene.Net.Contrib.Management
{
    public class SearcherManager : IDisposable
    {
        private readonly ISearcherWarmer _warmer;
        private readonly object _reopenLock = new object();
        private volatile IndexSearcher _currentSearcher;

        public SearcherManager(IndexWriter writer, bool applyAllDeletes = true, ISearcherWarmer warmer = null)
        {
            _warmer = warmer;
            _currentSearcher = new IndexSearcher(writer.GetReader());
            if (_warmer != null)
            {
                writer.MergedSegmentWarmer = new WarmerWrapper(_warmer);
            }
        }

        public SearcherManager(Directory directory, Analysis.Analyzer analyzer, ISearcherWarmer warmer = null)
        {
            _warmer = warmer;
            IndexReader reader;
            try
            {
                reader = IndexReader.Open(directory, true);
            }
            catch(System.IO.FileNotFoundException)
            {
                new IndexWriter(directory, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED).Dispose();
                reader = IndexReader.Open(directory, true);
            }
            _currentSearcher = new IndexSearcher(reader);
        }

        public bool MaybeReopen()
        {
            EnsureOpen();
            if (Monitor.TryEnter(_reopenLock))
            {
                try
                {
                    var currentReader = _currentSearcher.IndexReader;
                    IndexReader newReader;
                    try
                    {
                        LeoTrace.WriteLine("Reopening Lucene Index Reader");
                        newReader = _currentSearcher.IndexReader.Reopen();
                    }
                    catch (System.IO.FileNotFoundException)
                    {
                        return false;
                    }

                    if (newReader != currentReader)
                    {
                        var newSearcher = new IndexSearcher(newReader);
                        var success = false;
                        try
                        {
                            if (_warmer != null)
                            {
                                _warmer.Warm(newSearcher);
                            }
                            SwapSearcher(newSearcher);
                            success = true;
                        }
                        finally
                        {
                            if (!success)
                            {
                                ReleaseSearcher(newSearcher);
                            }
                        }
                    }
                    return true;
                }
                finally
                {
                    Monitor.Exit(_reopenLock);
                }
            }

            return false;
        }

        public bool IsSearcherCurrent
        {
            get
            {
                var searcher = AcquireSearcher();
                try
                {
                    return searcher.IndexReader.IsCurrent();
                }
                finally
                {
                    ReleaseSearcher(searcher);
                }
            }
        }

        public IndexSearcherToken Acquire()
        {
            return new IndexSearcherToken(AcquireSearcher(), this);
        }

        private IndexSearcher AcquireSearcher()
        {
            LeoTrace.WriteLine("Acquiring Lucene Searcher");
            IndexSearcher searcher;

            if ((searcher = _currentSearcher) == null)
            {
                throw new AlreadyClosedException("this SearcherManager is closed");
            }
            searcher.IndexReader.IncRef();
            return searcher;
        }

        private void ReleaseSearcher(IndexSearcher searcher)
        {
            LeoTrace.WriteLine("Releasing Lucene Searcher");
            searcher.IndexReader.DecRef();
        }

        private void EnsureOpen()
        {
            if (_currentSearcher == null)
            {
                throw new AlreadyClosedException("this SearcherManager is closed");
            }
        }

        private void SwapSearcher(IndexSearcher newSearcher)
        {
            LeoTrace.WriteLine("Swapping Lucene Searcher");
            EnsureOpen();
            var oldSearcher = _currentSearcher;
            _currentSearcher = newSearcher;
            ReleaseSearcher(oldSearcher);
        }

        public void Dispose()
        {
            if (_currentSearcher != null)
            {
                // make sure we can call this more than once
                // closeable javadoc says:
                // if this is already closed then invoking this method has no effect.
                SwapSearcher(null);
            }
        }


        public class IndexSearcherToken : IDisposable
        {
            private readonly SearcherManager _manager;
            public IndexSearcher Searcher { get; private set; }

            public IndexSearcherToken(IndexSearcher searcher, SearcherManager manager)
            {
                _manager = manager;
                Searcher = searcher;
            }

            public void Dispose()
            {
                _manager.ReleaseSearcher(Searcher);
            }
        }

        class WarmerWrapper : IndexWriter.IndexReaderWarmer
        {
            private readonly ISearcherWarmer _searcher;

            public WarmerWrapper(ISearcherWarmer searcher)
            {
                _searcher = searcher;
            }

            public override void Warm(IndexReader reader)
            {
                _searcher.Warm(new IndexSearcher(reader));
            }
        }
    }
}