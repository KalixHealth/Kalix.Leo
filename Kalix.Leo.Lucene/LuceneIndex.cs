using Kalix.Leo.Encryption;
using Kalix.Leo.Lucene.Analysis;
using Kalix.Leo.Lucene.Store;
using Kalix.Leo.Storage;
using Lucene.Net.Analysis;
using Lucene.Net.Contrib.Management;
using Lucene.Net.Contrib.Management.Client;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using IO = System.IO;
using L = Lucene.Net.Index;
using System.Linq;

namespace Kalix.Leo.Lucene
{
    public class LuceneIndex : IDisposable, ILuceneIndex
    {
        private readonly Directory _directory;
        private readonly Analyzer _analyzer;

        private readonly Directory _cacheDirectory;

        private IndexSearcher _reader;
        private DateTime _lastRead;

        private SearcherContextInternal _writer;
        private object _writerLock = new object();

        private readonly double _RAMSizeMb;
        private bool _isDisposed;

        /// <summary>
        /// Create a lucene index over the top of a secure store, using an encrypted file cache and english analyzer
        /// Only one instance should be used for both indexing and searching (on any number of threads) for best results
        /// </summary>
        /// <param name="store">Store to have the Indexer on top of</param>
        /// <param name="container">Container to put the index</param>
        /// <param name="RAMSizeMb">The max amount of memory to use before flushing when writing</param>
        /// <param name="basePath">The path to namespace this index in</param>
        /// <param name="encryptor">The encryptor to encryt any records being saved</param>
        /// <param name="fileBasedPath">If not null, will build lucene file cache at this location (instead of an in-memory one)</param>
        /// <param name="secsTillReaderRefresh">This is the amount of time to cache the reader before updating it</param>
        public LuceneIndex(ISecureStore store, string container, string basePath, Lazy<Task<IEncryptor>> encryptor, string fileBasedPath = null, double RAMSizeMb = 20, int secsTillReaderRefresh = 10)
        {
            encryptor = encryptor ?? new Lazy<Task<IEncryptor>>(() => Task.FromResult((IEncryptor)null));

            if (string.IsNullOrWhiteSpace(fileBasedPath))
            {
                _cacheDirectory = new RAMDirectory();
            }
            else
            {
                var path = IO.Path.Combine(fileBasedPath, container, basePath);
                _cacheDirectory = new MMapDirectory(new System.IO.DirectoryInfo(path));
            }
            
            _directory = new SecureStoreDirectory(_cacheDirectory, store, container, basePath, encryptor);
            _analyzer = new EnglishAnalyzer();
            _RAMSizeMb = RAMSizeMb;
        }

        /// <summary>
        /// Lower level constructor, put in your own cache, (lucene) directory and (lucene) analyzer
        /// </summary>
        /// <param name="directory">Lucene directory of your files</param>
        /// <param name="analyzer">Analyzer you want to use for your indexing/searching</param>
        /// <param name="RAMSizeMb">The max amount of memory to use before flushing when writing</param>
        /// <param name="secsTillReaderRefresh">This is the amount of time to cache the reader before updating it</param>
        public LuceneIndex(Directory directory, Analyzer analyzer, double RAMSizeMb = 20, int secsTillReaderRefresh = 10)
        {
            _directory = directory;
            _analyzer = analyzer;
            _RAMSizeMb = RAMSizeMb;
        }

        public Task WriteToIndex(IEnumerable<Document> documents, bool waitForGeneration = false)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            var writer = GetWriter();
            long gen = 0;
            foreach(var d in documents)
            {
                gen = writer.AddDocument(d);
            }

            if(waitForGeneration)
            {
                writer.WaitForGeneration(gen);

                // This is a bit hacky but no point waiting for the generation but then
                // not commiting it so that it can be used on other machines
                _writer._writer.Commit();
            }
            return Task.FromResult(0);
        }

        public async Task WriteToIndex(Func<NrtManager, Task> writeUsingIndex)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            var writer = GetWriter();
            await writeUsingIndex(writer).ConfigureAwait(false);
        }

        public IEnumerable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc, bool forceCheck = false)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            return SearchDocuments((s, a) => doSearchFunc(s), forceCheck);
        }

        public IEnumerable<Document> SearchDocuments(Func<IndexSearcher, Analyzer, TopDocs> doSearchFunc, bool forceCheck = false)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }
            
            if (_writer != null)
            {
                if (forceCheck)
                {
                    _writer.Manager.MaybeReopen(true);
                }
                using (var searcher = _writer.GetSearcher())
                {
                    var docs = doSearchFunc(searcher.Searcher, _analyzer);

                    foreach (var doc in docs.ScoreDocs)
                    {
                        yield return searcher.Searcher.Doc(doc.Doc);
                    }
                }
            }
            else
            {
                var reader = GetReader(forceCheck);
                
                var docs = doSearchFunc(reader, _analyzer);

                foreach (var doc in docs.ScoreDocs)
                {
                    yield return reader.Doc(doc.Doc);
                }
            }
        }

        public Task DeleteAll()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            var writer = GetWriter();
            writer.DeleteAll();
            return Task.FromResult(0);
        }

        private IndexSearcher GetReader(bool forceCheck)
        {
            var currentReader = _reader;
            if (currentReader == null)
            {
                try
                {
                    currentReader = new IndexSearcher(_directory, true);
                }
                catch (System.IO.FileNotFoundException)
                {
                    // this index doesn't exist... make it!
                    using (new L.IndexWriter(_directory, _analyzer, true, L.IndexWriter.MaxFieldLength.UNLIMITED)) { }
                    currentReader = new IndexSearcher(_directory, true);
                }

                _lastRead = DateTime.UtcNow;
            }
            else if (forceCheck || _lastRead.AddSeconds(5) < DateTime.UtcNow)
            {
                if (!currentReader.IndexReader.IsCurrent())
                {
                    // Note: we are specifically not disposing here so that any queries can finish on the old reader
                    currentReader = new IndexSearcher(_directory, true);
                }
                _lastRead = DateTime.UtcNow;
            }

            _reader = currentReader;
            return currentReader;
        }

        private NrtManager GetWriter()
        {
            if(_writer == null)
            {
                lock (_writerLock)
                {
                    if (_writer == null)
                    {
                        _writer = new SearcherContextInternal(_directory, _analyzer);
                    }
                }
            }

            // Once we have the writer that takes over!
            if(_reader != null)
            {
                // Note: we are specifically not disposing here so that any queries can finish on the old reader
                _reader = null;
            }

            return _writer.Manager;
        }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                if(_reader != null)
                {
                    _reader.Dispose();
                }
                if(_writer != null)
                {
                    _writer.Dispose();
                }
                _analyzer.Dispose();
                _directory.Dispose();

                if (_cacheDirectory != null)
                {
                    _cacheDirectory.Dispose();
                }
            }
        }

        /// <summary>
        /// Copied from (but slightly modified so we can access indexwriter)
        /// https://github.com/NielsKuhnel/NrtManager/blob/master/source/Lucene.Net.Contrib.Management/Client/SearcherContext.cs
        /// </summary>
        private class SearcherContextInternal : IDisposable
        {
            public NrtManager Manager { get; private set; }

            public PerFieldAnalyzerWrapper Analyzer { get; private set; }

            public readonly IndexWriter _writer;

            private readonly NrtManagerReopener _reopener;
            private readonly Committer _committer;

            private readonly List<Thread> _threads = new List<Thread>();

            public SearcherContextInternal(Directory dir, Analyzer defaultAnalyzer)
                : this(dir, defaultAnalyzer, TimeSpan.FromSeconds(.1), TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(10), TimeSpan.FromHours(2))
            {
            }

            public SearcherContextInternal(Directory dir, Analyzer defaultAnalyzer,
                            TimeSpan targetMinStale, TimeSpan targetMaxStale,
                            TimeSpan commitInterval, TimeSpan optimizeInterval)
            {
                Analyzer = new PerFieldAnalyzerWrapper(defaultAnalyzer);
                _writer = new IndexWriter(dir, Analyzer, IndexWriter.MaxFieldLength.UNLIMITED);

                Manager = new NrtManager(_writer);
                _reopener = new NrtManagerReopener(Manager, targetMaxStale, targetMinStale);
                _committer = new Committer(_writer, commitInterval, optimizeInterval);

                _threads.AddRange(new[] { new Thread(_reopener.Start), new Thread(_committer.Start) });

                foreach (var t in _threads)
                {
                    t.Start();
                }
            }

            public SearcherManager.IndexSearcherToken GetSearcher()
            {
                return Manager.GetSearcherManager().Acquire();
            }

            public void Dispose()
            {
                var disposeActions = new List<Action>
                {
                    _reopener.Dispose,
                    _committer.Dispose,
                    Manager.Dispose,
                    () => _writer.Dispose(true)
                };

                disposeActions.AddRange(_threads.Select(t => (Action)t.Join));

                DisposeUtil.PostponeExceptions(disposeActions.ToArray());
            }
        }
    }
}
