using Kalix.Leo.Encryption;
using Kalix.Leo.Lucene.Analysis;
using Kalix.Leo.Lucene.Store;
using Kalix.Leo.Storage;
using Lucene.Net.Analysis;
using Lucene.Net.Contrib.Management;
using Lucene.Net.Contrib.Management.Client;
using Lucene.Net.Documents;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using IO = System.IO;

namespace Kalix.Leo.Lucene
{
    public class LuceneIndex : IDisposable, ILuceneIndex
    {
        private readonly Directory _directory;
        private readonly Analyzer _analyzer;

        private readonly Directory _cacheDirectory;

        private readonly SearcherContext _searcher;

        private readonly double _RAMSizeMb;
        private bool _isDisposed;

        private static readonly string _baseDirectory = IO.Path.Combine(IO.Path.GetTempPath(), "LeoLuceneIndexes");

        /// <summary>
        /// Create a lucene index over the top of a secure store, using an encrypted file cache and english analyzer
        /// Only one instance should be used for both indexing and searching (on any number of threads) for best results
        /// </summary>
        /// <param name="store">Store to have the Indexer on top of</param>
        /// <param name="container">Container to put the index</param>
        /// <param name="RAMSizeMb">The max amount of memory to use before flushing when writing</param>
        /// <param name="basePath">The path to namespace this index in</param>
        /// <param name="encryptor">The encryptor to encryt any records being saved</param>
        /// <param name="secsTillReaderRefresh">This is the amount of time to cache the reader before updating it</param>
        public LuceneIndex(ISecureStore store, string container, string basePath, IEncryptor encryptor, double RAMSizeMb = 20, int secsTillReaderRefresh = 10)
        {
            //var path = IO.Path.Combine(_baseDirectory, container, basePath);
            //_cacheDirectory = FSDirectory.Open(path);
            _cacheDirectory = new RAMDirectory();
            _directory = new SecureStoreDirectory(_cacheDirectory, store, container, basePath, encryptor);
            _analyzer = new EnglishAnalyzer();
            _RAMSizeMb = RAMSizeMb;

            _searcher = new SearcherContext(_directory, _analyzer);
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

            _searcher = new SearcherContext(_directory, _analyzer);
        }

        public async Task WriteToIndex(IObservable<Document> documents, bool waitForGeneration = false)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            var writer = _searcher.Manager;
            long gen = 0;
            await documents
                .Do((d) => gen = writer.AddDocument(d))
                .LastOrDefaultAsync();

            if(waitForGeneration)
            {
                writer.WaitForGeneration(gen);
            }
        }

        public async Task WriteToIndex(Func<NrtManager, Task> writeUsingIndex)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            await writeUsingIndex(_searcher.Manager).ConfigureAwait(false);
        }

        public IObservable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            return SearchDocuments((s, a) => doSearchFunc(s));
        }

        public IObservable<Document> SearchDocuments(Func<IndexSearcher, Analyzer, TopDocs> doSearchFunc)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            return Observable.Create<Document>(obs =>
            {
                var cts = new CancellationTokenSource();
                var token = cts.Token;

                Task.Run(() =>
                {
                    using (var searcher = _searcher.GetSearcher())
                    {
                        var docs = doSearchFunc(searcher.Searcher, _analyzer);

                        foreach (var doc in docs.ScoreDocs)
                        {
                            obs.OnNext(searcher.Searcher.Doc(doc.Doc));
                            token.ThrowIfCancellationRequested();
                        }

                        obs.OnCompleted();
                    }
                }, token);

                return cts;
            });
        }

        public Task DeleteAll()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            _searcher.Manager.DeleteAll();
            return Task.FromResult(0);
        }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                _searcher.Dispose();
                _analyzer.Dispose();
                _directory.Dispose();

                if (_cacheDirectory != null)
                {
                    _cacheDirectory.Dispose();
                }
            }
        }
    }
}
