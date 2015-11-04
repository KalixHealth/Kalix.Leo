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
using System.Collections.Generic;
using System.Threading.Tasks;
using IO = System.IO;
using L = Lucene.Net.Index;

namespace Kalix.Leo.Lucene
{
    public class LuceneIndex : IDisposable, ILuceneIndex
    {
        private readonly Directory _directory;
        private readonly Analyzer _analyzer;

        private readonly Directory _cacheDirectory;

        private IndexSearcher _reader;
        private DateTime _lastRead;

        private SearcherContext _writer;

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
            }
            return Task.FromResult(0);
        }

        public async Task WriteToIndex(Func<NrtManager, Task> writeUsingIndex)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            await writeUsingIndex(GetWriter()).ConfigureAwait(false);
        }

        public IEnumerable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            return SearchDocuments((s, a) => doSearchFunc(s));
        }

        public IEnumerable<Document> SearchDocuments(Func<IndexSearcher, Analyzer, TopDocs> doSearchFunc)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }
            
            if (_writer != null)
            {
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
                var reader = GetReader();
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

        private IndexSearcher GetReader()
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

            if (_lastRead < DateTime.UtcNow.AddSeconds(-10))
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
                _writer = new SearcherContext(_directory, _analyzer);
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
    }
}
