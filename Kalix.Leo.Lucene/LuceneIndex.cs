﻿using Kalix.Leo.Encryption;
using Kalix.Leo.Lucene.Analysis;
using Kalix.Leo.Lucene.Store;
using Kalix.Leo.Storage;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene
{
    public class LuceneIndex : IDisposable, ILuceneIndex
    {
        private readonly Directory _directory;
        private readonly Analyzer _analyzer;
        private readonly int _readThrottleTimeMs;

        private double _RAMSizeMb;
        private bool _isDisposed;

        /// <summary>
        /// Create a lucene index over the top of a secure store, using an encrypted file cache and english analyzer
        /// Only one instance should be used for both indexing and searching (on any number of threads) for best results
        /// </summary>
        /// <param name="store">Store to have the Indexer on top of</param>
        /// <param name="container">Container to put the index</param>
        /// <param name="readIndexThottleMs">The time before the read index can be refreshed, defaults to five seconds</param>
        /// <param name="writeIndexThottleMs">The interval to wait before writes</param>
        /// <param name="RAMSizeMb">The max amount of memory to use before flushing when writing</param>
        public LuceneIndex(ISecureStore store, string container, string basePath, IEncryptor encryptor, int readIndexThottleMs = 5000, int writeIndexThottleMs = 5000, double RAMSizeMb = 20)
            : this(new SecureStoreDirectory(store, container, basePath, new EncryptedFileCache(), encryptor), new EnglishAnalyzer(), readIndexThottleMs, writeIndexThottleMs, RAMSizeMb)
        {
        }

        /// <summary>
        /// Lower level constructor, put in your own cache, (lucene) directory and (lucene) analyzer
        /// </summary>
        /// <param name="directory">Lucene directory of your files</param>
        /// <param name="analyzer">Analyzer you want to use for your indexing/searching</param>
        /// <param name="readIndexThottleMs">The time before the read index can be refreshed, defaults to five seconds</param>
        /// <param name="writeIndexThottleMs">The interval to wait before writes</param>
        /// <param name="RAMSizeMb">The max amount of memory to use before flushing when writing</param>
        public LuceneIndex(Directory directory, Analyzer analyzer, int readIndexThottleMs = 5000, int writeIndexThottleMs = 5000, double RAMSizeMb = 20)
        {
            _directory = directory;
            _analyzer = analyzer;
            _readThrottleTimeMs = readIndexThottleMs;
            _RAMSizeMb = RAMSizeMb;
        }

        public async Task WriteToIndex(IObservable<Document> documents)
        {
            using (var writer = Writer())
            {
                await documents
                    .Do(writer.AddDocument)
                    .LastOrDefaultAsync();

                writer.Commit();
            }
        }

        public async Task WriteToIndex(Action<IndexWriter> writeUsingIndex)
        {
            await Task.Run(() =>
            {
                using (var writer = Writer())
                {
                    writeUsingIndex(writer);
                    writer.Commit();
                }
            }).ConfigureAwait(false);
        }

        public IObservable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc)
        {
            return Observable.Create<Document>(obs =>
            {
                var cts = new CancellationTokenSource();
                var token = cts.Token;

                Task.Run(() =>
                {
                    try
                    {
                        using(var reader = Reader())
                        using (var searcher = new IndexSearcher(reader))
                        {
                            var docs = doSearchFunc(searcher);

                            foreach (var doc in docs.ScoreDocs)
                            {
                                obs.OnNext(searcher.Doc(doc.Doc));
                                token.ThrowIfCancellationRequested();
                            }
                        }

                        obs.OnCompleted();
                    }
                    catch (Exception e)
                    {
                        obs.OnError(e);
                    }
                }, token);

                return cts;
            });
        }

        public Task DeleteAll()
        {
            return Task.Run(() =>
            {
                using (var writer = Writer())
                {
                    writer.DeleteAll();
                    writer.Commit();
                }
            });
        }

        private IndexWriter Writer()
        {
            IndexWriter writer;
            try
            {
                writer = new IndexWriter(_directory, _analyzer, false, IndexWriter.MaxFieldLength.UNLIMITED);
            }
            catch(System.IO.FileNotFoundException e)
            {
                writer = new IndexWriter(_directory, _analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED);
            }
            writer.SetRAMBufferSizeMB(_RAMSizeMb);
            return writer;
        }

        private IndexReader Reader()
        {
            if(_isDisposed)
            {
                throw new ObjectDisposedException("Indexer");
            }

            var reader = IndexReader.Open(_directory, true);
            return reader;
        }

        public void Dispose()
        {
            if(!_isDisposed)
            {
                _isDisposed = true;
                _analyzer.Dispose();
                _directory.Dispose();
            }
        }
    }
}