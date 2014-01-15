using Lucene.Net.Analysis;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Kalix.Leo.Lucene.Analysis
{
    public class EnglishAnalyzer : Analyzer
    {
        private static readonly string[] _stopWords = new[]
        {
            "0", "1", "2", "3", "4", "5", "6", "7", "8",
            "9", "000", "$", "£",
            "about", "after", "all", "also", "an", "and",
            "another", "any", "are", "as", "at", "be",
            "because", "been", "before", "being", "between",
            "both", "but", "by", "came", "can", "come",
            "could", "did", "do", "does", "each", "else",
            "for", "from", "get", "got", "has", "had",
            "he", "have", "her", "here", "him", "himself",
            "his", "how","if", "in", "into", "is", "it",
            "its", "just", "like", "make", "many", "me",
            "might", "more", "most", "much", "must", "my",
            "never", "now", "of", "on", "only", "or",
            "other", "our", "out", "over", "re", "said",
            "same", "see", "should", "since", "so", "some",
            "still", "such", "take", "than", "that", "the",
            "their", "them", "then", "there", "these",
            "they", "this", "those", "through", "to", "too",
            "under", "up", "use", "very", "want", "was",
            "way", "we", "well", "were", "what", "when",
            "where", "which", "while", "who", "will",
            "with", "would", "you", "your",
            "a", "b", "c", "d", "e", "f", "g", "h", "i",
            "j", "k", "l", "m", "n", "o", "p", "q", "r",
            "s", "t", "u", "v", "w", "x", "y", "z"
        };

        private readonly ISet<string> _words;

        public EnglishAnalyzer()
            : this(_stopWords)
        {

        }

        public EnglishAnalyzer(IEnumerable<string> stopWords)
        {
            _words = StopFilter.MakeStopSet(stopWords.ToArray());
        }

        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            return new PorterStemFilter(new ISOLatin1AccentFilter(new StopFilter(false, new LowerCaseFilter(new WhitespaceTokenizer(reader)), _words)));
        }
    }
}
