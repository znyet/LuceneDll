using System.Collections.Generic;
using System.IO;
using JiebaNet.Segmenter;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Core;
using Lucene.Net.Analysis.TokenAttributes;

namespace JiebaNetForLucene
{
    public class JiebaAnalyzer : Analyzer
    {
        public TokenizerMode mode;

        public JiebaAnalyzer()
            : base()
        {
            this.mode = TokenizerMode.Search;
        }

        public JiebaAnalyzer(TokenizerMode Mode)
            : base()
        {
            this.mode = Mode;
        }

        protected override TokenStreamComponents CreateComponents(string filedName, TextReader reader)
        {
            var tokenizer = new JiebaTokenizer(reader, mode);
            var tokenstream = (TokenStream)new LowerCaseFilter(Lucene.Net.Util.LuceneVersion.LUCENE_48, tokenizer);
            tokenstream.AddAttribute<ICharTermAttribute>();
            tokenstream.AddAttribute<IOffsetAttribute>();
            return new TokenStreamComponents(tokenizer, tokenstream);
        }
    }
}
