using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using FastMember;
using System.Threading;
using System.ComponentModel;
using Lucene.Net.Store;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;


[AttributeUsage(AttributeTargets.Property)] //不存储数据
public class IndexNoStore : Attribute { }

[AttributeUsage(AttributeTargets.Property)] //索引且分词
public class IndexSplitWord : Attribute { }

[AttributeUsage(AttributeTargets.Property)] //不索引
public class IndexNo : Attribute { }

[AttributeUsage(AttributeTargets.Property)] //转化成数值类型
public class IndexNumeric : Attribute { }


[AttributeUsage(AttributeTargets.Property)] //权值
public class IndexBoost1 : Attribute { }

[AttributeUsage(AttributeTargets.Property)] //权值
public class IndexBoost2 : Attribute { }

[AttributeUsage(AttributeTargets.Property)] //权值
public class IndexBoost3 : Attribute { }

[AttributeUsage(AttributeTargets.Property)] //权值
public class IndexBoost4 : Attribute { }

[AttributeUsage(AttributeTargets.Property)] //权值
public class IndexBoost5 : Attribute { }

/// <summary>
/// Lucene自定义工具类
/// </summary>
public class LuceneTool
{

    #region 创建Directory IndexReader IndexWriter IndexSearcher

    //创建Directory
    public static Directory CreateDirectory(string indexName, LockFactory factory = null)
    {
        if (!indexName.Contains(":"))
            indexName = AppDomain.CurrentDomain.BaseDirectory + "LuceneDb/" + indexName;
        Directory directory = FSDirectory.Open(indexName);
        return directory;
    }

    //创建IndexReader
    public static IndexReader CreateIndexReader(Directory directory, bool readOnly = false)
    {
        IndexReader reader = IndexReader.Open(directory, readOnly);
        return reader;
    }

    //创建IndexWriter
    public static IndexWriter CreateIndexWriter(Directory directory, Analyzer analyzer)
    {
        bool isExist = IndexReader.IndexExists(directory);
        if (isExist)
        {
            if (IndexWriter.IsLocked(directory))
            {
                IndexWriter.Unlock(directory);
            }
        }
        IndexWriter writer = new IndexWriter(directory, analyzer, !isExist, IndexWriter.MaxFieldLength.UNLIMITED);
        return writer;
    }

    //创建IndexSearcher
    public static IndexSearcher CreateIndexSearcher(Directory directory)
    {
        IndexSearcher searcher = new IndexSearcher(directory, true);
        return searcher;
    }

    //创建IndexSearcher
    public static IndexSearcher CreateIndexSearcher(IndexReader reader)
    {
        IndexSearcher searcher = new IndexSearcher(reader);
        return searcher;
    }

    #endregion

    #region 创建全局字典semaphoreDic directoryDic indexReaderDic IndexWriter IndexSearcher 用于多线程读写

    //全局分词器
    public static Analyzer analyzerGloabal = new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30);

    //全局信号量,用于多线程一个索引只允许一个写操作
    private static Dictionary<string, SemaphoreSlim> semaphoreDic = new Dictionary<string, SemaphoreSlim>();
    private static object semaphoreLocker = new object();
    public static SemaphoreSlim GetSemaphore(string indexName)
    {
        if (!semaphoreDic.Keys.Any(a => a == indexName))
        {
            lock (semaphoreLocker)
            {
                if (!semaphoreDic.Keys.Any(a => a == indexName))
                {
                    SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1);
                    semaphoreDic.Add(indexName, semaphoreSlim);
                }
            }
        }
        return semaphoreDic[indexName];
    }

    //全局Directory
    private static Dictionary<string, Directory> directoryDic = new Dictionary<string, Directory>();
    private static object directoryLocker = new object();
    public static Directory GetDirectory(string indexName)
    {
        if (!directoryDic.Keys.Any(a => a == indexName))
        {
            lock (directoryLocker)
            {
                if (!directoryDic.Keys.Any(a => a == indexName))
                {
                    Directory directory = CreateDirectory(indexName);
                    directoryDic.Add(indexName, directory);
                }
            }
        }
        return directoryDic[indexName];
    }

    //全局IndexWriter
    public static Dictionary<string, IndexWriter> indexWriterDic = new Dictionary<string, IndexWriter>();
    private static object indexWriterLocker = new object();
    public static IndexWriter GetIndexWriter(string indexName)
    {
        if (!indexWriterDic.Keys.Any(a => a == indexName))
        {
            lock (indexWriterLocker)
            {
                if (!indexWriterDic.Keys.Any(a => a == indexName))
                {
                    Directory directory = GetDirectory(indexName);
                    IndexWriter writer = CreateIndexWriter(directory, analyzerGloabal);
                    indexWriterDic.Add(indexName, writer);

                }
            }
        }
        return indexWriterDic[indexName];
    }

    //全局IndexReader(ReadOnly只读模式)
    public static Dictionary<string, IndexReader> indexReaderDic = new Dictionary<string, IndexReader>();
    //indexReader收集器，用于回收旧的indexReader
    public static Dictionary<string, List<IndexReader>> indexReaderCollectorDic = new Dictionary<string, List<IndexReader>>();
    private static object indexReaderLocker = new object();
    private static object indexReaderLocker2 = new object();
    public static IndexReader GetIndexReader(string indexName)
    {
        //没有在字典中先加入字典
        if (!indexReaderDic.Keys.Any(a => a == indexName))
        {
            lock (indexReaderLocker)
            {
                if (!indexReaderDic.Keys.Any(a => a == indexName))
                {
                    Directory directory = GetDirectory(indexName);
                    IndexReader reader = CreateIndexReader(directory, true);
                    indexReaderDic.Add(indexName, reader);
                }
            }
        }

        //若是已经在字典，但是不是最新的，则进行更新
        if (!indexReaderDic[indexName].IsCurrent())
        {
            lock (indexReaderLocker2)
            {
                if (!indexReaderDic[indexName].IsCurrent())
                {
                    IndexReader newReader = indexReaderDic[indexName].Reopen(true);
                    if (newReader != null)
                    {
                        IndexReader oldReader = indexReaderDic[indexName];
                        if (!indexReaderCollectorDic.Keys.Any(a => a == indexName))
                            indexReaderCollectorDic.Add(indexName, new List<IndexReader>());
                        indexReaderCollectorDic[indexName].Add(oldReader); //把旧的IndexReader放入回收器
                        indexReaderDic[indexName] = newReader;
                    }
                }
            }

        }
        return indexReaderDic[indexName];
    }

    //回收旧的IndexReader
    public static void RecoveryOldIndexReader(string indexName)
    {
        if (LuceneTool.indexReaderCollectorDic.Keys.Any(a => a == indexName) && LuceneTool.indexReaderCollectorDic[indexName].Any()) //释放回收器里面的IndexReader
        {
            foreach (var item in LuceneTool.indexReaderCollectorDic[indexName])
            {
                item.Dispose();
            }
            LuceneTool.indexReaderCollectorDic[indexName].Clear();//移除所有元素
        }
    }

    //获取IndexSearcher，非全局。
    public static IndexSearcher GetIndexSearcher(string indexName)
    {
        return new IndexSearcher(GetIndexReader(indexName));
    }

    #endregion

    #region 分词，高亮等一些方法

    //分词
    public static IEnumerable<string> SplitWord(string text, Analyzer analyzer)
    {
        TokenStream tokenStream = analyzer.TokenStream("", new System.IO.StringReader(text));

        Lucene.Net.Analysis.Tokenattributes.ITermAttribute ita;

        bool hasNext = tokenStream.IncrementToken();

        while (hasNext)
        {
            ita = tokenStream.GetAttribute<Lucene.Net.Analysis.Tokenattributes.ITermAttribute>();
            yield return ita.Term;
            hasNext = tokenStream.IncrementToken();
        }
        yield break;
    }

    //分词去除重复
    public static IEnumerable<string> SplitWordDistinct(string text, Analyzer analyzer, int sort = 0)
    {
        if (sort == 0)
            return SplitWord(text, analyzer).Distinct();
        else if (sort == 1)
            return SplitWord(text, analyzer).Distinct().OrderBy(d => d.Length);
        else
            return SplitWord(text, analyzer).Distinct().OrderByDescending(d => d.Length);
    }

    //创建Or的Query
    public static Query CreateOrQuery(IEnumerable<string> words, params string[] fields)
    {
        BooleanQuery query = new BooleanQuery();
        foreach (var word in words)
        {
            foreach (var field in fields)
            {
                Term term = new Term(field, word);
                TermQuery termQuery = new TermQuery(term);
                query.Add(termQuery, Occur.SHOULD);
            }
        }

        return query;
    }

    //根据keyword高亮字符串
    public static string HighLightKeyword(string str, string keyword, string highlightBegin = null, string highlightEnd = null)
    {
        int index;
        int startIndex = 0;
        if (highlightBegin == null)
            highlightBegin = "<em style=\"color:#c00;font-style:normal\">";
        if (highlightEnd == null)
            highlightEnd = "</em>";
        int length = highlightBegin.Length + keyword.Length;
        int lengthHighlight = length + highlightEnd.Length;

        while ((index = str.IndexOf(keyword, startIndex, StringComparison.OrdinalIgnoreCase)) > -1)
        {
            str = str.Insert(index, highlightBegin).Insert(index + length, highlightEnd);
            startIndex = index + lengthHighlight;
        }

        return str;
    }

    //高亮字符串
    public static string GethighLightString(string content, IEnumerable<string> words, string highlightBegin = null, string highlightEnd = null)
    {
        if (string.IsNullOrEmpty(content) || !words.Any())
            return content;
        foreach (var word in words)
        {
            content = HighLightKeyword(content, word, highlightBegin, highlightEnd);
        }

        return content;
    }

    #endregion

    #region 类型转换 ，文档创建

    //ToString
    public static string DateTimeToString(DateTime value)
    {
        return DateTools.DateToString(value, DateTools.Resolution.MILLISECOND);
    }

    public static string IntToString(int value)
    {
        return Lucene.Net.Util.NumericUtils.IntToPrefixCoded(value);
    }

    public static string LongToString(long value)
    {
        return Lucene.Net.Util.NumericUtils.LongToPrefixCoded(value);
    }

    public static string FloatToString(float value)
    {
        return Lucene.Net.Util.NumericUtils.FloatToPrefixCoded(value);
    }

    public static string DoubleToString(double value)
    {
        return Lucene.Net.Util.NumericUtils.DoubleToPrefixCoded(value);
    }

    //ToObject
    public static DateTime StringToDateTime(string value)
    {
        return DateTools.StringToDate(value);
    }

    public static int StringToInt(string value)
    {
        return Lucene.Net.Util.NumericUtils.PrefixCodedToInt(value);
    }

    public static long StringToLong(string value)
    {
        return Lucene.Net.Util.NumericUtils.PrefixCodedToLong(value);
    }

    public static float StringToFloat(string value)
    {
        return Lucene.Net.Util.NumericUtils.PrefixCodedToFloat(value);
    }

    public static double StringToDouble(string value)
    {
        return Lucene.Net.Util.NumericUtils.PrefixCodedToDouble(value);
    }

    //创建一个文档
    public static Document CreateDoc<T>(T model)
    {
        Document doc = new Document();
        var type = typeof(T);
        var access = TypeAccessor.Create(type);

        foreach (var item in access.GetMembers())
        {
            var store = Field.Store.YES; //默认存储数据
            var index = Field.Index.NOT_ANALYZED; //默认索引不分词
            float boost = 1f;

            if (item.IsDefined(typeof(IndexNoStore)))
                store = Field.Store.NO;

            if (item.IsDefined(typeof(IndexSplitWord)))
                index = Field.Index.ANALYZED;
            else if (item.IsDefined(typeof(IndexNo)))
                index = Field.Index.NO;

            if (item.IsDefined(typeof(IndexBoost1)))
                boost = 2f;
            else if (item.IsDefined(typeof(IndexBoost2)))
                boost = 3f;
            else if (item.IsDefined(typeof(IndexBoost3)))
                boost = 4f;
            else if (item.IsDefined(typeof(IndexBoost4)))
                boost = 5f;
            else if (item.IsDefined(typeof(IndexBoost5)))
                boost = 6f;

            var mType = item.Type;
            string name = item.Name;
            object objvalue = access[model, name];
            string value;

            if (objvalue != null)
            {
                //DateTools 
                //Lucene.Net.Util.NumericUtils

                if (mType == typeof(string) || mType == typeof(bool))
                    value = objvalue.ToString();
                else if (mType == typeof(DateTime))
                    value = DateTimeToString((DateTime)objvalue);
                else
                {
                    if (item.IsDefined(typeof(IndexNumeric)))
                    {
                        if (mType == typeof(int))
                            value = IntToString((int)objvalue);
                        else if (mType == typeof(long))
                            value = LongToString((long)objvalue);
                        else if (mType == typeof(float))
                            value = FloatToString((float)objvalue);
                        else if (mType == typeof(double))
                            value = DoubleToString((double)objvalue);
                        else
                            value = objvalue.ToString();
                    }
                    else
                    {
                        value = objvalue.ToString();
                    }
                }
            }
            else
            {
                if (name == "Id")
                    value = Guid.NewGuid().ToString("N");
                else
                    value = "";
            }


            Field field = new Field(name, value, store, index);
            field.Boost = boost;

            doc.Add(field);

            if (name.ToLower() == "boost") //文档权重
            {
                doc.Boost = Convert.ToSingle(objvalue);
            }
        }

        return doc;
    }

    //创建model
    public static T CreateModel<T>(Document doc)
        where T : class,new()
    {
        var type = typeof(T);
        T model = new T();
        var fields = doc.GetFields();
        var access = TypeAccessor.Create(type);

        var members = access.GetMembers();
        foreach (var member in members)
        {
            string name = member.Name;
            var mType = member.Type;
            var field = fields.FirstOrDefault(f => f.Name == name);
            if (field != null)
            {
                string value = field.StringValue;
                if (mType == typeof(string))
                    access[model, name] = value;
                else
                {
                    if (value != null && value != "")
                    {
                        if (member.IsDefined(typeof(IndexNumeric)))
                        {
                            if (mType == typeof(int))
                                access[model, name] = StringToInt(value);
                            else if (mType == typeof(long))
                                access[model, name] = StringToLong(value);
                            else if (mType == typeof(float))
                                access[model, name] = StringToFloat(value);
                            else if (mType == typeof(double))
                                access[model, name] = StringToDouble(value);

                            else
                                access[model, name] = ConvertHelper.HackType(value, mType);
                        }
                        else if (mType == typeof(DateTime))
                            access[model, name] = StringToDateTime(value);
                        else
                            access[model, name] = ConvertHelper.HackType(value, mType);
                    }
                }
            }
        }

        return model;
    }


    #endregion

    #region 读操作

    //读取doc文档
    public static IEnumerable<T> ReadModels<T>(Searcher indexSearcher, TopDocs topDocs, string returnFields = null)
        where T : class,new()
    {
        if (topDocs.TotalHits == 0)
            yield break;
        foreach (var scoreDoc in topDocs.ScoreDocs)
        {
            Document doc;
            if (returnFields == null)
                doc = indexSearcher.Doc(scoreDoc.Doc);
            else
            {
                string[] fieldArr = returnFields.Split(',');
                MapFieldSelector field = new MapFieldSelector(fieldArr);//指定返回列
                doc = indexSearcher.Doc(scoreDoc.Doc, field);
            }
            yield return LuceneTool.CreateModel<T>(doc);
        }
    }

    //读取doc文档
    public static IEnumerable<T> ReadModels<T>(Searcher indexSearcher, TopDocs topDocs, int skip, string returnFields = null)
        where T : class,new()
    {
        if (skip >= topDocs.TotalHits)
            yield break;

        for (int i = skip; i < topDocs.ScoreDocs.Length; i++)
        {
            Document doc;
            if (returnFields == null)
                doc = indexSearcher.Doc(topDocs.ScoreDocs[i].Doc);
            else
            {
                string[] fieldArr = returnFields.Split(',');
                MapFieldSelector field = new MapFieldSelector(fieldArr);//指定返回列
                doc = indexSearcher.Doc(topDocs.ScoreDocs[i].Doc, field);
            }
            yield return LuceneTool.CreateModel<T>(doc);
        }
    }

    //根据id查询数据
    public static T GetById<T>(Searcher indexSearcher, object id, string returnFields = null)
        where T : class,new()
    {
        Term t = new Term("Id", id.ToString());
        Query q = new TermQuery(t);
        TopDocs topdocs = indexSearcher.Search(q, 1);
        IEnumerable<T> data = ReadModels<T>(indexSearcher, topdocs, returnFields);
        return data.FirstOrDefault();
    }

    //根据ids查询数据
    public static IEnumerable<T> GetByIds<T>(Searcher indexSearcher, IEnumerable<object> ids, string returnFields = null)
        where T : class,new()
    {
        int count = ids.Count();
        if (count == 0)
            return Enumerable.Empty<T>();

        BooleanQuery q = new BooleanQuery();
        foreach (var id in ids)
        {
            Term t = new Term("Id", id.ToString());
            TermQuery termQuery = new TermQuery(t);
            q.Add(termQuery, Occur.SHOULD);
        }
        TopDocs topdocs = indexSearcher.Search(q, int.MaxValue);
        return ReadModels<T>(indexSearcher, topdocs, returnFields);
    }

    //根据字段的值查询
    public static T GetByFieldFirst<T>(Searcher indexSearcher, string fieldName, string value, string returnFields = null)
        where T : class,new()
    {
        Term t = new Term(fieldName, value);
        Query q = new TermQuery(t);
        TopDocs topdocs = indexSearcher.Search(q, 1);
        IEnumerable<T> data = ReadModels<T>(indexSearcher, topdocs, returnFields);
        return data.FirstOrDefault();
    }

    //根据字段的值查询
    public static IEnumerable<T> GetByField<T>(Searcher indexSearcher, string fieldName, string value, string returnFields = null)
        where T : class,new()
    {
        Term t = new Term(fieldName, value);
        Query q = new TermQuery(t);
        TopDocs topdocs = indexSearcher.Search(q, int.MaxValue);
        return ReadModels<T>(indexSearcher, topdocs, returnFields);
    }

    //根据字段的多值查询
    public static IEnumerable<T> GetByFieldIn<T>(Searcher indexSearcher, string fieldName, IEnumerable<string> ids, string returnFields = null)
        where T : class,new()
    {
        int count = ids.Count();
        if (count == 0)
            return Enumerable.Empty<T>();

        BooleanQuery q = new BooleanQuery();
        foreach (var id in ids)
        {
            Term t = new Term(fieldName, id);
            TermQuery termQuery = new TermQuery(t);
            q.Add(termQuery, Occur.SHOULD);
        }
        TopDocs topdocs = indexSearcher.Search(q, int.MaxValue);
        return ReadModels<T>(indexSearcher, topdocs, returnFields);
    }

    //根据查询条件获取数据
    public static IEnumerable<T> GetByQuery<T>(Searcher indexSearcher, Query q, string returnFields = null, Sort sort = null, Filter filter = null, int maxReturnNum = int.MaxValue)
        where T : class,new()
    {
        if (q == null)
            q = new MatchAllDocsQuery();
        TopDocs topdocs;
        if (sort == null && filter == null)
            topdocs = indexSearcher.Search(q, maxReturnNum);
        else if (sort == null && filter != null)
            topdocs = indexSearcher.Search(q, filter, maxReturnNum);
        else
            topdocs = indexSearcher.Search(q, filter, maxReturnNum, sort);
        return ReadModels<T>(indexSearcher, topdocs, returnFields);
    }

    //获取当前文档总数
    public static int GetTotal(Searcher indexSearcher, Query q = null)
    {
        if (q == null)
            q = new MatchAllDocsQuery();
        TopDocs topdocs = indexSearcher.Search(q, 1);
        return topdocs.TotalHits;
    }

    //获取所有数据
    public static IEnumerable<T> GetAll<T>(Searcher indexSearcher, string returnFields = null, Sort sort = null)
        where T : class,new()
    {
        return GetByQuery<T>(indexSearcher, null, returnFields, sort: sort);
    }

    //查询分页数据 Sort sort = new Sort(new SortField(FieldName, SortField.DOC, false))
    public static IEnumerable<T> GetByPage<T>(Searcher indexSearcher, int pageIndex, int pageSize, out int total, string returnFields = null, Query q = null, Sort sort = null, Filter filter = null, int maxReturnNum = int.MaxValue)
        where T : class,new()
    {
        if (pageIndex <= 0)
            pageIndex = 1;
        int skip = (pageIndex - 1) * pageSize;
        int end = pageIndex * pageSize;

        if (end > maxReturnNum)
        {
            total = 0;
            return Enumerable.Empty<T>();
        }

        if (q == null)
            q = new MatchAllDocsQuery();

        TopDocs topdocs;
        if (sort == null && filter == null)
            topdocs = indexSearcher.Search(q, end);
        else if (sort == null && filter != null)
            topdocs = indexSearcher.Search(q, filter, end);
        else
            topdocs = indexSearcher.Search(q, filter, end, sort);

        total = topdocs.TotalHits;
        return ReadModels<T>(indexSearcher, topdocs, skip, returnFields);
    }

    #endregion

}

/// <summary>
/// LuceneConnection用于单个索引的读和写
/// </summary>
public class LuceneConnection : IDisposable
{
    public string indexName { get; set; }
    private IndexSearcher _indexSearcher;
    private IndexSearcher indexSearcher
    {
        get
        {
            if (_indexSearcher == null)
                _indexSearcher = LuceneTool.GetIndexSearcher(indexName);
            return _indexSearcher;
        }
    }

    //构造函数，传入索引名称
    public LuceneConnection(string indexName)
    {
        this.indexName = indexName;
    }

    #region 写操作(一般的增删改调用SaveChanges)

    //公共提交方法
    private void Save()
    {
        try
        {
            LuceneTool.GetSemaphore(indexName).Wait();
            LuceneTool.RecoveryOldIndexReader(indexName); //回收旧的IndexReader
            LuceneTool.GetIndexWriter(indexName).Commit();
        }
        finally
        {
            LuceneTool.GetSemaphore(indexName).Release();
        }
    }

    //提交保存数据
    public void SaveChanges()
    {
        Save();
    }

    //异步提交保存数据
    public void SaveChangesAsync()
    {
        new Thread(() =>
        {
            Save();
        }) { IsBackground = true }.Start();
    }

    //加入其它索引
    public void AppendIndexs(params string[] indexs)
    {
        foreach (var item in indexs)
        {
            Directory dir = LuceneTool.GetDirectory(item);
            LuceneTool.GetIndexWriter(indexName).AddIndexesNoOptimize(dir);
        }
    }

    //添加索引
    public void Insert<T>(T model)
    {
        var doc = LuceneTool.CreateDoc(model);
        LuceneTool.GetIndexWriter(indexName).AddDocument(doc);
    }

    //批量添加索引
    public void InsertMany<T>(IEnumerable<T> list)
    {
        foreach (var item in list)
        {
            var doc = LuceneTool.CreateDoc(item);
            LuceneTool.GetIndexWriter(indexName).AddDocument(doc);
        }
    }

    //根据id添加或者修改索引
    public void InsertOrUpdate<T>(T model)
    {
        var doc = LuceneTool.CreateDoc(model);
        var term = new Term("Id", doc.Get("Id"));
        LuceneTool.GetIndexWriter(indexName).UpdateDocument(term, doc);
    }

    //批量根据id添加或者修改索引
    public void InsertOrUpdateMany<T>(IEnumerable<T> list)
    {
        foreach (var item in list)
        {
            var doc = LuceneTool.CreateDoc(item);
            var term = new Term("Id", doc.Get("Id"));
            LuceneTool.GetIndexWriter(indexName).UpdateDocument(term, doc);
        }
    }

    //删除所有数据
    public void DeleteAll()
    {
        LuceneTool.GetIndexWriter(indexName).DeleteAll();
    }

    //根据id删除数据
    public void DeleteById(object id)
    {
        Term term = new Term("Id", id.ToString());
        LuceneTool.GetIndexWriter(indexName).DeleteDocuments(term);
    }

    //根据ids批量发删除数据
    public void DeleteByIds(IEnumerable<object> ids)
    {
        foreach (var id in ids)
        {
            Term term = new Term("Id", id.ToString());
            LuceneTool.GetIndexWriter(indexName).DeleteDocuments(term);
        }
    }

    //根据Query删除
    public void Delete(Query q)
    {
        LuceneTool.GetIndexWriter(indexName).DeleteDocuments(q);
    }

    //根据Querys删除
    public void Delete(params Query[] queries)
    {
        LuceneTool.GetIndexWriter(indexName).DeleteDocuments(queries);
    }

    //根据Term删除
    public void Delete(Term term)
    {
        LuceneTool.GetIndexWriter(indexName).DeleteDocuments(term);
    }

    //根据Terms删除
    public void Delete(params Term[] terms)
    {
        LuceneTool.GetIndexWriter(indexName).DeleteDocuments(terms);
    }

    //优化索引
    public void Optimize()
    {
        LuceneTool.GetIndexWriter(indexName).Optimize();
    }

    #endregion

    #region 读操作

    //根据id获取一条数据
    public T GetById<T>(object id, string returnFields = null) where T : class, new()
    {
        return LuceneTool.GetById<T>(indexSearcher, id, returnFields);
    }

    //根据ids获取数据
    public IEnumerable<T> GetByIds<T>(IEnumerable<object> ids, string returnFields = null) where T : class, new()
    {
        return LuceneTool.GetByIds<T>(indexSearcher, ids, returnFields);
    }

    //根据字段获取第一条数据
    public T GetByFieldFirst<T>(string fieldName, string value, string returnFields = null) where T : class, new()
    {
        return LuceneTool.GetByFieldFirst<T>(indexSearcher, fieldName, value, returnFields);
    }

    //根据字段获取数据
    public IEnumerable<T> GetByField<T>(string fieldName, string value, string returnFields = null) where T : class, new()
    {
        return LuceneTool.GetByField<T>(indexSearcher, fieldName, value, returnFields);
    }

    //根据字段ids获取数据
    public IEnumerable<T> GetByFieldIn<T>(string fieldName, IEnumerable<string> ids, string returnFields = null) where T : class, new()
    {
        return LuceneTool.GetByFieldIn<T>(indexSearcher, fieldName, ids, returnFields);
    }

    //根据Query获取数据
    public IEnumerable<T> GetByQuery<T>(Query q, string returnFields = null, Sort sort = null, Filter filter = null, int maxReturnNum = int.MaxValue) where T : class, new()
    {
        return LuceneTool.GetByQuery<T>(indexSearcher, q, returnFields, sort, filter, maxReturnNum);
    }

    //获取总数
    public int GetTotal(Query q = null)
    {
        return LuceneTool.GetTotal(indexSearcher, q);
    }

    //获取所有数据
    public IEnumerable<T> GetAll<T>(string returnFields = null, Sort sort = null)
        where T : class,new()
    {
        return LuceneTool.GetAll<T>(indexSearcher, returnFields, sort);
    }

    //查询分页数据 Sort sort = new Sort(new SortField(FieldName, SortField.DOC, false))
    public IEnumerable<T> GetByPage<T>(int pageIndex, int pageSize, out int total, string returnFields = null, Query q = null, Sort sort = null, Filter filter = null, int maxReturnNum = int.MaxValue) where T : class, new()
    {
        return LuceneTool.GetByPage<T>(indexSearcher, pageIndex, pageSize, out total, returnFields, q, sort, filter, maxReturnNum);
    }

    #endregion

    #region 释放资源

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private bool m_disposed;

    protected virtual void Dispose(bool disposing)
    {
        if (!m_disposed)
        {
            if (disposing)
            {
                // Release managed resources
            }

            // Release unmanaged resources

            if (_indexSearcher != null)
                _indexSearcher.Dispose();

            m_disposed = true;
        }
    }

    ~LuceneConnection()
    {
        Dispose(false);
    }


    #endregion
}

//用于多个索引的读
public class LuceneMultiConnection : IDisposable
{
    IndexReader[] readers;
    MultiReader multiReader;
    IndexSearcher indexSearcher;

    public LuceneMultiConnection(params string[] indexNames)
    {
        readers = new IndexReader[indexNames.Length];

        for (int i = 0; i < indexNames.Length; i++)
        {
            readers[i] = LuceneTool.GetIndexReader(indexNames[i]);
        }
        multiReader = new MultiReader(readers, false); //不关闭Readers
        indexSearcher = new IndexSearcher(multiReader);
    }

    public T GetById<T>(object id, string returnFields = null) where T : class, new()
    {
        return LuceneTool.GetById<T>(indexSearcher, id, returnFields);
    }

    public IEnumerable<T> GetByIds<T>(IEnumerable<object> ids, string returnFields = null) where T : class, new()
    {
        return LuceneTool.GetByIds<T>(indexSearcher, ids, returnFields);
    }

    public T GetByFieldFirst<T>(string fieldName, string value, string returnFields = null) where T : class, new()
    {
        return LuceneTool.GetByFieldFirst<T>(indexSearcher, fieldName, value, returnFields);
    }

    public IEnumerable<T> GetByField<T>(string fieldName, string value, string returnFields = null) where T : class, new()
    {
        return LuceneTool.GetByField<T>(indexSearcher, fieldName, value, returnFields);
    }

    public IEnumerable<T> GetByFieldIn<T>(string fieldName, IEnumerable<string> ids, string returnFields = null) where T : class, new()
    {
        return LuceneTool.GetByFieldIn<T>(indexSearcher, fieldName, ids, returnFields);
    }

    public IEnumerable<T> GetByQuery<T>(Query q, string returnFields = null, Sort sort = null, Filter filter = null, int maxReturnNum = int.MaxValue) where T : class, new()
    {
        return LuceneTool.GetByQuery<T>(indexSearcher, q, returnFields, sort, filter, maxReturnNum);
    }

    public int GetTotal(Query q = null)
    {
        return LuceneTool.GetTotal(indexSearcher, q);
    }

    public IEnumerable<T> GetAll<T>(string returnFields = null, Sort sort = null)
        where T : class,new()
    {
        return LuceneTool.GetAll<T>(indexSearcher, returnFields, sort);
    }

    public IEnumerable<T> GetByPage<T>(int pageIndex, int pageSize, out int total, string returnFields = null, Query q = null, Sort sort = null, Filter filter = null, int maxReturnNum = int.MaxValue) where T : class, new()
    {
        return LuceneTool.GetByPage<T>(indexSearcher, pageIndex, pageSize, out total, returnFields, q, sort, filter, maxReturnNum);
    }

    #region 释放资源

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private bool m_disposed;

    protected virtual void Dispose(bool disposing)
    {
        if (!m_disposed)
        {
            if (disposing)
            {
                // Release managed resources
            }
            // Release unmanaged resources

            if (indexSearcher != null)
                indexSearcher.Dispose();
            if (multiReader != null)
                multiReader.Dispose();
            readers = null;

            m_disposed = true;
        }
    }

    ~LuceneMultiConnection()
    {
        Dispose(false);
    }

    #endregion
}


