from tqdm import tqdm
from .. import config
from ..std import *
import csv
import re
tsvfieldnames = ['sidx','fid', 'qid', 'passages', 'question', 'option1', 'option2', 'option3', 'option4', 'label']
import logging

logger = logging.getLogger(__name__)

import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.store import SimpleFSDirectory, RAMDirectory
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.document import Document, StringField, TextField, Field
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.search.similarities import ClassicSimilarity, BM25Similarity
from org.apache.lucene.queryparser.classic import QueryParser

mySimilarity = BM25Similarity

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-llv',
                        default='INFO',
                        help='Logging level')
    parser.add_argument('-log',
                        default=None,
                        help='Output log file')
    parser.add_argument('-cmd',
                        required=True,
                        help='Command: {index_all} for indexing the raw documents, {test_search} for searching')
    parser.add_argument('-doc_path',
                        default=config.SSQA_WIKI,
                        help='/path/to/cosQA/doc')

    args = parser.parse_args()

    myLogFormat = '%(asctime)s ***%(levelname)s*** [%(name)s:%(lineno)s] - %(message)s'
    logging.basicConfig(level=str2llv(args.llv), format=myLogFormat, datefmt='%Y/%m/%d %H:%M:%S')
    if args.log:
        myhandlers = log_w(args.log)
        logger.addHandler(myhandlers)
        logger.log(100, ' '.join(sys.argv))
    else:
        logger.log(100, ' '.join(sys.argv))

    if args.cmd == 'index_all':
        index_all(args.doc_path)

    elif args.cmd == 'test_search':
        test_search()

    else:
        raise ValueError('Command {} not defined!'.format(args.cmd))
        
class SSQAIndexer:
    def __init__(self, lang):
        lucene.initVM()

        if lang == 'zh':
            logger.info("index directory:{}".format(config.IDX_SSQA))
            indexDir = SimpleFSDirectory(Paths.get(str(config.IDX_SSQA)))
            analyzer = SmartChineseAnalyzer()
        else:
            raise ValueError('lang should be "zh" or "en", {} is invalid!'.format(lang))
        writerConfig = IndexWriterConfig(analyzer)
        writerConfig.setSimilarity(mySimilarity())
        logger.debug('writer similarity func: {}'.format(writerConfig.getSimilarity()))
        writer = IndexWriter(indexDir, writerConfig)
        self.writer = writer

    def add(self, title, content):
        sent = Document()
        #sent.add(StringField("sid", sid, Field.Store.YES))
        sent.add(StringField("title", title, Field.Store.YES))
        sent.add(TextField("content", content, Field.Store.YES))
        self.writer.addDocument(sent)

    def close(self):
        self.writer.close()
class SSQA_S_Indexer:
    def __init__(self, indexDir, analyzer):
        lucene.initVM()
        logger.info("RAM index")
        writerConfig = IndexWriterConfig(analyzer)
        writerConfig.setSimilarity(mySimilarity())
        logger.debug('writer similarity func: {}'.format(writerConfig.getSimilarity()))
        writer = IndexWriter(indexDir, writerConfig)
        self.writer = writer
        
    def add(self, content):
        sent = Document()
        sent.add(TextField("content", content, Field.Store.YES))
        self.writer.addDocument(sent)
        
    def close(self):
        self.writer.close()
class SSQA_S_Searcher:
    def __init__(self,indexDir, analyzer):
        lucene.initVM()
        self.reader = DirectoryReader.open(indexDir)
        self.searcher = IndexSearcher(self.reader)
        self.searcher.setSimilarity(mySimilarity())
        self.analyzer = analyzer
        logger.debug("Search similarity func: {}". format(self.searcher.getSimilarity()))
    
    def search(self, query_text, top_n):
        query_text = query_text.strip()
        query = QueryParser("content", self.analyzer).parse(QueryParser.escape(query_text.strip()))
        scoreDocs = self.searcher.search(query, top_n).scoreDocs
        count = 0
        out_list = []
        for scoreDoc in tqdm(scoreDocs):
            docIndex = scoreDoc.doc
            doc = self.searcher.doc(docIndex)
            log_debug(doc, logger)
            log_debug(self.searcher.explain(query, docIndex), logger)
            
            out_list.append(doc['content'])
            count += 1
        logger.info("Added {} sentences".format(count))
        return out_list
    def close(self):
        self.reader.close()
        
def index_and_search_sentence(list_paragraph, question):
    ramDir = RAMDirectory()
    analyzer = SmartChineseAnalyzer()
    myIndexer = SSQA_S_Indexer(ramDir, analyzer)
    try:
        sent_num = 0
        logger.info("Start indexing sentences...")
        for paragraph in tqdm(list_paragraph):    
            sentences = re.split('(。|！|\!|\.|？|\?)',paragraph) 
            for sent in sentences:
                myIndexer.add(sent)
                sent_num += 1
        logger.info("Indexed {} sentences.".format(sent_num))
        myIndexer.close()
        mySearcher = SSQA_S_Searcher(ramDir, analyzer)
        ret_sents = mySearcher.search(question, 1)
        return ret_sents
        mySearcher.close()
    finally:
        myIndexer.close()
        mySearcher.close()
            
def index_all(doc_path):
    raw_fps = list_fps(doc_path, 'txt')
    logger.info("Read {} files".format(len(raw_fps)))

    # 	assert not os.path.exists(config.IDX_COS_EN), "{} already exists!".format(config.IDX_COS_EN)
    # 	assert not os.path.exists(config.IDX_COS_ZH), "{} already exists!".format(config.IDX_COS_ZH)

    new_dir(config.IDX_SSQA)
    myIndexer_zh = SSQAIndexer("zh")

    try:
        sent_num = 0
        logger.info("Start indexing...")
        for fp in tqdm(raw_fps):
            sents = open(fp, 'r')
            input_file = sents.readlines()
            i = 0
            while i < len(input_file):
                title = 'r'
                content = input_file[i]
                myIndexer_zh.add(title, content)
                i += 1
                sent_num += 1
        logger.info("Indexed {} docs.".format(sent_num))
        myIndexer_zh.close()

    finally:
        myIndexer_zh.close()

        
class SSQASearcher:
    def __init__(self, lang):
        lucene.initVM()

        if lang == 'zh':
            indexDir = SimpleFSDirectory(Paths.get(str(config.IDX_SSQA)))
            analyzer = SmartChineseAnalyzer()
        else:
            raise ValueError('lang should be "zh" or "en", {} is invalid!'.format(lang))

        self.reader = DirectoryReader.open(indexDir)
        self.searcher = IndexSearcher(self.reader)
        self.searcher.setSimilarity(mySimilarity())
        self.analyzer = analyzer
        logger.debug('search similarity func: {}'.format(self.searcher.getSimilarity()))

    def search(self, query_text, top_n=1):
        query_text = query_text.strip()
        query = QueryParser("content", self.analyzer).parse(QueryParser.escape(query_text.strip()))
#         query = QueryParser("content", self.analyzer).parse(query_text)
        scoreDocs = self.searcher.search(query, top_n).scoreDocs

        out_list = []
        out = []
        for scoreDoc in scoreDocs:
            docIndex = scoreDoc.doc
            doc = self.searcher.doc(docIndex)
            log_debug(doc, logger)
            log_debug(self.searcher.explain(query, docIndex), logger)

            out_list.append((doc['title'], doc['content'], scoreDoc.score))
            out.append(doc['content'])
        return out

    def close(self):
        self.reader.close()

    
def test_search():
    
    # instantiate a searcher, 'en' for English and 'zh' for Chinese
    mySearcher = SSQASearcher('zh')
    with open('lin_test.tsv', 'r') as inputf, open('searched_lin_test.tsv', 'w') as output:
        questions = csv.DictReader(inputf, delimiter = '\t')
        writer = csv.DictWriter(output, delimiter = '\t', fieldnames = tsvfieldnames)
        writer.writeheader()
        for q in questions:
            # query to be search
            test_query = q['question']
            ret_sents = mySearcher.search(query_text=test_query, top_n=1)
            #q['passages'] = ' '.join(index_and_search_sentence(ret_sents, test_query)).strip()
            q['passages'] = ret_sents[0].strip()
            writer.writerow(q)
    
    with open('lin_train.tsv', 'r') as inputf, open('searched_lin_train.tsv', 'w') as output:
        questions = csv.DictReader(inputf, delimiter = '\t')
        writer = csv.DictWriter(output, delimiter = '\t', fieldnames = tsvfieldnames)
        writer.writeheader()
        for q in questions:
            # query to be search
            test_query = q['question']
            ret_sents = mySearcher.search(query_text=test_query, top_n=1)
            #q['passages'] = ' '.join(index_and_search_sentence(ret_sents, test_query)).strip()
            q['passages'] = ret_sents[0].strip()
            writer.writerow(q)
    with open('lin_dev.tsv', 'r') as inputf, open('searched_lin_dev.tsv', 'w') as output:
        questions = csv.DictReader(inputf, delimiter = '\t')
        writer = csv.DictWriter(output, delimiter = '\t', fieldnames = tsvfieldnames)
        writer.writeheader()
        for q in questions:
            # query to be search
            test_query = q['question']
            ret_sents = mySearcher.search(query_text=test_query, top_n=1)
            #q['passages'] = ' '.join(index_and_search_sentence(ret_sents, test_query)).strip()
            q['passages'] = ret_sents[0].strip()
            writer.writerow(q)

    """
    search by calling searcher.search(query, top_n)
    return:
    A list of tuple(did, title_en, content, score)
    """

    

    # remember to close the searcher
    mySearcher.close()
    
if __name__ == "__main__":
    main()