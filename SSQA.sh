# indexing
python -m nlu_IR.lucene_search.SSQA_s_search -cmd index_all -doc_path data/SSQA

# test search
python -m nlu_IR.lucene_search.SSQA_s_search -cmd test_search