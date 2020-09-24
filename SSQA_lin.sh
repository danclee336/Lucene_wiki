# indexing
python -m nlu_IR.lucene_search.SSQA_lin_search -cmd index_all -doc_path data/SSQA

# test search
python -m nlu_IR.lucene_search.SSQA_lin_search -cmd test_search
