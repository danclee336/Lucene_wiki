# indexing
python -m nlu_IR.lucene_search.textbook_search -cmd index_all -doc_path data/textbook

# test search
python -m nlu_IR.lucene_search.textbook_search -cmd test_search