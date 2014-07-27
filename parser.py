from org.apache.lucene import index, queries, search, util
from org.apache.pylucene.queryparser.classic import PythonQueryParser

class ChannellyParser(PythonQueryParser):

    def rewrite(self, query):
        "Return term or phrase query with corrected terms substituted."
        print query
        if search.TermQuery.instance_(query):
            term = search.TermQuery.cast_(query).term
            if term.field() == 'channel':
                print term.text()
                return search.TermQuery(index.Term(term.field(), '#' + term.text()))

        return query

    def getFieldQuery_quoted(self, *args):
        return self.rewrite(self.getFieldQuery_quoted_super(*args))
    def getFieldQuery_slop(self, *args):
        return self.rewrite(self.getFieldQuery_slop_super(*args))
