class Cursor:
    def execute(self, query, params=None):
        self.rowcount = 1
    def executemany(self, query, params_seq):
        self.rowcount = len(list(params_seq))
    def fetchall(self):
        return []
    @property
    def description(self):
        return []
    def prepare(self, query):
        self._prepared = query
    def setinputsizes(self, sizes):
        pass
    fast_executemany = False

class Connection:
    def __init__(self, *args, **kwargs):
        pass
    def cursor(self):
        return Cursor()
    def commit(self):
        pass

SQL_STRUCTURED = object()

def connect(*args, **kwargs):
    return Connection()

