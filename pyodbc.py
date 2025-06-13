class Cursor:
    def execute(self, query, params=None):
        self.rowcount = 1
    def fetchall(self):
        return []
    @property
    def description(self):
        return []

class Connection:
    def __init__(self, *args, **kwargs):
        pass
    def cursor(self):
        return Cursor()
    def commit(self):
        pass

def connect(*args, **kwargs):
    return Connection()

