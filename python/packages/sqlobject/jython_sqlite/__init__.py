from sqlobject.dbconnection import registerConnection

def builder():
    import sqliteconnection
    return sqliteconnection.SQLiteConnection

registerConnection(['jython_sqlite'], builder)
