
import sqlite3
import os

tracelinks_db = None
tracelinks_db_cursor = None
def ConnectToTraceLinksDb(dbname):
    global tracelinks_db
    global tracelinks_db_cursor
    tracelinks_db = sqlite3.connect(dbname)
    # 使用 cursor() 方法创建一个游标对象 cursor
    if not tracelinks_db:
        print("ConnectToDb failed!")
    tracelinks_db_cursor = tracelinks_db.cursor()
    
    