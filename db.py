from report import *
from sqlalchemy import create_engine

_engine = ()
_conn = ()

def get_engine():

    return _engine


def get_conn():

    return _conn


def connect_to_db(conn_url):

    global _engine
    global _conn

    _engine = create_engine(conn_url)
    _conn = _engine.connect()


def close_db_connection():
    _conn.close()
