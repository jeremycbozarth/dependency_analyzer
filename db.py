from report import *
from sqlalchemy import create_engine, Engine
from sqlalchemy.engine import Connection
from typing import Optional

_engine: Optional[Engine] = None
_conn: Optional[Connection] = None

def get_engine() -> Engine:
    if _engine is None:
        raise RuntimeError("Database engine not initialized")

    return _engine


def get_conn() -> Connection:
    if _conn is None:
        raise RuntimeError("Database connection not initialized")

    return _conn


def connect_to_db(conn_url):

    global _engine
    global _conn

    _engine = create_engine(conn_url)
    _conn = _engine.connect()


def close_db_connection():

    global _conn

    if _conn is not None:
        _conn.close()
        _conn = None
