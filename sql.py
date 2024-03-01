import random
import sqlite3
from datetime import datetime, date
import os

sql_file = r"C:\Users\joshu\PycharmProjects\bierdeckel_api\test.db"
if os.environ.get("SQLFILE"):
    sql_file = os.environ["SQLFILE"]
con = sqlite3.connect(sql_file)
cur = con.cursor()

INSERT_STATEMENT_SETTINGS = """INSERT INTO settings (min_weight, max_weight, min_time_diff, tolerance) VALUES(?, ?, ?, ?)"""
UPDATE_STATEMENT_WITH_RECOGNISED_VALUE = """UPDATE stats SET time_stamp = ?, value = ?, last_seen = ? WHERE serial = ?"""
UPDATE_STATEMENT_WITH_UNRECOGNIZED_VALUE = """UPDATE stats SET last_seen = ? WHERE serial = ?"""
INSERT_STATEMENT = """INSERT INTO stats (time_stamp, value, serial, last_seen) VALUES(?, ?, ?, ?)"""
CREATE_TABLE_STATEMENT = """
CREATE TABLE IF NOT EXISTS stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time_stamp DATE,
    value INTEGER, 
    serial varchar(50),
    last_seen DATE
);
"""

CREATE_STATEMENT_SETTINGS = """
CREATE TABLE IF NOT EXISTS settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    min_weight integer,
    max_weight integer,
    min_time_diff integer,
    tolerance integer

);"""


cur.execute(CREATE_TABLE_STATEMENT)
cur.execute(CREATE_STATEMENT_SETTINGS)
if not cur.execute("SELECT * FROM SETTINGS").fetchone():
    cur.execute(INSERT_STATEMENT_SETTINGS, (100, 1000,1000, 10 ))
con.commit()


def insert(mqtt_string: str):
    serial, value = mqtt_string.split("**")
    time_stamp = datetime.now()
    min_recognise_value = cur.execute("SELECT min_weight FROM SETTINGS").fetchone()[0]
    if float(value) < min_recognise_value:
        if _get_stats_by_serial(serial):
            _update_unrecognised_value(time_stamp, serial)
        return
    if _get_stats_by_serial(serial):
        _update_recognised_value(time_stamp, value, serial)
    else:
        _insert_recognised_value(time_stamp, value, serial)
    con.commit()


def _get_stats_by_serial(serial: str) -> list[tuple]:
    return cur.execute("SELECT * FROM stats WHERE serial = ?", (serial,)).fetchone()


def _update_recognised_value(time_stamp: datetime, value: str, serial: str):
    cur.execute(UPDATE_STATEMENT_WITH_RECOGNISED_VALUE, (time_stamp, value, time_stamp, serial))


def _insert_recognised_value(time_stamp: datetime, value: str, serial: str):
    cur.execute(INSERT_STATEMENT, (time_stamp, value, serial, time_stamp))


def _update_unrecognised_value(time_stamp: datetime, serial: str):
    cur.execute(UPDATE_STATEMENT_WITH_UNRECOGNIZED_VALUE, (time_stamp, serial))


def close():
    con.close()

if __name__ == "__main__":
    for i in range(100  ):
        _insert_recognised_value(datetime.now(), str(random.randint(50, 120)), str(i))
    con.commit()