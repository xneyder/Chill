""" ManagedDbConnection.py:

 Description: Manage Oracle DB Connection

 Created by : Daniel Jaramillo
 Creation Date: 11/01/2019
 Modified by:     Date:
 All rights(C) reserved to Teoco
"""
import cx_Oracle
from LoggerInit import LoggerInit

class ManagedDbConnection:
    def __init__(self, DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST):
        self.DB_USER = DB_USER
        self.DB_PASSWORD = DB_PASSWORD
        self.ORACLE_SID = ORACLE_SID
        self.DB_HOST = DB_HOST
        self.app_logger=LoggerInit().get_logger("ManagedDbConnection")

    def __enter__(self):
        try:
            self.db = cx_Oracle.connect(
                '{DB_USER}/{DB_PASSWORD}@{DB_HOST}/{ORACLE_SID}'\
                .format(
                    DB_USER=self.DB_USER,
                    DB_PASSWORD=self.DB_PASSWORD,
                    DB_HOST=self.DB_HOST,
                    ORACLE_SID=self.ORACLE_SID), threaded=True)
        except cx_Oracle.DatabaseError as e:
            self.app_logger.error(e)
            quit()
        self.cursor = self.db.cursor()
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.db:
            self.db.close()
