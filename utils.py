import pymysql
import psycopg2
from time import time
import pyodbc
import pprint
import json
import warnings


class connect_mysql():
    def __init__(self, host="localhost", user="myuser", password=None, db="db"):
        self.db = pymysql.connect(host=host, user=user, password=password, db=db)
        self.cursor = self.db.cursor()

    def run_query(self, query_statement):
        start_time = int(round(time() * 1000))
        self.cursor.execute(query_statement)
        col_info = self.cursor.description
        result = self.cursor.fetchall()
        print(result)
        query_time = str(int(round(time() * 1000)) - start_time) + " ms"
        col_name = []
        for i in range(len(col_info)):
            col_name.append(col_info[i][0])
        return col_name, result, query_time



    def disconnect(self):
        self.cursor.close()
        self.db.close()



class connect_redshift():
    def __init__(self, host="localhost", database='database', user='awsuser', password='1234', port=5439):
        print("I am working")
        self.con = psycopg2.connect(host=host, dbname=database, port=port, password=password, user=user)
        self.cur = self.con.cursor()

    def run_query(self, query_statement):
        start_time = int(round(time() * 1000))
        self.cur.execute(query_statement)
        col_info = self.cur.description
        result = self.cur.fetchall()
        query_time = str(int(round(time() * 1000)) - start_time) + " ms"
        col_name = []
        for i in range(len(col_info)):
            col_name.append(col_info[i][0])
        return col_name, result, query_time
    
    def disconnect(self):
        self.cur.close()
        self.con.close()



class connect_mongodb():

    def __init__(self,database = "database"):
        print("Iam working")
        driver="{"+"MongoDB ODBC Driver"+"}"
        self.db = pyodbc.connect('DRIVER={MongoDB ODBC Driver};Server=18.219.52.254;Port=3307;User=userName;Password=pwd;Database=' + database, autocommit=True)
        print("connected")
        self.cursor = self.db.cursor()


    def run_query(self, query_statement):
        start_time = int(round(time() * 1000))
        self.cursor.execute(query_statement)
        col_info = self.cursor.description
        result = self.cursor.fetchall()
        mongoData = []
        for row in result:
            mongoData.append([x for x in row])
        query_time = str(int(round(time() * 1000)) - start_time) + " ms"
        col_name = []
        for i in range(len(col_info)):
            col_name.append(col_info[i][0])
        return col_name, mongoData, query_time


    def disconnect(self):
        self.cursor.close()
        self.db.close()
