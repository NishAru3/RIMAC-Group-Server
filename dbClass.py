from time import sleep

import pandas as pd
import json
import os
import sys
import pymysql
from pymysql import Error
import time, datetime

'''
for remote access - add HOSTNAME=localhost to env  
ssh -L 8676:127.0.0.1:3306 ialerner@cse191.ucsd.edu
'''
class dbClass:

    def __init__(self, type="JSON"):
        self.outType = type
        # AMAZON CLOUD - AWS DB Cluster
        print("connect to main DB")
        self.servername = "127.0.0.1"
        self.username = "root"
        self.password = "iotiot"
        self.dbname = "cse191"
        self.port = 3306

        if os.getenv('HOSTNAME') == "localpc":
            print("connect local ssh tunnel")
            self.port = 8676

        self.reconnect()

    def check_conn(self):

        # test connection
        try:
            if self.db.cursor().execute("SELECT now()") == 0:
                return self.reconnect()
            else:
                print("DB connection OK\n")
                return True
        except:
            print("Unexpected exception occurred: ", sys.exc_info())
            return self.reconnect()

    def reconnect(self):

        # try to connect 5 times
        retry = 5
        while retry > 0:
            try:
                print("connecting to DB...")
                self.db = pymysql.connect(
                    host=self.servername,
                    user=self.username,
                    password=self.password,
                    database=self.dbname,
                    port=self.port
                )
                retry = 0
                return True
            except:
                print("Unexpected exception occurred: ", sys.exc_info())
                retry -= 1
                if retry > 0:
                    print("retry\n")
                    sleep(2)
                else:
                    exit(-1)

        print("Success\n")

    def loadStudents(self, gn):
        if self.check_conn():
            stu_df = pd.DataFrame
            sqlStr = "SELECT * FROM cse191.students ORDER BY groupnumber"
            if (gn):
                sqlStr = "SELECT * FROM cse191.students WHERE groupnumber="+gn+" ORDER BY groupnumber"
            print(sqlStr)
            cursor = self.db.cursor()
            result = None
            try:
                cursor.execute(sqlStr)
                result = cursor.fetchall()
                print(result)
                stu_df = pd.DataFrame.from_dict(result) 
                stu_df.columns=["id","name","email","groupnumber","groupname"]
                print(stu_df)
            except Error as e:
                print(f"The error '{e}' occurred")

            return stu_df
        
    def loadDevices(self, gn):
        if self.check_conn():
            stu_df = pd.DataFrame
            sqlStr = "SELECT * FROM cse191.devices ORDER BY groupnumber"
            if (gn):
                sqlStr = "SELECT * FROM cse191.devices WHERE groupnumber="+gn+" ORDER BY groupnumber"
            print(sqlStr)
            cursor = self.db.cursor()
            result = None
            try:
                cursor.execute(sqlStr)
                result = cursor.fetchall()
                print(result)
                stu_df = pd.DataFrame.from_dict(result) 
                stu_df.columns=["device_id", "mac", "lastseen_ts", "last_rssi", "groupname", "location", "lang","long", "color", "groupnumber", "status"]
                print(stu_df)
            except Error as e:
                print(f"The error '{e}' occurred")

            return stu_df

    def getSettings(self):
        if self.check_conn():
            sqlStr = "FETCH FROM cse191.settings"
            cursor = self.db.cursor()
            result = None
            try:
                cursor.execute(sqlStr)
                return "it executed alright"
                result = cursor.fetchall()
                settings_df = pd.DataFrame.from_dict(result) 
                settings_df.columns=["setting_id", "rssi_limit", "sample_period"]
                return settings_df
            except Error as e:
                print(f"The error '{e}' occurred")
                return "The error: " + e
        return False
    
    def registerDevice(self, data):
        if self.check_conn():
            sqlstr = f"SELECT mac FROM cse191.devices WHERE mac = \"{data.espmac}\""
            cursor = self.db.cursor()
            cursor.execute(sqlstr)
            result = cursor.fetchall()
            try:
                ts = time.time()
                timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                if len(result) == 0: #Insert if not registered
                    sqlstr = f"INSERT INTO cse191.devices (mac,  lastseen_ts, groupname, groupnumber, status)\
                        VALUES (\"{data.espmac}\", \"{timestamp}\", \"CSE191\", 0, \"ACTIVE\");"
                else: #Update last_rssi and last_seen_ts if already exists
                    sqlstr = f"UPDATE cse191.devices \
                                SET lastseen_ts = \"{timestamp}\", status=\"ACTIVE\" \
                                WHERE mac = \"{data.espmac}\";"
                cursor.execute(sqlstr)
                cursor.execute("COMMIT;")
                return True
            except Error as e:
                print(f"The error '{e}' has occurred")
        return False
    
    def addDevices(self, data):
        if self.check_conn():
            
            ts = time.time()
            timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            cursor = self.db.cursor()

            registerSQLString = f"UPDATE cse191.devices \
                            SET lastseen_ts = \"{timestamp}\", status=\"ACTIVE\" \
                            WHERE mac = \"{data.espmac}\";"

            if data.devices:
                
                sqlStr = "INSERT INTO cse191.ble_logs\
                    (device_mac, ble_rssi, ble_mac, log_ts)\
                    VALUES"
                for i in range(len(data.devices)):
                    device = data.devices[i]
                    valStr = " ('{0}', '{1}', '{2}', '{3}')".format(data.espmac, device.get("rssi"), device.get("mac"), timestamp)
                    sqlStr += valStr
                    if i == len(data.devices)-1:
                        registerSQLString = f"UPDATE cse191.devices \
                            SET lastseen_ts = \"{timestamp}\", last_rssi = \"{device.get('rssi')}\", status=\"ACTIVE\" \
                            WHERE mac = \"{data.espmac}\";"
                        sqlStr += ";"
                    else:
                        sqlStr += ","
                try:
                    cursor.execute(sqlStr)
                    cursor.execute("COMMIT;")
                except Error as e:
                    print(f"The error '{e}' has occurred")
                    return False
            try:
                cursor.execute(registerSQLString)
                cursor.execute("COMMIT;")
                return True
            except Error as e:
                print(f"The error '{e}' has occurred")

        return False
  
    def timeoutCheck(self):
        if self.check_conn():
            newSQLStr = "UPDATE cse191.devices SET status=\"TIMEOUT\" WHERE DATE_ADD(lastseen_ts, INTERVAL 5 MINUTE) < NOW()"
            cursor = self.db.cursor()
            try:
                cursor.execute(newSQLStr)
                cursor.execute("COMMIT;")
                return True
            except Error as e:
                print(e)
        return False
