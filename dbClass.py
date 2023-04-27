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
    
    def registerDevice(self, data, gn):
        """
            Resister or update an ESP32 chip to the device table. 
            For now using last device in devices list to update last_rssi field.
            Incoming request should include ESP32's wifi RSSI.
        """
        if self.check_conn():
            mac = data.espmac
            rssi = data.devices[-1].get("rssi")
            sqlstr = f"SELECT mac FROM cse191.devices WHERE mac = \"{mac}\""
            cursor = self.db.cursor()
            result = None
            cursor.execute(sqlstr)
            result = cursor.fetchall()
            print(result)
            try:
                if len(result) == 0: #Insert if not registered
                    ts = time.time()
                    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                    group_name_finder = f"SELECT groupname from cse191.students where groupnumber = {data.gn};"
                    cursor.execute(group_name_finder)
                    groupname = cursor.fetchall()
                    sqlstr = f"INSERT INTO cse191.devices (mac, last_rssi, lastseen_ts, groupname, groupnumber, status)\
                        VALUES (\"{mac}\", {rssi}, \"{timestamp}\", \"{groupname}\", {gn}, \"ACTIVE\");"
                    cursor.execute(sqlstr)
                    cursor.execute("COMMIT;")
                    return True
                else: #Update last_rssi and last_seen_ts if already exists
                    ts = time.time()
                    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') 
                    sqlstr = f"UPDATE cse191.devices \
                                SET last_rssi = {rssi}, lastseen_ts = \"{timestamp}\", status=\"ACTIVE\" \
                                WHERE mac = \"{mac}\";"
                    cursor.execute(sqlstr)
                    cursor.execute("COMMIT;")
                    return True
            except Error as e:
                print(f"The error '{e}' has occurred")
                return False
    
    def addDevices(self, data):
        if self.check_conn():
            if not data.devices:
                return True

            ts = time.time()
            timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            sqlStr = "INSERT INTO cse191.ble_logs\
                    (device_mac, ble_rssi, ble_mac, groupname, groupnumber, log_ts, ble_count)\
                    VALUES"
            
            for i in range(len(data.devices)):
                device = data.devices[i]
                valStr = " ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}')".format(data.espmac, device.get("rssi"), device.get("mac"), "Global API", data.gn, timestamp, "30")
                sqlStr += valStr
                if i == len(data.devices)-1:
                    sqlStr += ";"
                else:
                    sqlStr += ","

            cursor = self.db.cursor()
            try:
                cursor.execute(sqlStr)
                cursor.execute("COMMIT;")
                return True
            except Error as e:
                print(sqlStr)
                print(e)
        return False
    
   def timeoutCheck(self):
         if self.check_conn():
            sqlStr = "UPDATE cse191.devices SET status=\"TIMEOUT\" WHERE DATE_ADD(lastseen_ts, INTERVAL 5 MINUTE) < now()"
            cursor = self.db.cursor()
            cursor.execute(sqlStr)
            self.db.commit()
