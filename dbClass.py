from time import sleep

import pandas as pd
import json
import os
import sys
import pymysql
from pymysql import Error
import time
from datetime import datetime, timedelta


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
            sqlStr = "SELECT * FROM cse191.settings"
            cursor = self.db.cursor()
            result = None
            try:
                cursor.execute(sqlStr)
                result = cursor.fetchall()
                settings_df = pd.DataFrame.from_dict(result) 
                settings_df.columns=["setting_id", "rssi_limit", "sample_period"]
                return settings_df
            except Error as e:
                print(f"The error '{e}' occurred")
        return False
    
    def registerDevice(self, data):
        if self.check_conn():
            sqlstr = f"SELECT mac FROM cse191.devices WHERE mac = \"{data.espmac}\""
            cursor = self.db.cursor()
            cursor.execute(sqlstr)
            result = cursor.fetchall()
            try:
                ts = time.time()
                timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
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
            timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
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
                    sqlStr += " ('{0}', '{1}', '{2}', '{3}')".format(data.espmac, device.get("rssi"), device.get("mac"), timestamp)
                    if i != len(data.devices)-1:
                        sqlStr += ","
                    else:
                        registerSQLString = f"UPDATE cse191.devices \
                            SET lastseen_ts = \"{timestamp}\", last_rssi = \"{device.get('rssi')}\", status=\"ACTIVE\" \
                            WHERE mac = \"{data.espmac}\";"
                        sqlStr += ";"
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
    
    def getDataFromTime(self, getTime):
        if self.check_conn():
            initialTime = datetime.strptime(getTime, '%Y-%d-%m %H:%M:%S')
            output = {}
            firstTime = initialTime
            secondTime = firstTime + timedelta(seconds=20)
            firstTime = firstTime.strftime("%Y-%d-%m %H:%M:%S")
            secondTime = secondTime.strftime("%Y-%d-%m %H:%M:%S")
            output[firstTime] = {}
            durationSQLStr = "SELECT * FROM cse191.ble_logs WHERE log_ts >= \"" + firstTime + "\" AND log_ts < \"" + secondTime + "\""
            cursor = self.db.cursor()
            print(durationSQLStr)
            return False
            # try:
            #     cursor.execute(durationSQLStr)
            #     result = cursor.fetchall()
            #     data_df = pd.DataFrame.from_dict(result) 
            #     data_df.columns=["log_id", "device_mac", "ble_rssi", "ble_mac", "log_ts"]
            #     for index, row in data_df.iterrows():
            #         if row["ble_mac"] not in output[firstTime]:
            #             output[firstTime][row["ble_mac"]] = {}
            #         output[firstTime][row["ble_mac"]][row["device_mac"]] = row["ble_rssi"] 
            #     return output
            # except Error as e:
            #     print(e)


            # for i in range(20):
            #     firstTime = initialTime + timedelta(minutes=(5*i))
            #     secondTime = firstTime + timedelta(seconds=20)
            #     firstTime = firstTime.strftime("%Y-%d-%m %H:%M:%S")
            #     secondTime = secondTime.strftime("%Y-%d-%m %H:%M:%S")
            #     output[firstTime] = {}
            #     durationSQLStr = "SELECT * FROM cse191.ble_logs WHERE log_ts >= \"" + firstTime + "\" AND log_ts < \"" + secondTime + "\""
            #     cursor = self.db.cursor()
            #     try:
            #         cursor.execute(durationSQLStr)
            #         result = cursor.fetchall()
            #         data_df = pd.DataFrame.from_dict(result) 
            #         data_df.columns=["log_id", "device_mac", "ble_rssi", "ble_mac", "log_ts"]
            #         for index, row in data_df.iterrows():
            #             if row["ble_mac"] not in output[firstTime]:
            #                 output[firstTime][row["ble_mac"]] = {}
            #             output[firstTime][row["ble_mac"]][row["device_mac"]] = row["ble_rssi"] 
            #     except Error as e:
            #         print(e)
            # return output
        return False
    
    def getFloorData(self, floor, date=None):
        if self.check_conn():
            if not date:
                date = datetime.now().date()
            SQLStr = f"SELECT * FROM cse191.rimac_data WHERE idfloor_a_rimac={floor} AND DATE(`ts_5min`) = {date};"
            cursor = self.db.cursor()
            try:
                cursor.execute(SQLStr)
                data = cursor.fetchall()
                data_df = pd.DataFrame.from_dict(data)
                data_df.columns = [
                    "floor_number",
                    "ts_5min",
                    "xy_array",
                    "floor_name"
                ]
                return data_df
            except Exception as e:
                print(e)
            return None
            
            
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

    def getDF(self, time):
        if self.check_conn():
            start_time = time
                    
            # Convert the string to a datetime object
            datetime_object = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")

            # Add 20 seconds to the datetime object
            new_datetime = datetime_object + timedelta(seconds=20)

            # Convert the new datetime back to a string
            end_time = new_datetime.strftime("%Y-%m-%d %H:%M:%S")

            hashmap = {
                "E0:5A:1B:9C:FD:90": 2,
                "CC:50:E3:A8:F3:00": 1,
                "CC:50:E3:A8:D9:6C": 3,
                "80:7D:3A:BC:C6:30": 12,
                "E0:5A:1B:A0:57:0C": 20,
                "3C:71:BF:63:83:28": 22,
                "E0:5A:1B:A0:40:F8": 8,
                "E0:5A:1B:A0:37:D8": 4,
                "E0:5A:1B:A0:1F:D0": 5,
                "E0:5A:1B:A0:38:C0": 6,
                "E0:5A:1B:A0:3D:C8": 7,
                "E0:5A:1B:A0:2A:28": 17,
                "E0:5A:1B:A0:1E:88": 11,
                "A4:CF:12:43:6A:A0": 9,
                "E0:5A:1B:A0:1A:C0": 13,
                "E0:5A:1B:A0:33:C4": 14,
                "3C:71:BF:64:3B:74": 15,
                "E0:5A:1B:A0:51:9C": 23,
                "3C:71:BF:62:C2:B8": 16,
                "E0:5A:1B:A0:4C:84": 18,
                "3C:71:BF:64:26:74": 24,
                "E0:5A:1B:A0:3E:7C": 19,
                "E0:5A:1B:A0:2F:B8": 21
            }
            try:
                cursor = self.db.cursor()
                print("SELECT * FROM cse191.ble_logs WHERE log_ts BETWEEN '{}' AND '{}' GROUP BY ble_mac ASC".format(start_time, end_time))
                cursor.execute("SELECT * FROM cse191.ble_logs WHERE log_ts BETWEEN '{}' AND '{}' GROUP BY ble_mac ASC".format(start_time, end_time))
                results = cursor.fetchall()
                # print(results)
                df = pd.DataFrame()
                cur_device = results[0][3]
                rowarray = ["PLACEHOLDER", -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, time]
                for i in range(0, len(results) - 1): 
                    rowarray[0] = results[i][3]
                    while (results[i][3] == cur_device):
                        index = hashmap.get(results[i][1])
                        rowarray[index] = results[i][2]
                        i += 1
                    if results[i][3] != cur_device:
                        cur_device = results[i][3]
                        df = pd.concat([df, pd.DataFrame([rowarray])], ignore_index=True)
                        # reset for next device
                        rowarray = [results[i][3], -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, -150, time]
                        index = hashmap.get(results[i][1])
                        rowarray[index] = results[i][2]
                # append the remaining ones (issue)
                df = pd.concat([df, pd.DataFrame([rowarray])], ignore_index=True)
                df.columns=['DEVICE MAC','A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', "TIME"]
                print(df)
                return df
            except Error as  e:
                print(f"The error '{e}' occurred")