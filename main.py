import json
from typing import Union
import pandas as pd

import uvicorn
from fastapi import FastAPI, Response
from fastapi.responses import PlainTextResponse
from starlette.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from dbClass import dbClass

app = FastAPI()

cse191db = dbClass()

origins = [
    "https://cse191.ucsd.edu"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"],
    max_age=3600,
)


def setHeaders(response: Response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Origin,X-Requested-With,Content-Type,Authorization,Accept'
    response.headers['Access-Control-Allow-Methods'] = 'OPTIONS,GET,PUT,POST,DELETE'
    response.headers['Service'] = 'CSE191-API'


class DeviceInfo(BaseModel):
    group_id: str
    mac: str

class DeviceLog(BaseModel):
    gn: str
    espmac: str
    devices: list

@app.get('/', response_class=PlainTextResponse)
def home():
    return 'CSE 191 API\n'


@app.get('/health')
def process_health(response: Response):
    setHeaders(response)
    return {"resp": "OK"}


@app.get('/list-students', response_class=PlainTextResponse)
def process_list_students(response: Response, gn: Union[str,None] = None, outtype: Union[str, None] = None):
    setHeaders(response)
    student_list = cse191db.loadStudents(gn)
    if outtype == "JSON":
        sl_string = student_list.to_json(orient="records")
    else:
        sl_string = student_list.to_string()
    return sl_string

@app.get('/list-devices', response_class=PlainTextResponse)
def process_list_devices(response: Response, gn: Union[str,None] = None, outtype: Union[str, None] = None):
    setHeaders(response)
    device_list = cse191db.loadDevices(gn)
    if outtype == "JSON":
        dl_string = device_list.to_json(orient="records")
    else:
        dl_string = device_list.to_string()
    return dl_string

# @app.post('/list-device')
# def process_list_device(response: Response, data: DeviceInfo):
#     setHeaders(response)
#     device_list = cse191db.loadDevices()
#     return {"resp": "OK"}

@app.post('/register-device')
def process_register_device(response: Response, data: DeviceInfo):
    setHeaders(response)
    if (not cse191db.registerDevice(data)):
        return {"resp": "FAIL"}
    else: 
        return {"resp": "OK"}

@app.post('/log-devices')
def process_log_devices(response: Response, data: DeviceLog):
    setHeaders(response)
    if (not cse191db.addDevices(data)):
        return {"resp": "FAIL"}
    else: 
        return {"resp": "OK"}


# run the app
if __name__ == '__main__':
    uvicorn.run("main:app", host="localhost", port=8080, reload=True)