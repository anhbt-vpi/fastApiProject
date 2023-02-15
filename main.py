from fastapi import FastAPI
import requests
import cdata.powerbixmla as mod
import adal
import pandas as pd
import json
from pypowerbi.dataset import Column, Table, Dataset, Row
from pypowerbi.client import PowerBIClient
from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

app = FastAPI()


class Item(BaseModel):
    query: str


@app.get("/powerbi/workspaces")
async def get_all_workspace():
    authority_url = 'https://login.windows.net/common'
    resource_url = 'https://analysis.windows.net/powerbi/api'
    api_url = 'https://api.powerbi.com'

    # change these to your credentials
    client_id = 'ced5a0b0-3886-452b-9ca7-1fc9993a4ef5'
    username = 'anhbt@vpi.pvn.vn'
    password = 'Mac0901'

    # Authenticate using adal
    context = adal.AuthenticationContext(authority=authority_url,
                                         validate_authority=True,
                                         api_version=None)

    # get your authentication token
    token = context.acquire_token_with_username_password(resource=resource_url,
                                                         client_id=client_id,
                                                         username=username,
                                                         password=password)

    # create your powerbi api client
    client = PowerBIClient.get_client_with_username_password(client_id=client_id, username=username, password=password)

    # get all wsp!
    data = client.groups.get_groups()

    return data


@app.get("/powerbi/dataset")
async def get_dataset(workspaceId=None, datasetId=None):
    authority_url = 'https://login.windows.net/common'
    resource_url = 'https://analysis.windows.net/powerbi/api'
    api_url = 'https://api.powerbi.com'

    # change these to your credentials
    client_id = 'ced5a0b0-3886-452b-9ca7-1fc9993a4ef5'
    username = 'anhbt@vpi.pvn.vn'
    password = 'Mac0901'

    # Authenticate using adal
    context = adal.AuthenticationContext(authority=authority_url,
                                         validate_authority=True,
                                         api_version=None)

    # get your authentication token
    token = context.acquire_token_with_username_password(resource=resource_url,
                                                         client_id=client_id,
                                                         username=username,
                                                         password=password)

    # create your powerbi api client
    client = PowerBIClient.get_client_with_username_password(client_id=client_id, username=username, password=password)

    # get all your dataset!
    if workspaceId is None:
        data = client.datasets.get_dataset(dataset_id=datasetId)
    else:
        data = client.datasets.get_datasets(group_id=workspaceId)

    return data


@app.post("/powerbi/table")
async def query_dax(datasetId: str, item: Item):
    authority_url = 'https://login.windows.net/common'
    resource_url = 'https://analysis.windows.net/powerbi/api'
    api_url = 'https://api.powerbi.com'

    # change these to your credentials
    client_id = 'ced5a0b0-3886-452b-9ca7-1fc9993a4ef5'
    username = 'anhbt@vpi.pvn.vn'
    password = 'Mac0901'

    # Authenticate using adal
    context = adal.AuthenticationContext(authority=authority_url,
                                         validate_authority=True,
                                         api_version=None)

    # get your authentication token
    token = context.acquire_token_with_username_password(resource=resource_url,
                                                         client_id=client_id,
                                                         username=username,
                                                         password=password)

    headers = {
        'Authorization': f'Bearer {token.get("accessToken")}',
        'Content-Type': 'application/json'
    }

    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/executeQueries"
    data = {
        "queries": [
            {
                "query": f"{item.query}"
            }
        ]
    }
    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        # Print the tables in the report
        print(response.json())
    else:
        # Print the error message
        print("Error: " + response.text)

    return response.json()


@app.post("/powerbi/addnew")
async def add_new(workspaceId=None):
    authority_url = 'https://login.windows.net/common'
    resource_url = 'https://analysis.windows.net/powerbi/api'
    api_url = 'https://api.powerbi.com'

    # change these to your credentials
    client_id = 'ced5a0b0-3886-452b-9ca7-1fc9993a4ef5'
    username = 'anhbt@vpi.pvn.vn'
    password = 'Mac0901'

    # Authenticate using adal
    context = adal.AuthenticationContext(authority=authority_url,
                                         validate_authority=True,
                                         api_version=None)

    # get your authentication token
    token = context.acquire_token_with_username_password(resource=resource_url,
                                                         client_id=client_id,
                                                         username=username,
                                                         password=password)

    # create your powerbi api client
    client = PowerBIClient.get_client_with_username_password(client_id=client_id, username=username, password=password)

    # create your columns
    columns = []
    columns.append(Column(name='a', data_type='Int64'))
    columns.append(Column(name='b', data_type='Int64'))
    columns.append(Column(name='c', data_type='Int64'))
    columns.append(Column(name='d', data_type='Int64'))
    columns.append(Column(name='e', data_type='string'))
    columns.append(Column(name='f', data_type='string'))
    rows = []
    rows.append(Row(a=1, b=2, c=4, d=4, e='abc', f='abc'))
    rows.append(Row(a=1, b=2, c=4, d=4, e='abc', f='abc'))
    # create your tables
    tables = Table(name='2222', columns=columns)
    # tables.append(Table(name='contacts', columns=columns, rows=rows))
    # tables = Table(name='2222', columns=columns, rows=rows)
    # create your dataset
    dataset = Dataset(name='apidemo3', tables=tables)
    # abc = client.datasets.put_table(dataset_id='3cc7a461-dd86-4382-bfa8-f395e562007d', table_name='2222', table=tables)
    abc = client.datasets.delete_dataset(dataset_id='3cc7a461-dd86-4382-bfa8-f395e562007d')
    # client.datasets.datasets_from_get_datasets_response()
    # post your dataset!
    # client.datasets.post_dataset(dataset, group_id=workspaceId)
    # abc = client.datasets.post_rows(dataset_id='3cc7a461-dd86-4382-bfa8-f395e562007d', table_name='contacts', rows=rows)
    # print(dataset.name)

    return abc


@app.get("/powerbi/duplicate")
async def query_dax(datasetId: str):
    authority_url = 'https://login.windows.net/common'
    resource_url = 'https://analysis.windows.net/powerbi/api'
    api_url = 'https://api.powerbi.com'

    # change these to your credentials
    client_id = 'ced5a0b0-3886-452b-9ca7-1fc9993a4ef5'
    username = 'anhbt@vpi.pvn.vn'
    password = 'Mac0901'

    # Authenticate using adal
    context = adal.AuthenticationContext(authority=authority_url,
                                         validate_authority=True,
                                         api_version=None)

    # get your authentication token
    token = context.acquire_token_with_username_password(resource=resource_url,
                                                         client_id=client_id,
                                                         username=username,
                                                         password=password)

    headers = {
        'Authorization': f'Bearer {token.get("accessToken")}',
        'Content-Type': 'application/json'
    }

    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/clone"

    response = requests.post(url, headers=headers)

    if response.status_code == 200:
        # Print the tables in the report
        print(response.json())
    else:
        # Print the error message
        print("Error: " + response.text)

    return response.json()
