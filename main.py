from fastapi import FastAPI
import requests
import adal
import json
from pypowerbi.dataset import Column, Table, Dataset, Row
from pypowerbi.client import PowerBIClient
from pydantic import BaseModel
from credential import username, password, client_id, client_secret
from constant import resource_url, authority_url, api_Power_BI_url, Gen_header

app = FastAPI()


class QueryModel(BaseModel):
    query: str
class DatasetModel(BaseModel):
    datasetName: str
    tableName: str
    colCount: int
    colListName: list

@app.get("/powerbi/workspaces")
async def get_all_workspace():

    # Authenticate using adal
    context = adal.AuthenticationContext(authority=authority_url,
                                             validate_authority=True,
                                             api_version=None)


    # get your authentication token
    # Uncomment this following line if you choose login authenticate
    token = context.acquire_token_with_username_password(resource=resource_url,
                                                             client_id=client_id,
                                                             username=username,
                                                             password=password)



    # Uncomment this following line if you choose client credentials
    # token = context.acquire_token_with_client_credentials(resource=resource_url,
    #                                                       client_id=client_id,
    #                                                       client_secret=client_secret)

    # Gen part of API
    headers = Gen_header(token.get("accessToken"))
    url = api_Power_BI_url.get("workspaces")


    # Call to REST API Power BI endpoint
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # raises an HTTPError if the status code indicates an error
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return "Error"
    except Exception as err:
        print(f"Other error occurred: {err}")
        return "Other Error"
    else:
        data = response.json()
        print(f"Data retrieved successfully. The data is: {data}")
        return data


@app.get("/powerbi/datasets")
async def get_datasets(workspaceId=None):

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
    data = client.datasets.get_datasets(group_id=workspaceId)

    return data


@app.post("/powerbi/queryDax")
async def query_dax(datasetId: str, body: QueryModel):

    # Authenticate using adal
    context = adal.AuthenticationContext(authority=authority_url,
                                         validate_authority=True,
                                         api_version=None)

    # get your authentication token
    token = context.acquire_token_with_username_password(resource=resource_url,
                                                         client_id=client_id,
                                                         username=username,
                                                         password=password)

    # Gen part of API
    headers = Gen_header(token.get("accessToken"))
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/executeQueries"
    data = {
        "queries": [
            {"query": f'{body.query}'}
        ]
    }

    # Call to REST API Power BI endpoint
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # raises an HTTPError if the status code indicates an error
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return "Error"
    except Exception as err:
        print(f"Other error occurred: {err}")
        return "Other Error"
    else:
        data = response.json()
        print(f"Data retrieved successfully. The data is: {data}")
        return data


@app.post("/powerbi/addnew")
async def add_new(workspaceId: str, body:DatasetModel):

    # Authenticate using adal
    context = adal.AuthenticationContext(authority=authority_url,
                                         validate_authority=True,
                                         api_version=None)


    # create your powerbi api client
    client = PowerBIClient.get_client_with_username_password(client_id=client_id, username=username, password=password)

    # create your columns

    columns = []
    for item in body.colListName:
        print(item)
        columns.append(Column(name=item.get("name"), data_type=item.get("type")))

    # create your tables
    tables = []
    tables.append(Table(name=body.tableName, columns=columns))

    # create your dataset
    dataset = Dataset(name=body.datasetName, tables=tables)

    # post your dataset!
    client.datasets.post_dataset(dataset, group_id=workspaceId)


    return "add success"

