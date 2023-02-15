authority_url = 'https://login.windows.net/common'
resource_url = 'https://analysis.windows.net/powerbi/api'

api_Power_BI_url = {
    'workspaces': "https://api.powerbi.com/v1.0/myorg/groups",
    'dataset': "https://api.powerbi.com/v1.0/myorg/groups"
}

def Gen_header (accessToken: str):
    headers = {
        'Authorization': f'Bearer {accessToken}',
        'Content-Type': 'application/json'
    }
    return headers
