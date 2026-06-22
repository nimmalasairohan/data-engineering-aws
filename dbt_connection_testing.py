import requests
import os

ACCOUNT_ID = os.environ["DBT_ACCOUNT_ID"]
JOB_ID = os.environ["DBT_JOB_ID"]
SERVICE_TOKEN = os.environ["DBT_SERVICE_TOKEN"]
DBT_API_BASE_URL = os.environ["DBT_API_BASE_URL"]

url = f"{DBT_API_BASE_URL.rstrip('/')}/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/"

headers = {
    "Authorization": f"Token {SERVICE_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "cause": "Manual test from Python"
}

response = requests.post(
    url,
    headers=headers,
    json=payload
)

print("Status Code:", response.status_code)
print(response.text)
