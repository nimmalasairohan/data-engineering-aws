import requests

ACCOUNT_ID = "70506183137352"
JOB_ID = "70506183132521"
SERVICE_TOKEN = "dbtc_0wxqBcQ9nCy38xMZUOPoyVzGftIr_tAYSG9l8oe9evf3AI2gYE"

url = f"https://kw833.us1.dbt.com/api/v2/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/"

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