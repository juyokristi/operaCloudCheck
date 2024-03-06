import streamlit as st
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO
import time

# UI setup
st.title('Opera Cloud PMS Data Checking Tool')

# Input fields
hostname = st.text_input('Hostname')
x_app_key = st.text_input('X-App-Key')
client_id = st.text_input('Client ID')
client_secret = st.text_input('Client Secret', type='password')
username = st.text_input('Username')
password = st.text_input('Password', type='password')
ext_system_code = st.text_input('External System Code')
hotel_id = st.text_input('Hotel ID')
start_date = st.date_input('Start Date')
end_date = st.date_input('End Date')

def authenticate():
    url = f"{hostname}/oauth/v1/tokens"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'x-app-key': x_app_key,
        'Authorization': 'Basic ' + requests.auth._basic_auth_str(client_id, client_secret),
    }
    data = {
        'username': username,
        'password': password,
        'grant_type': 'password'
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        st.error(f'Authentication failed: {response.status_code} - {response.text}')
        return None

def start_async_process(token):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'x-app-key': x_app_key,
        'x-hotelId': hotel_id
    }
    data = {
        "dateRangeStart": start_date.strftime("%Y-%m-%d"),
        "dateRangeEnd": end_date.strftime("%Y-%m-%d"),
        "roomTypes": [""]
    }
    url = f"{hostname}/inv/async/v1/externalSystems/{ext_system_code}/hotels/{hotel_id}/revenueInventoryStatistics"
    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 202:
        return response.headers.get('Location')
    else:
        st.error(f"Failed to start asynchronous process: {response.status_code} - {response.text}")
        return None

def wait_for_data_ready(location_url, token):
    headers = {
        'Authorization': f'Bearer {token}',
        'x-app-key': x_app_key,
        'x-hotelId': hotel_id
    }
    retry_interval = 10  # seconds
    while True:
        response = requests.head(location_url, headers=headers)
        if response.status_code == 201:
            return True
        elif response.status_code in [202, 404]:  # Continue retrying if processing or not found
            time.sleep(retry_interval)
        else:
            st.error(f"Error checking data readiness: {response.status_code} - {response.reason}")
            return False

def retrieve_data(location_url, token):
    headers = {
        'Authorization': f'Bearer {token}',
        'x-app-key': x_app_key,
        'x-hotelId': hotel_id
    }
    response = requests.get(location_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Failed to retrieve data: {response.status_code} - {response.reason}")
        return None

def data_to_excel(data):
    df = pd.json_normalize(data, 'revInvStats')
    excel_file = BytesIO()
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_data = excel_file.getvalue()
    st.download_button(label='Download Excel file', data=excel_data, file_name='report.xlsx', mime='application/vnd.ms-excel')

if st.button('Retrieve Data'):
    token = authenticate()
    if token:
        location_url = start_async_process(token)
        if location_url and wait_for_data_ready(location_url, token):
            data = retrieve_data(location_url, token)
            if data:
                data_to_excel(data)
