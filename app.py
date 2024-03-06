import streamlit as st
import requests
import pandas as pd
from io import BytesIO
import json
import time

# Define placeholder JSON for user guidance
placeholder_json = '''{
  "authentication": {
    "xapikey": "replace_with_your_xapikey",
    "clientId": "replace_with_your_clientId",
    "hostname": "replace_with_your_hostname",
    "password": "replace_with_your_password",
    "username": "replace_with_your_username",
    "clientSecret": "replace_with_your_clientSecret",
    "externalSystemId": "replace_with_your_externalSystemId"
  }
}'''

# Streamlit app layout
st.title('Opera Cloud PMS Data Checking Tool')

# Text area for JSON configuration input
json_config = st.text_area("Paste your configuration JSON here:", placeholder=placeholder_json, height=100)

# Layout
col1, col2 = st.columns([2, 1])

with col1:
    # Parse JSON configuration and display inputs
    if json_config:
        try:
            # Attempt to load the provided JSON
            config_data = json.loads(json_config)
            # Retrieve the authentication details
            authentication = config_data['authentication']
            # Display inputs with prefilled data from JSON
            x_app_key = st.text_input('X-App-Key', value=authentication.get('xapikey', ''))
            client_id = st.text_input('Client ID', value=authentication.get('clientId', ''))
            hostname = st.text_input('Hostname', value=authentication.get('hostname', ''))
            password = st.text_input('Password', value=authentication.get('password', ''), type='password')
            username = st.text_input('Username', value=authentication.get('username', ''))
            client_secret = st.text_input('Client Secret', value=authentication.get('clientSecret', ''), type='password')
            ext_system_code = st.text_input('External System Code', value=authentication.get('externalSystemId', ''))
        except json.JSONDecodeError:
            st.error('JSON format error.')
            x_app_key = st.text_input('X-App-Key')
            client_id = st.text_input('Client ID')
            hostname = st.text_input('Hostname')
            password = st.text_input('Password', type='password')
            username = st.text_input('Username')
            client_secret = st.text_input('Client Secret', type='password')
            ext_system_code = st.text_input('External System Code')
    else:
        x_app_key = st.text_input('X-App-Key')
        client_id = st.text_input('Client ID')
        hostname = st.text_input('Hostname')
        password = st.text_input('Password', type='password')
        username = st.text_input('Username')
        client_secret = st.text_input('Client Secret', type='password')
        ext_system_code = st.text_input('External System Code')

with col2:
    # Inputs for initiating the data retrieval
    hotel_id = st.text_input('Hotel ID', key="hotel_id")
    start_date = st.date_input('Start Date', key="start_date")
    end_date = st.date_input('End Date', key="end_date")
    retrieve_button = st.button('Retrieve Data', key='retrieve')

# Function to authenticate and get token
def authenticate(host, x_key, client, secret, user, passw):
    url = f"{host}/oauth/v1/tokens"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'x-app-key': x_key,
        'Authorization': 'Basic ' + requests.auth._basic_auth_str(client, secret),
    }
    data = {
        'username': user,
        'password': passw,
        'grant_type': 'password'
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        st.error(f'Authentication failed: {response.text}')
        return None

# Function to start async process
def start_async_process(token, host, x_key, h_id, ext_code, s_date, e_date):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'x-app-key': x_key,
        'x-hotelId': h_id
    }
    data = {
        "dateRangeStart": s_date.strftime("%Y-%m-%d"),
        "dateRangeEnd": e_date.strftime("%Y-%m-%d"),
        "roomTypes": [""]
    }
    url = f"{host}/inv/async/v1/externalSystems/{ext_code}/hotels/{h_id}/revenueInventoryStatistics"
    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 202:
        return response.headers.get('Location')  # Location (1)
    else:
        st.error(f"Failed to start asynchronous process: {response.status_code} - {response.text}")
        return None

# Function to wait for data ready
def wait_for_data_ready(location_url, token, x_key, h_id):
    headers = {
        'Authorization': f'Bearer {token}',
        'x-app-key': x_key,
        'x-hotelId': h_id
    }
    while True:
        response = requests.head(location_url, headers=headers)
        if response.status_code == 201:
            return response.headers.get('Location')  # Location (2) for GET request
        elif response.status_code in [202, 404]:
            time.sleep(10)  # Retry every 10 seconds
        else:
            st.error(f"Error checking data readiness: {response.status_code} - {response.reason}")
            return None

# Function to retrieve data
def retrieve_data(location_url, token, x_key, h_id):
    headers = {
        'Authorization': f'Bearer {token}',
        'x-app-key': x_key,
        'x-hotelId': h_id
    }
    response = requests.get(location_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Failed to retrieve data: {response.status_code} - {response.reason}")
        return None

# Function to download data as an Excel file
def data_to_excel(data, h_id, s_date, e_date):
    df = pd.json_normalize(data, 'revInvStats')
    excel_file = BytesIO()
    filename = f"statistics_{h_id}_{s_date.strftime('%Y-%m-%d')}_{e_date.strftime('%Y-%m-%d')}.xlsx"
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_data = excel_file.getvalue()
    st.download_button(label='Download Excel file', data=excel_data, file_name=filename, mime='application/vnd.ms-excel')
    st.success("Your report is ready!")

# Data retrieval process
if retrieve_button:
    # Progress bar setup
    with st.spinner('Processing... Please wait.'):
        # Authentication
        token = authenticate(hostname, x_app_key, client_id, client_secret, username, password)
        if token:
            # Start async process
            initial_location_url = start_async_process(token, hostname, x_app_key, hotel_id, ext_system_code, start_date, end_date)
            if initial_location_url:
                # Wait for data ready
                final_location_url = wait_for_data_ready(initial_location_url, token, x_app_key, hotel_id)
                if final_location_url:
                    # Retrieve data
                    data = retrieve_data(final_location_url, token, x_app_key, hotel_id)
                    if data:
                        # Download data as an Excel file
                        data_to_excel(data, hotel_id, start_date, end_date)
