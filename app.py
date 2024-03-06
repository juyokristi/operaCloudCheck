import streamlit as st
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO
import time

# Function definitions
def authenticate(hostname, x_app_key, client_id, client_secret, username, password):
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

def start_async_process(token, hostname, x_app_key, hotel_id, ext_system_code, start_date, end_date):
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
        return response.headers.get('Location')  # Location (1)
    else:
        st.error(f"Failed to start asynchronous process: {response.status_code} - {response.text}")
        return None

def wait_for_data_ready(location_url, token, x_app_key, hotel_id):
    headers = {
        'Authorization': f'Bearer {token}',
        'x-app-key': x_app_key,
        'x-hotelId': hotel_id
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

def retrieve_data(location_url, token, x_app_key, hotel_id):
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

def data_to_excel(data, hotel_id, start_date, end_date):
    df = pd.json_normalize(data, 'revInvStats')
    excel_file = BytesIO()
    filename = f"statistics_{hotel_id}_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.xlsx"
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_data = excel_file.getvalue()
    st.download_button(label='Download Excel file', data=excel_data, file_name=filename, mime='application/vnd.ms-excel')
    st.success("Your report is ready!")

# UI setup
st.title('Opera Cloud PMS Data Checking Tool')

# Splitting the layout into two columns
col1, col2 = st.columns([1, 2])

with col1:
    # Configuration inputs
    st.write("## Configuration")
    hostname = st.text_input('Hostname', key="hostname")
    x_app_key = st.text_input('X-App-Key', key="x_app_key")
    client_id = st.text_input('Client ID', key="client_id")
    client_secret = st.text_input('Client Secret', key="client_secret", type='password')
    username = st.text_input('Username', key="username")
    password = st.text_input('Password', key="password", type='password')
    ext_system_code = st.text_input('External System Code', key="ext_system_code")
    hotel_id = st.text_input('Hotel ID', key="hotel_id")

with col2:
    # Data retrieval inputs
    st.write("## Retrieve Data")
    start_date = st.date_input('Start Date', key="start_date")
    end_date = st.date_input('End Date', key="end_date")
    if st.button('Retrieve Data', key="retrieve", help="Click to retrieve data"):
        # Display progress bar and messages
        with st.spinner('Please wait, your report is being prepared...'):
            progress_bar = st.progress(0)
            token = authenticate(hostname, x_app_key, client_id, client_secret, username, password)
            if token:
                progress_bar.progress(25)
                initial_location_url = start_async_process(token, hostname, x_app_key, hotel_id, ext_system_code, start_date, end_date)
                if initial_location_url:
                    progress_bar.progress(50)
                    final_location_url = wait_for_data_ready(initial_location_url, token, x_app_key, hotel_id)
                    if final_location_url:
                        progress_bar.progress(75)
                        data = retrieve_data(final_location_url, token, x_app_key, hotel_id)
                        if data:
                            progress_bar.progress(100)
                            data_to_excel(data, hotel_id, start_date, end_date)
