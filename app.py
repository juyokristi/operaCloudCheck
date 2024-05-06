import streamlit as st
import requests
import pandas as pd
from io import BytesIO
import json
import time
from datetime import timedelta

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

# Function Definitions
def authenticate(host, x_key, client, secret, user, passw):
    url = f"{host}/oauth/v1/tokens"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'x-app-key': x_key,
        'Authorization': 'Basic ' + requests.auth._basic_auth_str(client, secret)
    }
    data = {'username': user, 'password': passw, 'grant_type': 'password'}
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        st.error(f'Authentication failed: {response.text}')
        return None

def fetch_data(token, host, x_key, h_id, ext_code, s_date, e_date):
    url = f"{host}/inv/async/v1/externalSystems/{ext_code}/hotels/{h_id}/revenueInventoryStatistics"
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
    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 202:
        return response.headers.get('Location')
    else:
        st.error(f"Failed to start asynchronous process: {response.status_code} - {response.text}")
        return None

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

def compare_and_display_results(primary_data, comparison_data):
    primary_df = pd.DataFrame(primary_data)
    primary_df['arrivalDate'] = pd.to_datetime(primary_df['arrivalDate'])

    comparison_df = pd.read_csv(comparison_data)
    comparison_df['occupancyDate'] = pd.to_datetime(comparison_df['occupancyDate'], format='%Y-%m-%d')

    merged_df = pd.merge(primary_df, comparison_df, left_on='arrivalDate', right_on='occupancyDate', suffixes=('_set1', '_set2'))
    merged_df['RN_Difference'] = merged_df['roomSold_set1'] - merged_df['roomSold_set2']
    merged_df['Revenue_Difference'] = merged_df['roomRevenue_set1'] - merged_df['roomRevenue_set2']

    display_columns = ['arrivalDate', 'roomSold_set1', 'roomSold_set2', 'RN_Difference', 'roomRevenue_set1', 'roomRevenue_set2', 'Revenue_Difference']
    st.table(merged_df[display_columns])

# Streamlit Application Layout
st.title('Opera Cloud PMS Data Checking Tool')

json_input = st.text_area("Paste your configuration JSON here:", placeholder=placeholder_json, height=100)
submit_json = st.button('Submit JSON')

if submit_json:
    try:
        config_data = json.loads(json_input)
        st.session_state['config_data'] = config_data
        st.success("JSON loaded successfully!")
    except json.JSONDecodeError:
        st.error("Invalid JSON format. Please correct it and try again.")

# Setup authentication and parameters input forms
if 'config_data' in st.session_state:
    auth_data = st.session_state['config_data'].get('authentication', {})
    x_app_key = st.text_input('X-App-Key', value=auth_data.get('xapikey', ''))
    client_id = st.text_input('Client ID', value=auth_data.get('clientId', ''))
    hostname = st.text_input('Hostname', value=auth_data.get('hostname', ''))
    password = st.text_input('Password', value=auth_data.get('password', ''), type='password')
    username = st.text_input('Username', value=auth_data.get('username', ''))
    client_secret = st.text_input('Client Secret', value=auth_data.get('clientSecret', ''), type='password')
    ext_system_code = st.text_input('External System Code', value=auth_data.get('externalSystemId', ''))
    hotel_id = st.text_input('Hotel ID')
    start_date = st.date_input('Start Date')
    end_date = st.date_input('End Date')
    retrieve_button = st.button('Retrieve Data')

    if retrieve_button:
        token = authenticate(hostname, x_app_key, client_id, client_secret, username, password)
        if token:
            location_url = fetch_data(token, hostname, x_app_key, hotel_id, ext_system_code, start_date, end_date)
            if location_url:
                st.write('Data fetching started, please wait for the process to complete.')

# Feature to upload and compare CSV data
st.subheader("Upload Comparison CSV Data")
comparison_file = st.file_uploader("Choose a CSV file", type="csv")
if comparison_file:
    compare_and_display_results(st.session_state.get('all_data', []), comparison_file)
