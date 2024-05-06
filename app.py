import streamlit as st
import requests
import pandas as pd
from io import BytesIO
import json
import time
from datetime import timedelta

def authenticate(host, x_key, client, secret, user, passw):
    url = f"{host}/oauth/v1/tokens"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'x-app-key': x_key,
        'Authorization': 'Basic ' + requests.auth._basic_auth_str(client, secret),
    }
    data = {'username': user, 'password': passw, 'grant_type': 'password'}
    response = requests.post(url, headers=headers, data=data)
    return response.json()['access_token'] if response.status_code == 200 else None

def fetch_data(token, host, x_key, h_id, ext_code, s_date, e_date):
    url = f"{host}/inv/async/v1/externalSystems/{ext_code}/hotels/{h_id}/revenueInventoryStatistics"
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {token}', 'x-app-key': x_key, 'x-hotelId': h_id}
    data = {"dateRangeStart": s_date.strftime("%Y-%m-%d"), "dateRangeEnd": e_date.strftime("%Y-%m-%d"), "roomTypes": [""]}
    response = requests.post(url, json=data, headers=headers)
    return response.headers.get('Location') if response.status_code == 202 else None

def wait_for_data_ready(location_url, token, x_key, h_id):
    headers = {'Authorization': f'Bearer {token}', 'x-app-key': x_key, 'x-hotelId': h_id}
    while True:
        response = requests.head(location_url, headers=headers)
        if response.status_code == 201:
            return response.headers.get('Location')
        time.sleep(10)  # Sleep to delay the next request

def retrieve_data(location_url, token, x_key, h_id):
    headers = {'Authorization': f'Bearer {token}', 'x-app-key': x_key, 'x-hotelId': h_id}
    response = requests.get(location_url, headers=headers)
    return response.json() if response.status_code == 200 else None

def data_to_excel(all_data, h_id, s_date, e_date):
    df = pd.concat([pd.json_normalize(data, 'revInvStats') for data in all_data], ignore_index=True)
    excel_file = BytesIO()
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return excel_file.getvalue(), f"statistics_{h_id}_{s_date.strftime('%Y-%m-%d')}_{e_date.strftime('%Y-%m-%d')}.xlsx"

def compare_and_display_results(primary_data, comparison_data):
    primary_df = pd.concat([pd.json_normalize(data, 'revInvStats') for data in primary_data], ignore_index=True)
    primary_df['arrivalDate'] = pd.to_datetime(primary_df['arrivalDate'])
    comparison_data['occupancyDate'] = pd.to_datetime(comparison_data['occupancyDate'], format='%Y-%m-%d')
    merged_df = pd.merge(primary_df, comparison_data, left_on='arrivalDate', right_on='occupancyDate', suffixes=('_set1', '_set2'))
    merged_df['RN_Difference'] = merged_df['roomSold_set1'] - merged_df['roomSold_set2']
    merged_df['Revenue_Difference'] = merged_df['roomRevenue_set1'] - merged_df['roomRevenue_set2']
    return merged_df[['arrivalDate', 'roomSold_set1', 'roomSold_set2', 'RN_Difference', 'roomRevenue_set1', 'roomRevenue_set2', 'Revenue_Difference']]

# App Layout
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

if 'config_data' in st.session_state:
    auth_data = st.session_state['config_data'].get('authentication', {})
    x_app_key = auth_data.get('xapikey', '')
    client_id = auth_data.get('clientId', '')
    hostname = auth_data.get('hostname', '')
    password = auth_data.get('password', '')
    username = auth_data.get('username', '')
    client_secret = auth_data.get('clientSecret', '')
    ext_system_code = auth_data.get('externalSystemId', '')
    hotel_id = st.text_input('Hotel ID')
    start_date = st.date_input('Start Date')
    end_date = st.date_input('End Date')
    retrieve_button = st.button('Retrieve Data')

    if retrieve_button:
        token = authenticate(hostname, x_app_key, client_id, client_secret, username, password)
        if token:
            location_url = fetch_data(token, hostname, x_app_key, hotel_id, ext_system_code, start_date, end_date)
            if location_url:
                final_location_url = wait_for_data_ready(location_url, token, x_app_key, hotel_id)
                if final_location_url:
                    all_data = retrieve_data(final_location_url, token, x_app_key, hotel_id)
                    if all_data:
                        excel_data, filename = data_to_excel(all_data, hotel_id, start_date, end_date)
                        st.download_button('Download Excel file', excel_data, file_name=filename, mime='application/vnd.ms-excel')
                        st.session_state['all_data'] = all_data

# Comparison Feature
st.subheader("Upload Comparison CSV Data")
comparison_file = st.file_uploader("Choose a CSV file", type="csv")
if comparison_file and 'all_data' in st.session_state:
    comparison_df = pd.read_csv(comparison_file)
    result_df = compare_and_display_results(st.session_state['all_data'], comparison_df)
    st.table(result_df)
else:
    st.error("No comparison data uploaded or primary data not available.")
