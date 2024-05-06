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

# Streamlit app layout
st.title('Opera Cloud PMS Data Checking Tool')

# JSON configuration input and Submit button
json_input = st.empty()  # Create an empty placeholder for dynamic layout management
json_config = json_input.text_area("Paste your configuration JSON here:", placeholder=placeholder_json, height=100)
submit_json = st.button('Submit JSON')

# Process and validate JSON when submitted
if submit_json:
    if not json_config.strip().startswith('{'):
        json_config = '{' + json_config + '}'
    try:
        config_data = json.loads(json_config)
        st.session_state['config_data'] = config_data  # Store in session state if further processing is needed
        st.success("JSON loaded successfully!")
    except json.JSONDecodeError:
        st.error("Invalid JSON format. Please correct it and try again.")

# Display forms even before JSON input
col1, col2 = st.columns([2, 1])

with col1:
    auth_data = st.session_state.get('config_data', {}).get('authentication', {})
    x_app_key = st.text_input('X-App-Key', value=auth_data.get('xapikey', ''))
    client_id = st.text_input('Client ID', value=auth_data.get('clientId', ''))
    hostname = st.text_input('Hostname', value=auth_data.get('hostname', ''))
    password = st.text_input('Password', value=auth_data.get('password', ''), type='password')
    username = st.text_input('Username', value=auth_data.get('username', ''))
    client_secret = st.text_input('Client Secret', value=auth_data.get('clientSecret', ''), type='password')
    ext_system_code = st.text_input('External System Code', value=auth_data.get('externalSystemId', ''))

with col2:
    hotel_id = st.text_input('Hotel ID', key="hotel_id")
    start_date = st.date_input('Start Date', key="start_date")
    end_date = st.date_input('End Date', key="end_date")
    retrieve_button = st.button('Retrieve Data', key='retrieve')

# Additional feature: Upload and compare CSV data
st.subheader("Upload and Compare CSV Data")
uploaded_file = st.file_uploader("Choose a CSV file for comparison", type=["csv"])
if uploaded_file is not None:
    comparison_df = pd.read_csv(uploaded_file)
    st.write("Comparison CSV loaded successfully!")

    if 'all_data' in st.session_state:
        # Assuming all_data is a list of dictionaries with each dictionary representing data from a different date range
        all_data_df = pd.concat([pd.DataFrame(data) for data in st.session_state['all_data']], ignore_index=True)
        all_data_df['arrivalDate'] = pd.to_datetime(all_data_df['arrivalDate'])

        # Process comparison DataFrame
        comparison_df['occupancyDate'] = pd.to_datetime(comparison_df['occupancyDate'])
        merged_df = pd.merge(all_data_df, comparison_df, left_on='arrivalDate', right_on='occupancyDate', how='inner', suffixes=('_retrieved', '_comparison'))

        # Calculate differences
        merged_df['RN_Difference'] = merged_df['roomSold_retrieved'] - merged_df['roomSold_comparison']
        merged_df['Revenue_Difference'] = merged_df['roomRevenue_retrieved'] - merged_df['roomRevenue_comparison']

        # Display results
        display_columns = ['arrivalDate', 'roomSold_retrieved', 'roomSold_comparison', 'RN_Difference', 'roomRevenue_retrieved', 'roomRevenue_comparison', 'Revenue_Difference']
        st.table(merged_df[display_columns])
    else:
        st.error("Please retrieve data from the PMS before uploading a comparison file.")

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
        return response.headers.get('Location')
    else:
        st.error(f"Failed to start asynchronous process: {response.status_code} - {response.text}")
        return None

def wait_for_data_ready(location_url, token, x_key, h_id):
    headers = {
        'Authorization': f'Bearer {token}',
        'x-app-key': x_key,
        'x-hotelId': h_id
    }
    while True:
        response = requests.head(location_url, headers=headers)
        if response.status_code == 201:
            return response.headers.get('Location')
        elif response.status_code in [200, 202, 404]:
            time.sleep(10)
        else:
            st.error(f"Error checking data readiness: {response.status_code} - {response.reason}")
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

def data_to_excel(all_data, h_id, s_date, e_date):
    df = pd.concat([pd.json_normalize(data, 'revInvStats') for data in all_data], ignore_index=True)
    excel_file = BytesIO()
    filename = f"statistics_{h_id}_{s_date.strftime('%Y-%m-%d')}_{e_date.strftime('%Y-%m-%d')}.xlsx"
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_data = excel_file.getvalue()
    st.download_button(label='Download Excel file', data=excel_data, file_name=filename, mime='application/vnd.ms-excel')
    st.success("Your report is ready!")

if retrieve_button:
    with st.spinner('Processing... Please wait.'):
        token = authenticate(hostname, x_app_key, client_id, client_secret, username, password)
        if token:
            date_ranges = split_date_range(start_date, end_date)
            all_data = []
            for s_date, e_date in date_ranges:
                initial_location_url = start_async_process(token, hostname, x_app_key, hotel_id, ext_system_code, s_date, e_date)
                if initial_location_url:
                    final_location_url = wait_for_data_ready(initial_location_url, token, x_app_key, hotel_id)
                    if final_location_url:
                        data = retrieve_data(final_location_url, token, x_app_key, hotel_id)
                        if data:
                            all_data.append(data)

            if all_data:
                data_to_excel(all_data, hotel_id, start_date, end_date)
