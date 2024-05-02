import streamlit as st
import requests
import pandas as pd
from io import BytesIO
import json
import time
from datetime import timedelta, datetime
import csv

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
st.title('Opera Cloud PMS Data Checking and Comparison Tool')

# JSON configuration input and Submit button
json_config = st.text_area("Paste your configuration JSON here:", placeholder=placeholder_json, height=100)
submit_json = st.button('Submit JSON')

if submit_json:
    # JSON parsing and authentication code
    if not json_config.strip().startswith('{'):
        json_config = '{' + json_config + '}'
    try:
        config_data = json.loads(json_config)
        st.session_state['config_data'] = config_data
        st.success("JSON loaded successfully!")
    except json.JSONDecodeError:
        st.error("Invalid JSON format. Please correct it and try again.")

# Display authentication inputs
auth_data = st.session_state.get('config_data', {}).get('authentication', {})
x_app_key = st.text_input('X-App-Key', value=auth_data.get('xapikey', ''))
client_id = st.text_input('Client ID', value=auth_data.get('clientId', ''))
hostname = st.text_input('Hostname', value=auth_data.get('hostname', ''))
password = st.text_input('Password', value=auth_data.get('password', ''), type='password')
username = st.text_input('Username', value=auth_data.get('username', ''))
client_secret = st.text_input('Client Secret', value=auth_data.get('clientSecret', ''), type='password')
ext_system_code = st.text_input('External System Code', value=auth_data.get('externalSystemId', ''))

hotel_id = st.text_input('Hotel ID', key="hotel_id")
start_date = st.date_input('Start Date', key="start_date")
end_date = st.date_input('End Date', key="end_date")

retrieve_button = st.button('Retrieve Data', key='retrieve')

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

def split_date_range(start_date, end_date, max_days=400):
    ranges = []
    current_start_date = start_date
    while current_start_date < end_date:
        current_end_date = min(current_start_date + timedelta(days=max_days - 1), end_date)
        ranges.append((current_start_date, current_end_date))
        current_start_date = current_end_date + timedelta(days=1)
    return ranges

def load_csv_data(uploaded_file):
    try:
        content = uploaded_file.getvalue().decode("utf-8")
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(content.splitlines()[0])
        juyo_data = pd.read_csv(uploaded_file, delimiter=dialect.delimiter)
        juyo_data['arrivalDate'] = pd.to_datetime(juyo_data['arrivalDate'], errors='coerce')
        st.success("CSV uploaded and processed!")
        return juyo_data
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        return None

# Authenticate and retrieve data
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

# Upload CSV for comparison
st.header("Upload CSV File for Comparison")
uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

compare_button = st.button("Compare Data")
if compare_button and uploaded_file is not None:
    juyo_data = load_csv_data(uploaded_file)
    if juyo_data is not None:
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
                    hf_data = pd.concat([pd.json_normalize(data, 'revInvStats') for data in all_data], ignore_index=True)
                    hf_data['occupancyDate'] = hf_data['occupancyDate'].astype(str)  # Ensure it's string for comparison
                    juyo_data['arrivalDate'] = pd.to_datetime(juyo_data['arrivalDate'], errors='coerce')  # Convert to datetime

                    # Convert 'occupancyDate' column in hf_data to datetime
                    hf_data['occupancyDate'] = pd.to_datetime(hf_data['occupancyDate'])

                    # Merge dataframes
                    merged_data = pd.merge(hf_data, juyo_data, left_on='occupancyDate', right_on='arrivalDate', how='inner')
                    
                    # Select only the desired columns
                    merged_data = merged_data[['occupancyDate', 'roomsSold_x', 'roomsSold_y', 'revNet_x', 'revNet_y']]
                    
                    # Rename columns for clarity
                    merged_data.rename(columns={
                        'roomsSold_x': 'RN HF',
                        'roomsSold_y': 'RN Juyo',
                        'revNet_x': 'Rev HF',
                        'revNet_y': 'Rev Juyo'
                    }, inplace=True)
                    
                    # Calculate differences
                    merged_data['RN Diff'] = merged_data['RN Juyo'] - merged_data['RN HF']
                    merged_data['Rev Diff'] = merged_data['Rev Juyo'] - merged_data['Rev HF']
                    
                    st.subheader("Merged Data")
                    st.write(merged_data)
                    
                    # Download merged data as Excel
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        merged_data.to_excel(writer, index=False)
                    st.download_button(label="Download Merged Data", data=output.getvalue(), file_name="Merged_Data.xlsx", mime="application/vnd.ms-excel")

# Calculate and display stats
if 'merged_data' in locals():
    total_days = len(merged_data)
    discrepancy_days = len(discrepancies)
    st.info(f"Discrepancies found on {discrepancy_days} out of {total_days} days.")

    # Split data into past and future
    today_str = datetime.today().strftime('%Y-%m-%d')
    past_data = merged_data[merged_data['occupancyDate'] < today_str]
    future_data = merged_data[merged_data['occupancyDate'] >= today_str]

    # Calculate accuracy percentages for past and future
    past_rn_accuracy = 1 - (abs(past_data['rn'] - past_data['roomsSold']).sum() / past_data['rn'].sum()) if not past_data.empty else 0
    past_rev_accuracy = 1 - (abs(past_data['revNet'] - past_data['roomRevenue']).sum() / past_data['revNet'].sum()) if not past_data.empty else 0
    future_rn_accuracy = 1 - (abs(future_data['rn'] - future_data['roomsSold']).sum() / future_data['rn'].sum()) if not future_data.empty else 0
    future_rev_accuracy = 1 - (abs(future_data['revNet'] - future_data['roomRevenue']).sum() / future_data['revNet'].sum()) if not future_data.empty else 0

    st.write(f"Past RN accuracy: {past_rn_accuracy:.2%}")
    st.write(f"Past Rev accuracy: {past_rev_accuracy:.2%}")
    st.write(f"Future RN accuracy: {future_rn_accuracy:.2%}")
    st.write(f"Future Rev accuracy: {future_rev_accuracy:.2%}")
