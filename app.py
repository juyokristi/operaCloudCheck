import streamlit as st
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO
import time

# Streamlit UI
st.title('Opera Cloud PMS Data Checking Tool')

hostname = st.text_input('Hostname')
client_id = st.text_input('Client ID')
client_secret = st.text_input('Client Secret')
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
        'Authorization': f'Basic {client_id}:{client_secret}',
    }
    data = {
        'username': username,
        'password': password,
        'grant_type': 'password'
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        st.error('Authentication failed')
        return None

def async_data_request(token):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'x-app-key': client_id,
        'x-hotelId': hotel_id
    }
    data = {
        "dateRangeStart": start_date.strftime("%Y-%m-%d"),
        "dateRangeEnd": end_date.strftime("%Y-%m-%d"),
        "roomTypes": [""]
    }
    url = f"{hostname}/inv/async/v1/externalSystems/{ext_system_code}/hotels/{hotel_id}/revenueInventoryStatistics"
    post_response = requests.post(url, json=data, headers=headers)
    if post_response.status_code == 202:
        location_url = post_response.headers['Location']
        return location_url
    else:
        st.error("Failed to initiate data retrieval")
        return None

def check_async_status(location_url, token):
    headers = {
        'Authorization': f'Bearer {token}',
        'x-app-key': client_id,
        'x-hotelId': hotel_id
    }
    while True:
        head_response = requests.head(location_url, headers=headers)
        if head_response.status_code == 201:
            return location_url
        elif head_response.status_code == 202:
            time.sleep(10)  # Wait before retrying
        else:
            st.error("Error checking data retrieval status")
            return None

def get_data(location_url, token):
    headers = {
        'Authorization': f'Bearer {token}',
        'x-app-key': client_id,
        'x-hotelId': hotel_id
    }
    response = requests.get(location_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Failed to retrieve data")
        return None

def data_to_excel(data):
    df = pd.json_normalize(data, 'revInvStats')
    excel_file = BytesIO()
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_data = excel_file.getvalue()
    st.download_button(label='Download Excel file',
                       data=excel_data,
                       file_name='report.xlsx',
                       mime='application/vnd.ms-excel')

if st.button('Retrieve Data'):
    token = authenticate()
    if token:
        location_url = async_data_request(token)
        if location_url:
            final_location_url = check_async_status(location_url, token)
            if final_location_url:
                data = get_data(final_location_url, token)
                if data:
                    # Display warning/note message before download button
                    st.markdown("""
                        **Note:** The data downloaded reflects the current state and may differ from scheduled data retrieval times, such as those around the night audit. Discrepancies may arise.
                    """)
                    data_to_excel(data)

