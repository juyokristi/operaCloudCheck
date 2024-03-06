import streamlit as st
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO
import time

# Streamlit UI
st.title('Opera Cloud PMS Data Checking Tool')

hostname = st.text_input('Hostname', help='Enter the API Hostname.')
x_app_key = st.text_input('X-App-Key', help='Enter the x-app-key specific to the hotel.')
client_id = st.text_input('Client ID', help='Enter the Client ID for Basic Auth.')
client_secret = st.text_input('Client Secret', help='Enter the Client Secret for Basic Auth.', type='password')
username = st.text_input('Username', help='Enter your username for authentication.')
password = st.text_input('Password', help='Enter your password for authentication.', type='password')
ext_system_code = st.text_input('External System Code', help='Enter the External System Code.')
hotel_id = st.text_input('Hotel ID', help='Enter the Hotel ID.')
start_date = st.date_input('Start Date', help='Select the start date for the report.')
end_date = st.date_input('End Date', help='Select the end date for the report.')

def authenticate():
    url = f"{hostname}/oauth/v1/tokens"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'x-app-key': x_app_key,
        'Authorization': f'Basic {client_id}:{client_secret}',
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
        st.error(f'Authentication failed: {response.text}')
        return None

def async_data_request(token):
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
    post_response = requests.post(url, json=data, headers=headers)
    if post_response.status_code == 202:
        location_url = post_response.headers.get('Location')
        return location_url
    else:
        st.error(f"Failed to initiate data retrieval: {post_response.text}")
        return None

def check_async_status(location_url, token):
    headers = {
        'Authorization': f'Bearer {token}',
        'x-app-key': x_app_key,
        'x-hotelId': hotel_id
    }
    while True:
        head_response = requests.head(location_url, headers=headers)
        if head_response.status_code == 201:
            return location_url
        elif head_response.status_code == 202:
            time.sleep(10)  # Wait before retrying
        else:
            st.error(f"Error checking data retrieval status: {head_response.text}")
            return None

def get_data(location_url, token):
    headers = {
        'Authorization': f'Bearer {token}',
        'x-app-key': x_app_key,
        'x-hotelId': hotel_id
    }
    response = requests.get(location_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Failed to retrieve data: {response.text}")
        return None

def data_to_excel(data):
    df = pd.json_normalize(data, 'revInvStats')
    excel_file = BytesIO()
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_data = excel_file.getvalue()
    
    # Display the warning/note message
    st.markdown("""
    **Note:** This report will download the data as of this moment. Juyo is scheduled to retrieve the data around night audit time, therefore discrepancies may arise.
    """, unsafe_allow_html=True)

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
                    data_to_excel(data)
