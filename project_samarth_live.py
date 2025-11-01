import streamlit as st
import requests
import pandas as pd
import re
import json

# -----------------------------------------------------------------
# !! IMPORTANT !!
# This is the "Production URL" from your n8n Webhook node
# Make sure your n8n workflow is "Active"
N8N_WEBHOOK_URL = "https://ghost-n8n-4baj.onrender.com/webhook/samarth-query" 
# -----------------------------------------------------------------

st.set_page_config(layout="wide", page_title="Project Samarth (n8n)")
st.title("🇮🇳 Project Samarth (Powered by n8n)")
st.write("This app uses a live `data.gov.in` API. You must provide your own API key to use it. The n8n workflow must be set to 'Active'.")

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if isinstance(message["content"], pd.DataFrame):
            st.dataframe(message["content"], use_container_width=True)
        else:
            st.markdown(message["content"])

api_key_input = st.text_input(
    "Enter your data.gov.in API Key",
    type="password",
    help="Get your API key by registering on data.gov.in"
)

def analyze_tax_data(df, query):
    try:
        df = df.apply(pd.to_numeric, errors='ignore')
        
        states_to_find = []
        if "telangana" in query:
            states_to_find.append("Telangana")
        if "karnataka" in query:
            states_to_find.append("Karnataka")

        if not states_to_find:
            states_to_find = ["Telangana", "Karnataka"] 

        years_to_find = []
        year_matches = re.findall(r'(\d{4})', query)
        if year_matches:
            years_to_find = [f"{y}-{int(y[2:])+1}" for y in year_matches]
        
        if "2016-2018" in query or not years_to_find:
             years_to_find = ["2016-17", "2017-18"]

        filtered_df = df[df['state_name'].isin(states_to_find)]
        
        if filtered_df.empty:
            return "Could not find data for the specified states. The dataset includes: " + ", ".join(df['state_name'].unique())

        cols_to_sum = ['state_name'] + years_to_find
        
        missing_cols = [col for col in cols_to_sum if col not in df.columns]
        if missing_cols:
            return f"The dataset is missing the following year columns: {', '.join(missing_cols)}. Available years are: {', '.join(df.columns[2:])}"

        analysis_df = filtered_df[cols_to_sum].copy()
        
        analysis_df[years_to_find] = analysis_df[years_to_find].apply(pd.to_numeric, errors='coerce').fillna(0)
        
        analysis_df['Total_Devolution_ (2016-18)'] = analysis_df[years_to_find].sum(axis=1)
        
        response = f"**Analysis of Union Taxes and Duties Devolved (Figures in Rs. Crore):**\n\n"
        response += analysis_df.to_markdown(index=False)
        
        return response, analysis_df

    except Exception as e:
        return f"An error occurred during data analysis: {e}\n\n**Raw Data:**\n{df.head().to_markdown()}", None

def analyze_generic_data(df, title):
    response = f"**Successfully fetched {len(df)} records for '{title}':**\n\n"
    response += df.to_markdown(index=False)
    return response, df

if prompt := st.chat_input("Ask a question, e.g., 'Share of Union Taxes'"):
    if not api_key_input:
        st.error("Please enter your data.gov.in API key.")
    elif N8N_WEBHOOK_URL == "YOUR_N8N_PRODUCTION_URL_HERE":
        st.error("Please update the N8N_WEBHOOK_URL in the Python code.")
    else:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            try:
                payload = {
                    "query": prompt,
                    "api_key": api_key_input
                }
                
                with st.spinner("Contacting n8n workflow to find and fetch data..."):
                    response = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=30)
                    response.raise_for_status() 
                    data = response.json()
                
                if "error" in data:
                    st.error(f"Error from n8n workflow: {data.get('message', data.get('error'))}")
                    st.session_state.messages.append({"role": "assistant", "content": f"Error: {data.get('message', data.get('error'))}"})
                
                elif "data" in data and "records" in data["data"]:
                    records = data["data"]["records"]
                    df = pd.DataFrame(records)
                    title = data["data"].get("title", "Dataset")
                    
                    analysis_response = ""
                    analysis_df = None

                    if "tax" in prompt.lower() or "duties" in prompt.lower():
                        analysis_response, analysis_df = analyze_tax_data(df, prompt.lower())
                    else:
                        analysis_response, analysis_df = analyze_generic_data(df, title)
                    
                    st.markdown(analysis_response)
                    if analysis_df is not None:
                        st.dataframe(analysis_df, use_container_width=True)
                        st.session_state.messages.append({"role": "assistant", "content": analysis_df})
                    else:
                         st.session_state.messages.append({"role": "assistant", "content": analysis_response})

                # *** THIS IS THE NEW, HELPFUL ERROR CHECK ***
                elif "message" in data and data["message"] == "Workflow was started":
                    st.error("Error from n8n: The workflow is not 'Active'. Please go to your n8n UI, toggle the workflow to 'Active', and make sure you are using the 'Production URL'.")
                    st.session_state.messages.append({"role": "assistant", "content": "Error: n8n workflow is not Active. Please Activate the workflow in the n8n UI."})

                else:
                    st.error(f"Error from n8n workflow: An unknown response was received. {json.dumps(data)}")
                    st.session_state.messages.append({"role": "assistant", "content": "Error: Unknown response from n8n."})

            except requests.exceptions.HTTPError as http_err:
                st.error(f"HTTP error occurred: {http_err} - Check if your n8n webhook URL is correct and n8n is running.")
            except requests.exceptions.ConnectionError as conn_err:
                st.error(f"Connection error: {conn_err} - Could not connect to the n8n webhook. Is n8n running and the URL correct?")
            except requests.exceptions.Timeout:
                st.error("The request to n8n timed out. The workflow may be taking too long to execute.")
            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")

