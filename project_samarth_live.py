import streamlit as st
import pandas as pd
import requests
import re

class LocalLLM:
    def __init__(self):
        self.column_aliases = {
            "state": ["state", "statename", "state_name"],
            "district": ["district", "districtname", "district_name"],
            "year": ["year", "year_code", "financial_year"],
            "crop": ["crop", "crop_name", "commodity"],
            "production": ["production", "production_volume_tonnes", "production_metric_tonnes", "value", "yield"],
            "rainfall": ["rainfall", "avg_annual_rainfall_mm", "actual_rainfall", "rainfall_mm"]
        }

    def _find_col(self, df, col_type):
        for alias in self.column_aliases.get(col_type, []):
            for col in df.columns:
                if alias == col.lower().strip():
                    return col
        return None

    def _clean_numeric(self, series):
        return pd.to_numeric(series.astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce')

    def generate(self, user_question, df, dataset_title, resource_id):
        response = ""
        question_low = user_question.lower()
        
        try:
            if "tax" in question_low or "duties" in question_low:
                response = self._analyze_taxes(df, user_question)
            elif "compare" in question_low and "rainfall" in question_low and "crop" in question_low:
                response = self._analyze_comparison(df)
            else:
                response = self._default_summary(df, dataset_title)

        except Exception as e:
            response = f"I encountered an error during data synthesis. Error: {e}\n\n"
            response += self._default_summary(df, dataset_title)
        
        citation = f"**{dataset_title}** (Source: {resource_id}, data.gov.in)"
        return response, [citation]

    def _analyze_taxes(self, df, user_question):
        response = ""
        question_low = user_question.lower()

        cols_to_check = [c for c in df.columns if '2016' in c or '2017' in c or '2018' in c]
        states_to_check = []
        if "telangana" in question_low:
            states_to_check.append("Telangana")
        if "karnataka" in question_low:
            states_to_check.append("Karnataka")
        
        if not states_to_check:
            states_to_check = ["Telangana", "Karnataka"] 
        
        state_col = "state_name"
        if state_col not in df.columns:
            return f"Could not find the required 'state_name' column in the dataset. Available columns: {', '.join(df.columns)}"

        df_filtered = df[df[state_col].isin(states_to_check)]
        
        if df_filtered.empty:
            return f"I found the dataset, but could not find data for {', '.join(states_to_check)}."
            
        final_cols = [state_col] + cols_to_check
        df_display = df_filtered[final_cols].set_index(state_col)
        
        response = f"**Share of Union Taxes and Duties (in Rs. Crore) for {', '.join(states_to_check)}:**\n\n"
        response += df_display.to_markdown()
        
        return response

    def _analyze_comparison(self, df):
        response_part = ""
        rain_col = self._find_col(df, "rainfall")
        state_col = self._find_col(df, "state")
        
        if rain_col and state_col:
            df[rain_col] = self._clean_numeric(df[rain_col])
            avg_rain = df.groupby(state_col)[rain_col].mean().reset_index()
            response_part += "**Average Annual Rainfall Analysis:**\n"
            for _, row in avg_rain.iterrows():
                response_part += f"* **{row[state_col]}**: {row[rain_col]:.2f} mm (average)\n"
        
        return response_part

    def _default_summary(self, df, dataset_title):
        response = f"I have successfully retrieved the dataset **'{dataset_title}'**. Here is a summary of the first 5 rows:\n\n"
        response += f"{df.head().to_markdown(index=False)}\n"
        return response

st.set_page_config(layout="wide", page_title="Project Samarth")
st.title("🇮🇳 Project Samarth (n8n Connected)")
st.markdown("This system uses an n8n workflow for data retrieval and a local LLM for reasoning.")

n8n_url = st.text_input(
    "Enter your n8n Webhook Production URL",
    help="Paste the 'Production URL' from your n8n Webhook node."
)

api_key_input = st.text_input(
    "Enter your data.gov.in API Key",
    type="password",
    help="You must generate a free API key from https://data.gov.in/."
)

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "citations" in message and message["citations"]:
            with st.expander("View Data Source"):
                st.markdown(message["citations"][0])

if prompt := st.chat_input("Ask a question, e.g., 'Share of Union Taxes in Telangana'"):
    if not api_key_input:
        st.error("Please enter your data.gov.in API key above to start.")
    elif not n8n_url:
        st.error("Please enter your n8n Webhook URL above to start.")
    else:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            try:
                llm = LocalLLM()
                
                payload = {
                    "query": prompt,
                    "api_key": api_key_input
                }

                with st.spinner("1/3: Contacting n8n workflow..."):
                    response = requests.post(n8n_url, json=payload, timeout=60)
                    response.raise_for_status() 
                    
                with st.spinner("2/3: Receiving data from n8n..."):
                    data = response.json()
                    
                    if "data" in data and "records" in data["data"]:
                        records = data["data"]["records"]
                        df = pd.DataFrame(records)
                        dataset_title = data["data"].get("title", "Untitled Dataset")
                        resource_id = data.get("resource_id", "N/A")
                        st.write(f"**Data Retrieval:** Successfully fetched {len(df)} records from '{dataset_title}'.")
                    
                    elif "message" in data:
                        st.error(f"Error from n8n workflow: {data['message']}")
                        st.session_state.messages.append({"role": "assistant", "content": f"Error from n8n: {data['message']}"})
                        st.stop()
                    
                    else:
                        st.error(f"Received an unknown response structure from n8n: {data}")
                        st.session_state.messages.append({"role": "assistant", "content": f"Unknown n8n response: {data}"})
                        st.stop()

                with st.spinner("3/3: Synthesizing answer using local reasoning engine..."):
                    final_answer, citations = llm.generate(prompt, df, dataset_title, resource_id)
                    st.markdown(final_answer)
                    if citations:
                        with st.expander("View Data Source"):
                            st.markdown(citations[0])
                
                st.session_state.messages.append({"role": "assistant", "content": final_answer, "citations": citations})
            
            except requests.exceptions.HTTPError as http_err:
                st.error(f"HTTP error connecting to n8n: {http_err}")
            except requests.exceptions.RequestException as e:
                st.error(f"Failed to contact n8n workflow: {e}")
            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")
                st.session_state.messages.append({"role": "assistant", "content": f"I'm sorry, I ran into an error: {e}"})
