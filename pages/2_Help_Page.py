import streamlit as st
# Retrieve the Snowflake session from the session state
session = st.session_state.get('Session')
# Check if the session exists
if session:
    # Snowflake session is available, user is logged in
    st.title("Help Page")
    st.write("Welcome to the help page!")
    # Add more content as needed
else:
    # Snowflake session is not available, user is not logged in
    st.error("You are not logged in. Please go back to the login page and connect to an account.")