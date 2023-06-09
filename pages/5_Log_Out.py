import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import *

st.set_page_config(
    layout="wide",
    page_title="Log Out"
)

st.title('Log Out')

try:
    session = st.session_state['Session']
    if session:
            if st.button('Log Out'):
                session.close()
                st.error('Logged Out Successfully')
except KeyError:
     st.info('Please Login first using correct credentials')
     