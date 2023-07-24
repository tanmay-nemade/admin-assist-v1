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
except KeyError as e:
    if e.args[0] == '''st.session_state has no key "Session". Did you forget to initialize it? More info: https://docs.streamlit.io/library/advanced-features/session-state#initialization''':
        st.info('Please Login first using correct credentials')
     