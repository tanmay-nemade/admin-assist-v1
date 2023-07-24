# Import python packages
import configparser
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
import streamlit as st
import pandas as pd

# Get the current credentials
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')
    

#Function to select the Snowflake Account
#@st.cache_resource(experimental_allow_widgets=True)
def sfAccount_selector(_config):
    #setup config.ini read rules
 
    account = st.selectbox("Select an account to connect",['Infosys','CMACGM','Personal Account'])
    if account == 'Infosys':
        sfAccount = _config['Accounts']['Infosys']
    elif account == 'CMACGM':
        sfAccount = _config['Accounts']['Infosys']
    else:
        sfAccount = st.text_input("Enter Account")
    st.session_state['sfAccount'] = sfAccount
    sfUser = st.text_input("Enter Username")
    st.session_state['sfUser'] = sfUser    
    sfPass = st.text_input("Enter Password",type='password')
    st.session_state['sfPass'] = sfPass
    if sfUser and sfPass:
        conn = {"account": sfAccount,
                "user": sfUser,
                "password": sfPass}
        if conn is not None:
            return conn

#Function to Create a session using the connection parameters
def switch_page(page_name: str):
    from streamlit.runtime.scriptrunner import RerunData, RerunException
    from streamlit.source_util import get_pages
    def standardize_name(name: str) -> str:
        return name.lower().replace("_", " ")
    page_name = standardize_name(page_name)
    pages = get_pages("2_help_page.py")  # OR whatever your main page is called
    for page_hash, config in pages.items():
        if standardize_name(config["page_name"]) == page_name:
            raise RerunException(
                RerunData(
                    page_script_hash=page_hash,
                    page_name=page_name,
                )
            )
    page_names = [standardize_name(config["page_name"]) for config in pages.values()]
    raise ValueError(f"Could not find page {page_name}. Must be one of {page_names}")

def session_builder(conn):
    session = Session.builder.configs(conn).create()
    return session

st.set_page_config(
    layout="wide",
)

st.title('Login')
st.info('Login on this screen to use other tools seemlessly. Please refresh the page while switching between accounts')

conn = sfAccount_selector(config)
connect = st.button('Connect')
if connect:
    try:
        session = session_builder(conn)
        st.session_state['Session'] = session
        if session:
            st.success('Connection Successful')
            switch_page("Help_page")  
    except snowflake.connector.errors.DatabaseError as e:
        if 'Your free trial has ended and all of your virtual warehouses have been suspended. Add billing information in the Snowflake web UI to continue using the full set of Snowflake features.' in e.args[0]:
            st.write('Your Free Trial has expired. Use a different account')
        if e.args[0] == '''Failed to connect to DB: wy20041.ap-southeast-1.snowflakecomputing.com:443. Incorrect username or password was specified.''':
            st.error('Connection Unsuccessful. Incorect personal account id, username or password or expired personal account. Please refresh the page')
            st.info('Note: 3 unsucessful attempts to login temporarily locks the account. In this case, try to login after 15 minutes')
    # except snowflake.connector.errors.OperationalError:
    #     st.error('Connection Unsuccessful. Please turn off Zscaler and refresh the page')
    # except:
    #     st.error('Connection Unsuccessful. Please refresh the page and try again with correct details')
    

        
