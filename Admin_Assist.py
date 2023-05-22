# Import python packages
import configparser
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
import streamlit as st

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
    sfUser = st.text_input("Enter Username")
    sfPass = st.text_input("Enter Password",type='password')
    st.write('Press Enter to Login')
    if sfUser and sfPass:
        conn = {"account": sfAccount,
                "user": sfUser,
                "password": sfPass}
        if conn is not None:
    # if st.sidebar.button('Connect'):
            session = session_builder(conn)
            return session

#Function to Create a session using the connection parameters

def session_builder(conn):
    session = Session.builder.configs(conn).create()
    return session
# # Page config must be set

st.set_page_config(
    layout="wide",
)

st.title('Admin Assist Login')
st.info('Login on this screen to use other tools seemlessly')

try:
    session = sfAccount_selector(config)
    st.session_state['Session'] = session
    if session:
        st.success('Connection Successful')    
except snowflake.connector.errors.DatabaseError:
    st.error('Connection Unsuccessful. Please check your personal account id, username or password and refresh the page')
    st.info('Note: 3 unsucessful attempts to login temporarily locks the account. In this case, try to login after 15 minutes')
except snowflake.connector.errors.OperationalError:
    st.error('Connection Unsuccessful. Please turn off Zscaler and refresh the page')
except:
    st.error('Connection Unsuccessful. Please refresh the page and try again with correct details')

