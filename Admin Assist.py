# Import python packages
import os
import configparser
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
import pandas as pd
import streamlit as st

# Get the current credentials
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')
    

#Function to select the Snowflake Account
@st.cache_resource(experimental_allow_widgets=True)
def sfAccount_selector(_config):
    #setup config.ini read rules
 
    account = st.sidebar.selectbox("Select an account to connect",['Infosys','CMACGM','Personal Account'])
    if account == 'Infosys':
        sfAccount = _config['Accounts']['Infosys']
    elif account == 'CMACGM':
        sfAccount = _config['Accounts']['Infosys']
    else:
        sfAccount = st.sidebar.text_input("Enter Account")
    sfUser = st.sidebar.text_input("Enter Username")
    sfPass = st.sidebar.text_input("Enter Password",type='password')
    conn = {"account": sfAccount,
            "user": sfUser,
            "password": sfPass}
    if st.sidebar.button('Connect'):
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

st.title('Admin Assist V1')
@st.cache_data
def get_user_list(_session):
    user_list_df = _session.sql("SELECT *,datediff('day',last_success_login,current_timestamp()) as last_success_login_days FROM snowflake.account_usage.USERS WHERE DELETED_ON IS NULL;").collect()
    pd_user_list_df = pd.DataFrame(user_list_df)
    return pd_user_list_df

def get_actionable_user_list(session,selected_action,pd_user_list_df):
    if selected_action == 'Enable User':
        action_user_list = pd_user_list_df[pd_user_list_df['DISABLED'] == 'true']
        return action_user_list
    if selected_action == 'Disable User':
        action_user_list = pd_user_list_df[pd_user_list_df['DISABLED'] == 'false']
        return action_user_list
    if (selected_action == 'Drop User'):
        return pd_user_list_df
    if selected_action == 'Assign Role':
        return pd_user_list_df
    if selected_action == 'Search User':
        return pd_user_list_df
    #'Enable User','Disable User','Drop User'

def search_user(session,pd_user_list_df):
    #selected_name = pd_user_list_df1[pd_user_list_df1['DISPLAY_NAME'] == selected_user]
    selected_criteria = st.sidebar.selectbox("Select relevant search criteria",['Display Name','Name','Email'])
    selected_value = st.sidebar.text_input("Enter serach keyword")
    if selected_criteria == 'Display Name':
        st.write("You selected Display Name and selected value is ",selected_value)
        #selected_name = pd_user_list_df['DISPLAY_NAME'].str.contains(selected_value, na=False)
        selected_name = pd_user_list_df[pd_user_list_df['DISPLAY_NAME'].str.contains(selected_value, na=False, case = False)]
        st.write(selected_name)
    if selected_criteria == 'Name':
        st.write("You selected Name and selected value is ",selected_value)
        selected_name = pd_user_list_df[pd_user_list_df['NAME'].str.contains(selected_value, na=False)]
        st.write(selected_name)
    if selected_criteria == 'Email':
        st.write("You selected Email and selected value is ",selected_value)
        selected_name = pd_user_list_df[pd_user_list_df['EMAIL'].str.contains(selected_value, na=False)]
        st.write(selected_name)

    selected_id = selected_name['NAME']
    #index1 = selected_name['NAME'].index[selected_name['NAME']].tolist()
    #str1 = selected_name['ALLOWED_VALUES'][index1[0]][1:-1]
    st.write(selected_id)
    
def disable_user(session,name,selected_user):
    #session.sql("ALTER USER {} SET DISABLED = TRUE;".format(name))
    st.write("**:blue[ALTER USER ",name," SET DISABLED = TRUE;]**")
    if st.button("Do you wish to continue ?"):
        st.write("**:blue[User ",selected_user," disabled successfully !]**")

def enable_user(session,name,selected_user):
    #session.sql("ALTER USER {} SET DISABLED = TRUE;".format(name))
    st.write("**:blue[ALTER USER ",name," SET DISABLED = FALSE;]**")
    if st.button("Do you wish to continue ?"):
        st.write("**:blue[User ",selected_user," enabled successfully !]**")

def drop_user(session,name,selected_user):
    #session.sql("ALTER USER {} SET DISABLED = TRUE;".format(name))
    st.write("**:blue[DROP USER ",name,";]**")
    if st.button("Do you wish to continue ?"):
        st.write("**:blue[User ",selected_user," successfully dropped !]**")

#Define a function to get list of all programs and also cache this data for multiple time consumption
#@st.cache_data
def get_program_list(session):
    program_list = session.sql("select distinct tag_value from snowflake.account_usage.tag_references where domain = 'DATABASE' and tag_name = 'PROGRAM' order by tag_value;").collect()
    program_list = [list(row.asDict().values())[0] for row in program_list]
    return program_list

def get_project_list(session):
    project_list = session.sql("select distinct tag_value as projects from snowflake.account_usage.tag_references where tag_name = 'PROJECT' and domain = 'WAREHOUSE' and object_deleted is null and substr(tag_value,1,3) = 'PRJ' order by projects;").collect()
    project_list = [list(row.asDict().values())[0] for row in project_list]
    return project_list


def assign_role(session,selected_name,selected_user):
    program_list = get_program_list(session)
    project_list = get_project_list(session)
    
    selected_program = st.sidebar.selectbox(label = "Select Program for Role assignment",options = program_list)
    selected_env = st.sidebar.selectbox("Select Environment ",["ADM","DEV","OTHERS","PRD","PRE","QUA","QUD","SDB","SHR","UAT"])
    if selected_program != 'PRJ':
        pgm_role_list = session.sql("select a.object_name from snowflake.account_usage.tag_references a\
                                    inner join snowflake.account_usage.tag_references b\
                                    on a.object_name = b.object_name and b.tag_name = 'ENV' and b.tag_value = '{}'\
                                    where a.domain = 'ROLE' and a.tag_name = 'PROGRAM'\
                                    and a.tag_value = '{}'".format(selected_env,selected_program)).collect()
        
        selected_role = st.sidebar.selectbox("Select role from program ",pgm_role_list)
        st.write("**:blue[GRANT ROLE ",selected_role," TO USER ",selected_name,";]**")
        st.button("Do you wish to continue ?")
        
    else:
        selected_project = st.sidebar.selectbox(label = "Select Project for Role assignment",options = project_list)
    
        prj_role_list = session.sql("select a.object_name from snowflake.account_usage.tag_references a\
                                    inner join snowflake.account_usage.tag_references b\
                                    on a.object_name = b.object_name and b.tag_name = 'ENV' and b.tag_value = '{}'\
                                    where a.domain = 'ROLE' and a.tag_name = 'PROJECT'\
                                    and a.tag_value = '{}'".format(selected_env,selected_project)).collect()
        
        selected_role = st.sidebar.selectbox("Select role from program ",prj_role_list)
        st.write("**:blue[GRANT ROLE ",selected_role," TO USER ",selected_name,";]**")
        st.button("Do you wish to continue ?")
    

@st.cache_data
def get_off_size_files(_session):
    file_list_df = _session.sql("SELECT PROGRAM,\
                                    TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || T.TABLE_NAME as TableName,\
                                    count(C.TABLE_ID) as FileCount,\
                                    avg(FILE_SIZE) / 1024 / 1024 as AvgFileSize_MB\
                                FROM\
                                snowflake.account_usage.copy_history_program C\
                                LEFT JOIN snowflake.account_usage.tables T ON C.TABLE_ID = T.TABLE_ID\
                                WHERE DELETED IS NOT NULL\
                                GROUP BY 1,2 \
                                HAVING\
                                    AvgFileSize_MB < 10 OR AvgFileSize_MB > 250 \
                                    ORDER BY 3 desc;").collect()

    pd_file_list_df = pd.DataFrame(file_list_df)
    st.dataframe(pd_file_list_df)

try:
    session = sfAccount_selector(config)
    if session:
        selected_app = st.sidebar.radio("Select relevant assistance utility",['User Management','Over/Under Size File Identifier'])
        if selected_app == 'User Management':
            pd_user_list_df = get_user_list(session)
            pd_user_list_df = pd_user_list_df[['DISPLAY_NAME','NAME','LOGIN_NAME','EMAIL','DISABLED','LAST_SUCCESS_LOGIN_DAYS']]
            pd_user_list_df1 = pd_user_list_df[['DISPLAY_NAME','NAME','EMAIL','DISABLED']]
            selected_action = st.sidebar.radio("Select User Management Option",['Search User','Enable User','Disable User','Drop User','Assign Role','Revoke Role'])
            action_user_list = get_actionable_user_list(session,selected_action,pd_user_list_df)
            if selected_action != 'Search User':
                selected_user = st.sidebar.selectbox("Select user for specified action",action_user_list)
                selected_name = pd_user_list_df1[pd_user_list_df1['DISPLAY_NAME'] == selected_user]
                selected_name = selected_name.iloc[-1]['NAME']
                with st.spinner("Wait a minute"):
                    if selected_action == 'Disable User':
                        disable_user(session,selected_name,selected_user)
                    if selected_action == 'Enable User':
                        enable_user(session,selected_name,selected_user)
                    if selected_action == 'Drop User':
                        drop_user(session,selected_name,selected_user)
                    if selected_action == 'Assign Role':
                        assign_role(session,selected_name,selected_user)
            else:
                search_user(session,pd_user_list_df)
            st.success('Done!')
            
        else:
            with st.spinner("Wait a minute"):
                st.header('**:blue[Over / Under Size Identifier]**')
                get_off_size_files(session)
            st.success('Done!')

#except:
 #   st.write("Something went wrong !!! ")
finally:
    st.write("Thank you !!! :champagne:")
