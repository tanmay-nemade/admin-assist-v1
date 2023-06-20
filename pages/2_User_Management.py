import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import *

st.set_page_config(
    layout="wide",
    page_title="User Management"
)

st.title('User Management')

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
    selected_value = st.sidebar.text_input("Enter search keyword")
    if selected_criteria == 'Display Name':
        st.write("You selected Display Name and selected value is ",selected_value)
        #selected_name = pd_user_list_df['DISPLAY_NAME'].str.contains(selected_value, na=False)
        selected_name = pd_user_list_df[pd_user_list_df['DISPLAY_NAME'].str.contains(selected_value, na=False, case = False)]
        st.write(selected_name)
    if selected_criteria == 'Name':
        st.write("You selected Name and selected value is ",selected_value)
        selected_name = pd_user_list_df[pd_user_list_df['NAME'].str.contains(selected_value, na=False, case = False)]
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
    st.write("**:blue[ALTER USER ",name," SET DISABLED = TRUE;]**")
    if st.button("Do you wish to continue ?"):
        # session.sql("ALTER USER {} SET DISABLED = TRUE;".format(name)).collect()
        st.write("**:blue[User ",selected_user," disabled successfully !]**")

def enable_user(session,name,selected_user):
    st.write("**:blue[ALTER USER ",name," SET DISABLED = FALSE;]**")
    if st.button("Do you wish to continue ?"):
        # session.sql("ALTER USER {} SET DISABLED = FALSE;".format(name)).collect()
        st.write("**:blue[User ",selected_user," enabled successfully !]**")

def drop_user(session,name,selected_user):
    st.write("**:blue[DROP USER ",name,";]**")
    if st.button("Do you wish to continue ?"):
        # session.sql("DROP USER {};".format(name)).collect()
        st.write("**:blue[User ",selected_user," successfully dropped !]**")

#Define a function to get list of all programs and also cache this data for multiple time consumption
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

def role_selection(_session):
    role_df = _session.sql('show roles;').collect()
    role_df = pd.DataFrame(role_df)
    role_list = role_df['name']
    role_select = st.sidebar.selectbox('Select a Role', role_list)
    if st.sidebar.button('Use Role'):
        set_role = _session.sql(f'''USE ROLE {role_select} ;''').collect()
        return set_role

def warehouse_selection(_session):
    warehouse_df = _session.sql('show warehouses;').collect()
    warehouse_df = pd.DataFrame(warehouse_df)
    warehouse_df
    warehouse_list = warehouse_df['name']
    warehouse_select = st.sidebar.selectbox('Select a Role', warehouse_list)
    if st.sidebar.button('Use Warehouse'):
        set_warehouse = _session.sql(f'''USE WAREHOUSE {warehouse_select} ;''').collect()

try:
    session = st.session_state['Session']
    set_role = role_selection(session)
    if set_role != '':
        warehouse_selection(session)
    
    current_role = session.sql('select current_role();').collect()
    current_role
    current_warehouse = session.sql('select current_warehouse();').collect()
    current_warehouse

    pd_user_list_df = get_user_list(session)
    pd_user_list_df = pd_user_list_df[['DISPLAY_NAME','NAME','LOGIN_NAME','EMAIL','DISABLED','LAST_SUCCESS_LOGIN_DAYS']]
    pd_user_list_df1 = pd_user_list_df[['DISPLAY_NAME','NAME','EMAIL','DISABLED']]
    selected_action = st.sidebar.radio("Select User Management Option",['Search User','Enable User','Disable User','Drop User','Assign Role','Revoke Role'])
    action_user_list = get_actionable_user_list(session,selected_action,pd_user_list_df)
    if selected_action != 'Search User':
        selected_user = st.selectbox("Select user for specified action",action_user_list)
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

except KeyError as e:
    if e.args[0] == '''st.session_state has no key "Session". Did you forget to initialize it? More info: https://docs.streamlit.io/library/advanced-features/session-state#initialization''':
        st.info('Please Login first using correct credentials')

except snowflake.snowpark.exceptions.SnowparkSessionException as e:
    if e.args[0] == 'Cannot perform this operation because the session has been closed.':
       st.info('You have Logged Out. Please Login Again')