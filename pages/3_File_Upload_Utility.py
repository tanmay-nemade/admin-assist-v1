import os
import configparser
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
import pandas as pd
import streamlit as st

# # Page config must be set
#comment
st.set_page_config(
    layout="wide",
    page_title="File Upload App"
)

st.title('File Upload Utility')



def db_list(session):
    database_list_df = session.sql("show databases ;").collect()
    database_list_df =  pd.DataFrame(database_list_df)
    database_list_df = database_list_df["name"]
    return database_list_df


def schemas_list(chosen_db, session):
    set_database = session.sql(f'''USE DATABASE {chosen_db}   ;''').collect()
    schema_list_df = session.sql("show schemas;").collect()
    schema_list_df =  pd.DataFrame(schema_list_df)
    schema_list_df = schema_list_df["name"]
    return schema_list_df

def tables_list(chosen_db,chosen_sc, session):
    set_database = session.sql(f'''USE DATABASE {chosen_db}   ;''').collect()
    set_schema = session.sql(f'''USE SCHEMA {chosen_sc}   ;''').collect()
    table_list_df = session.sql("show tables;").collect()
    if table_list_df != []:
        table_list_df =  pd.DataFrame(table_list_df)
        table_list_df = table_list_df["name"]
        return table_list_df


def file_to_upload(chosen_db, chosen_schema, chosen_table):
    label_for_file_upload = "Select file to ingest into {d}.{s}.{t}"\
      .format(d = chosen_db, s = chosen_schema, t = chosen_table)
    return label_for_file_upload


# def add_column(num):
#     col1, col2 = st.columns(2)
#     with col1:
#         column_name = st.text_input('Enter Column {} Name'.format(num))
#     with col2:
#         column_type = st.text_input('Enter Column {} Data Type'.format(num))
#     col_def = column_name +' '+column_type
#     num += 1

def data_selector():
    session = st.session_state['Session']
    database = db_list(session)
    current_role = session.sql('select current_role();').collect()
    current_role = pd.DataFrame(current_role)
    current_role = current_role.iloc[0]['CURRENT_ROLE()']
    st.write('Your current role is '+str(current_role))
    current_warehouse = session.sql('select current_warehouse();').collect()
    current_warehouse = pd.DataFrame(current_warehouse)
    current_warehouse = current_warehouse.iloc[0]['CURRENT_WAREHOUSE()']
    st.write('Your current warehouse is '+str(current_warehouse))
    db_select = st.selectbox('Choose Database',(database))
    schemas = schemas_list(db_select, session)
    sc_select = st.selectbox('Choose Schema',(schemas))
    tables = tables_list(db_select, sc_select, session)
    if list(tables) != []:
        table_select = st.selectbox('Choose table',(tables))
        return db_select, sc_select, table_select
    
    
# Step 12 Create a radio input with the schemas_list() function
try:
    session, db_select,sc_select,table_select = data_selector()
    file_type = st.selectbox('Choose a file type you want to upload',('Excel','CSV'))
    chosen_file = st.file_uploader(label=file_to_upload(db_select,sc_select,table_select))
    if chosen_file is not None:
        if file_type == 'CSV':
            data = pd.read_csv(chosen_file)
        elif file_type == 'Excel':
            data = pd.read_excel(chosen_file)
        st.info("Always close the previous file with 'x' before uploading another file ")
        conn2 = {
                'ACCOUNT': st.session_state['sfAccount'],
                'user': st.session_state['sfUser'],
                'password': st.session_state['sfPass'],
                'schema': sc_select,
                'database': db_select,
            }
        Mode = st.radio("Select mode",('Overwrite','Append'))
        if st.button('Upload'):
            session = Session.builder.configs(conn2).create()
            data = session.create_dataframe(data)
            try:
                if Mode == 'Overwrite':
                    data.write.mode('Overwrite').save_as_table(table_select)
                else:
                    data.write.mode('append').save_as_table(table_select)
            except ValueError:
                st.error('Upload failed')
            else:
                st.write('File uploaded')
    if chosen_file is not None:
        st.table(data)
except TypeError:
    st.write('Schema has no tables. Choose a different schema or a different database')
except KeyError as e:
    if e.args[0] == '''st.session_state has no key "Session". Did you forget to initialize it? More info: https://docs.streamlit.io/library/advanced-features/session-state#initialization''':
        st.info('Please Login first using correct credentials')

except snowflake.snowpark.exceptions.SnowparkSessionException as e:
    if e.args[0] == 'Cannot perform this operation because the session has been closed.':
       st.info('You have Logged Out. Please Login Again')


    
