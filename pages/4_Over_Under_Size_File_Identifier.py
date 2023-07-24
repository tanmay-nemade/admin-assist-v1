import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import *

st.set_page_config(
    layout="wide",
    page_title="Over/Under Size File Identifier"
)

st.title('Over/Under Size File Indentifier')

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
    session = st.session_state['Session']
    current_role = session.sql('select current_role();').collect()
    current_role = pd.DataFrame(current_role)
    current_role = current_role.iloc[0]['CURRENT_ROLE()']
    st.write('Your current role is '+str(current_role))
    current_warehouse = session.sql('select current_warehouse();').collect()
    current_warehouse = pd.DataFrame(current_warehouse)
    current_warehouse = current_warehouse.iloc[0]['CURRENT_WAREHOUSE()']
    st.write('Your current warehouse is '+str(current_warehouse))
    with st.spinner("Wait a minute"):
        get_off_size_files(session)
    st.success('Done!')

except KeyError as e:
    if e.args[0] == '''st.session_state has no key "Session". Did you forget to initialize it? More info: https://docs.streamlit.io/library/advanced-features/session-state#initialization''':
        st.info('Please Login first using correct credentials')

except snowflake.snowpark.exceptions.SnowparkSessionException as e:
    if e.args[0] == 'Cannot perform this operation because the session has been closed.':
       st.info('You have Logged Out. Please Login Again')