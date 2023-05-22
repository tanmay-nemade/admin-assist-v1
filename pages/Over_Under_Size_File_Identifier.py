import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import *

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

session = st.session_state['Session']

with st.spinner("Wait a minute"):
    st.header('**:blue[Over / Under Size Identifier]**')
    get_off_size_files(session)
st.success('Done!')