import os
import datetime
# Database configurations
WAREHOUSE = 'ITPYTHON_WH'
databasename = 'POWERDB_DEV'
schemaname = 'PLOG'
stream_ddl_create_vw = 'STREAM_STORE_DDL'
stream_ddl_store_vw = 'STREAM_STORE_VW'
table_stream_log_dev = 'STREAM_LOG_DEV'
table_tableau_refresh_details = 'TABLEAU_REFRESH_DETAILS'
tableau_refresh_vw = 'TABLEAU_REFRESH'
log_path = os.getcwd() + '\\' + 'logs' + '\\' + 'TABLEAU_REFRESH.txt'


   


