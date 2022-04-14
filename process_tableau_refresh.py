# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 22:08:26 2020

@author: Michael.Huang


"""
# from matplotlib import image
# import requests
import os
import logging
# import sys
from datetime import datetime,timedelta
import numpy as np
import pandas as pd
from bu_snowflake import get_connection
import tableauserverclient as TSC
from math import log
from dateutil import tz
import smtplib
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import bu_alerts
from bu_config import get_config
from email.utils import make_msgid
import config as config



def extract_tableau_pdf(token_name, token_secret, projectid, viewname_list):
    """
    Downloads image of given view and saves in image/pdf dump

    Args: 
        username - the username for Tableau
        password - the password for Tableau
        viewname - the desired view which will be turned into an image

    Returns:
        the name of the image file
    """
    try:
        images_name_list = []
        url_list = []
        # Signing into Tableau
        # tableau_auth = TSC.TableauAuth(username, password, 'biourja')
        tableau_auth = TSC.PersonalAccessTokenAuth(token_name=token_name, personal_access_token=token_secret, site_id=group)
        server = TSC.Server(tableau_url, use_server_version=True)
        server.auth.sign_in(tableau_auth) #sign in
        print('signing in...')
    
        # Getting all of the views on the server
        viewname_list_1 = []
        # project = []
        # name =[]
        for viewname in viewname_list:
            for v in TSC.Pager(server.views):
                if v.project_id == projectid and v.name == viewname:
                    # datasource_id = v.id
                    # resource = server.datasources.get_by_id(datasource_id)
                    # server.datasources.refresh(resource)
                    file_name, image_url = save_images(server,v,viewname) 
                    images_name_list.append(file_name)
                    url_list.append(image_url)
                    viewname_list_1.append(viewname)
    except Exception as e:
        print(f"Exception caught during extract_tableau_pdf execution {e}")
        logging.info(f"Exception caught during extract_tableau_pdf execution {e}")
    finally:
        # Signing out of Tableau
        server.auth.sign_out()
    return url_list,images_name_list,viewname_list_1


def save_images(server,view1,viewname):
    try:
        image_req_option = TSC.ImageRequestOptions(imageresolution=TSC.ImageRequestOptions.Resolution.High,maxage=100)
        file_name = f"{os.getcwd()}\\download\\test_{viewname.replace('/','_')}.png"
        # image_req_option.vf("TRADE_DATE",(datetime.now()-timedelta(1)).strftime("%Y-%m-%d"))
        image_req_option.vf("TRADE_DATE",(datetime.now()).strftime("%Y-%m-%d"))
        logging.info(f"The current date(TRADE_DATE) is  {datetime.now().strftime('%Y-%m-%d')} for icesettle")
        server.views.populate_image(view1,image_req_option)  
        server.views.populate_image(view1)    
        with open(file_name, 'wb') as f:
            f.write(view1.image)
    
        image_url = views_url.format(view1.content_url.replace('/sheets',''))

        return (file_name, image_url)
    except Exception as e:
        print(f"Exception caught during save_images execution {e}")
        logging.info(f"Exception caught during save_images execution {e}")

def SendImageEmail(url_list, images_name_list,view_name,to_addr,log):
    """
    Sends an email with desired image

    Args:
        imagename - the desired image
        extractname - the extract which was refreshed
        viewlink - link to where the view in question can be seen in realtime
    """
    try :
        # Read file containing authentication information
        try:
            x = open(r"G:\Gas & Pwr\Pwr\FTR\Python Scripts\JP\SQL Updaters\passwords.txt","r") 
            auth_rawtext = x.read()
            x.close()
        except:
            auth_rawtext = 'ftr-dev@biourja.com\nME=5A:mR>we:mEPP'


        # Read from file
        sender = auth_rawtext.split('\n')[0]
        auth = auth_rawtext.split('\n')[1]
        
        # Desired recipient of the email
        # mailinglist = ["priyanka.solanki@biourja.com","radha.waswani@biourja.com","ayushi.joshi@biourja.com","devina.ligga@biourja.com","arghya.mondal@biourja.com"]
        mailinglist = ["arghya.mondal@biourja.com","indiapowerit@biourja.com"]
        # mailinglist = ["priyanka.solanki@biourja.com","radha.waswani@biourja.com"]
        # Signing into email
        s = smtplib.SMTP(host='us-smtp-outbound-1.mimecast.com', port=587)
        s.starttls()
        s.login(sender, auth)
        msg = MIMEMultipart()        
        # Email details
        # msg["To"] = ", ".join(mailinglist)
        msg["To"] = to_addr
        msg["From"] = sender
        # suject = viewname_list[0]+ "_LIVE_TABLEAU_REPORT"
        # msg["Subject"]  = suject
        msg["Subject"] = tableau_email_subject
        url_list_new = [url.split('views/')[1] for url in url_list]

        st = datetime.today().strftime('%m/%d/%Y')
        # st = (datetime.today()-timedelta(days=1)).strftime('%m/%d/%Y')
        # Email body
        url_list = '<br>'.join(url_list)
        body = f'''<html>  <font size="+.5"> Hello,<br> {tableau_email_subject} were released for {st} <br>{tableau_email_subject} LIVE report for {view_name} update on Tableau Server has been completed.<br> List of Live Report, view and image are attached.</font> <br>{url_list}<br>'''
        images_name_list = [x.replace("./", "") for x in images_name_list]
        for pdf in images_name_list:
            image_cid = make_msgid()
            body += '<img src="cid:{image_cid}"><br>'.format(image_cid=image_cid[1:-1])
            binary_pdf  = open(pdf, 'rb')
            payload = MIMEImage(binary_pdf.read())
            payload.add_header('Content-ID', image_cid)
            msg.attach(payload)
        body += '</body></html>'    
        logging.info(f"pdf added to attachemet {pdf}")
        body_mimed = MIMEText(body,'html')
        msg.attach(body_mimed)
        # Send email
        s.sendmail(sender, mailinglist, msg.as_string())
        logging.info("Email sent successfully.")
    except Exception as e:
        print(f'Exception caught while sending mail {e}')
        logging.info(f'Exception caught while sending mail {e}')
    finally:
        s.quit()

def update_tableau_refresh_on_daychange(cursor,tablename):
    try:
        result = f'''UPDATE {databasename}.{schemaname}.{tablename} SET FLAG_TABLEAU_REFRESH = 'NULL' '''
        updated_records = cursor.execute(result).fetchall()
        return updated_records
    except Exception as e:
        print(f"Exception caught in update_tableau_refresh_on_daychange {e}")
        logging.info(f"Exception caught in update_tableau_refresh_on_daychange {e}")
        raise e

def fetch_records_to_tableau_refresh(cursor,tablename):
    try:
        query_fetch_data = f'''Select * from {databasename}.{schemaname}.{tablename}'''
        # cursor = conn.cursor()
        cursor.execute(query_fetch_data)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"Exception caught in fetch_records_to_tableau_refresh {e}")
        logging.info(f"Exception caught in fetch_records_to_tableau_refresh {e}")
        raise e

def update_flag_tableau_refresh(view_name,cursor,tablename):
    try:
        query_update_column = f'''UPDATE {databasename}.{schemaname}.{tablename} SET FLAG_TABLEAU_REFRESH = '1' WHERE VIEW_NAME = '{view_name}' '''
        records_updated = cursor.execute(query_update_column).fetchall()[0]
        return records_updated
    except Exception as e:
        print(f"Exception caught {e} in update_flag_tableau_refresh")
        logging.exception(f'Exception caught in update_flag_tableau_refresh: {e}')
        raise e        

if __name__ == "__main__":
    try:
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        log_file_location = config.log_path

        if os.path.isfile(log_file_location):
            os.remove(log_file_location)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] - %(message)s',
            filename=log_file_location)
        logging.info('Execution Started')
        current_time = datetime.now()
        job_id = np.random.randint(1000000, 9999999)
         # Get tableau credentials from BU_CONFIG_PARAMS table
        credential_dict =  get_config('TABLEAU_GENERIC_AUTO_REFRESH','TABLEAU_REFRESH_DETAILS') 
        api_key = credential_dict['API_KEY']
        # project_id = api_key.split(';')[0]
        group = api_key.split(';')[1]
        # projectname = credential_dict['PROJECT_NAME']
        urls = credential_dict['SOURCE_URL']
        tableau_url = urls.split(';')[0]
        views_url = urls.split(';')[1]
        token_name =credential_dict['USERNAME']
        token_secret = credential_dict['PASSWORD']
        to_addr = 'arghya.mondal@biourja.com,indiapowerit@biourja.com'
        # to_addr = 'priyanka.solanki@biourja.com,radha.waswani@biourja.com'

        databasename = config.databasename
        schemaname = config.schemaname
        # Created global connection object
        conn = get_connection(role='OWNER_{}'.format(config.databasename),
                                database=config.databasename, schema=config.schemaname)
        cursor = conn.cursor() 
        # Call for create DDL to fecth streams data
        view_ddls = cursor.execute(f"select VIEW_DDL_FINAL from {databasename}.{schemaname}.{config.stream_ddl_create_vw}").fetchone()[0].replace('\n','') 
        cursor.execute(view_ddls)
        # Fetch streams data, perform grouping of ACTION type and then insert into stream_log_dev table
        records = cursor.execute(f"Insert into {databasename}.{schemaname}.{config.table_stream_log_dev} (select * from {databasename}.{schemaname}.{config.stream_ddl_store_vw})").fetchall() 
        # TABLEAU_REFRESH_VW contains logic to get average of last 5 days that will check for the range of data insertion
        # TABLEAU_REFRESH_DETAILS would be updated from TABLEAU_REFRESH_VW on view_name and statusdate basis
        update_records = cursor.execute(f'Update {databasename}.{schemaname}.{config.table_tableau_refresh_details} A set a.FLAG=b.FLAG , a.DATETIME=CURRENT_TIMESTAMP from \
        {databasename}.{schemaname}.{config.tableau_refresh_vw} B where a.VIEW_NAME=b.TABLEAU_NAME and b.STATUSDATE=current_date').fetchall()
        # Code to check for 1 day back (lines 279-280)
        # update_records = cursor.execute(f'Update {databasename}.{schemaname}.{config.table_tableau_refresh_details} A set a.FLAG=b.FLAG , a.DATETIME=current_date-1 from \
        # {databasename}.{schemaname}.{config.tableau_refresh_vw} B where a.VIEW_NAME=b.TABLEAU_NAME and b.STATUSDATE=current_date-1').fetchall()

        
        # Set FLAG_TABLEAU_REFRESH in TABLEAU_REFRESH_DETAILS as NULL daily at 12:00am
        print("Current time hour and minute ::::::",current_time.hour,current_time.minute)
        # current_time_hour = current_time.strftime('%H:%M')
        if current_time.hour == 0 and current_time.minute in (0,1,2,3):
        # if current_time.hour == 8 and current_time.minute in (15,16,17,18):
            print("In if condition to set FLAG_TABLEAU_REFRESH = NULL for all reports")
            records = update_tableau_refresh_on_daychange(cursor,config.table_tableau_refresh_details)
            print("Length of records updated:::::::: ",len(records))

        result = fetch_records_to_tableau_refresh(cursor,config.table_tableau_refresh_details)
        df = pd.DataFrame(result)
        for i,x in df.iterrows():
            project_id =x[0]
            view_name = x[1]
            viewname_list =[view_name.replace('','')]
            flag_true = x[4]
            tableau_refresh =x[5]
            table_datetime = x[6]
            tableau_email_subject = x[7]
            
            if flag_true == 'TRUE' and  (tableau_refresh == 'NULL'or tableau_refresh == None) and table_datetime.date() == datetime.today().date() :
            # if flag_true == 'TRUE' and  (tableau_refesh == 'NULL'or tableau_refesh == None) and table_datetime.date() == (datetime.today()-timedelta(days=1)).date():
                logging.info('Function for extract tableau pdf')
                url_list,images_name_list,viewname_list= extract_tableau_pdf(token_name,token_secret, project_id, viewname_list)
                logging.info('sending Image email for tableau refresh')
                SendImageEmail(url_list, images_name_list,view_name,to_addr, log)
                column_update = update_flag_tableau_refresh(view_name,cursor,config.table_tableau_refresh_details) 
        logging.info('Execution Done')
        bu_alerts.send_mail(
            receiver_email = to_addr,
            mail_subject='JOB_SUCCESS - TABLEAU_REFRESH',
            mail_body='TABLEAU_REFRESH completed successfully',
            attachment_location = log_file_location
        )   
    except Exception as e:
        print("Exception caught during execution: ", e)
        logging.exception(f'Exception caught during execution: {e}')
        log_json = '[{"JOB_ID": "' + \
            str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=config.PROCESS_NAME, database=config.DATABASE, status='Failed',
                        table_name=config.TABLENAME, row_count=0, log=log_json, warehouse='QUANT_WH', process_owner=config.PROCESS_OWNER)
        
        bu_alerts.send_mail(
            receiver_email = to_addr,
            mail_subject='JOB_FAILED - TABLEAU_REFRESH',
            mail_body='TABLEAU_REFRESH failed during execution',
            attachment_location = log_file_location
        )
    endtime = datetime.now()
    print('Complete work at {} ...'.format(
        endtime.strftime('%Y-%m-%d %H:%M:%S')))
    # print('Total time taken: {} seconds'.format(
    #     (endtime-starttime).total_seconds()))
