# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 22:08:26 2020

@author: Michael.Huang


"""
from math import log
import requests
import logging
import sys
sys.path.append(r'S:\IT Dev\Production_Environment\ICE_NEX_PLATTS\sftp')
from tosnowflake_new import load_df_to_sf, get_max_datetime_ice, get_primary_keys
from datetime import datetime, timedelta, date
import json
import time
import numpy as np
import pandas as pd
from bu_snowflake import get_connection
import tableauserverclient as TSC
from dateutil import tz
import os
import re
import smtplib
from email.utils import make_msgid
from email.message import EmailMessage
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import bu_alerts
from email import encoders
import config as config
from bu_snowflake import get_connection






def extract_tableau_pdf(username, password, projectid, viewname_list,images_name_list, url_list):
    """
    Downloads image of given view and saves in image/pdf dump

    Args: 
        username - the username for Tableau
        password - the password for Tableau
        viewname - the desired view which will be turned into an image

    Returns:
        the name of the image file
    """
    
    # Signing into Tableau
    # tableau_auth = TSC.TableauAuth(username, password, 'biourja')
    tableau_auth = TSC.TableauAuth(username, password, 'biourja')
    # server = TSC.Server('https://prod-useast-a.online.tableau.com/', use_server_version=True)
    server = TSC.Server(url_1, use_server_version=True)
    server.auth.sign_in(tableau_auth) #sign in
    print('signing in...')
    try:
        # Getting all of the views on the server
        viewname_list_1 = []
        project = []
        name =[]
        for viewname in viewname_list:
            for v in TSC.Pager(server.views):
                # project.append(v.project_id)
                # name.append(v.name)
                if v.project_id == projectid and v.name == viewname:
                    file_name, image_url = save_images(server,v,viewname) 
                    images_name_list.append(os.getcwd() +"\\"+file_name)
                    url_list.append(image_url)
                    viewname_list_1.append(viewname)
            # df = pd.DataFrame({'project':project,'name':name})
            # df.to_csv(os.getcwd()+"\\"+'project_viewname_tableau.csv',index=True)
    finally:
        # Signing out of Tableau
        server.auth.sign_out()
    return url_list,images_name_list,viewname_list_1


def save_images(server,view1,viewname):
    # pdf_req_option = TSC.PDFRequestOptions(page_type=TSC.PDFRequestOptions.PageType.A5,
    #                                        orientation=TSC.PDFRequestOptions.Orientation.Landscape,maxage=100)
    image_req_option = TSC.ImageRequestOptions(imageresolution=TSC.ImageRequestOptions.Resolution.High,maxage=100)
    file_name = f"./test_{viewname.replace('/','_')}.png"
    server.views.populate_image(view1)    
    with open(file_name, 'wb') as f:
        f.write(view1.image)
       
    
    # image_url = "https://prod-useast-a.online.tableau.com/#/site/biourja/views/{}".format(view1.content_url.replace('/sheets',''))
    image_url = url_2.format(view1.content_url.replace('/sheets',''))

    return (file_name, image_url)
def SendImageEmail(url_list, images_name_list,viewname_list,to_addr,log):
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
        # mailinglist = ["rohit.pawar@biourja.com","power@biourja.com","ayushi.joshi@biourja.com","devina.ligga@biourja.com","srishti.sharma@biourja.com","Pritish.jain@biourja.com","Abhinav.Tamrakar@biourja.com"]
        # mailinglist = ["priyanka.solanki@biourja.com","radha.waswani@biourja.com","ayushi.joshi@biourja.com","devina.ligga@biourja.com"]
        mailinglist = ["priyanka.solanki@biourja.com","radha.waswani@biourja.com"]
        # mailinglist = mailinglist
        # Signing into email
        s = smtplib.SMTP(host='us-smtp-outbound-1.mimecast.com', port=587)
        s.starttls()
        s.login(sender, auth)

        msg = MIMEMultipart()
        # msg.preamble = '====================================================='
        
        # Email details
        msg["To"] = ", ".join(mailinglist)
        # msg["To"] = to_addr
        msg["From"] = sender
        # msg["Subject"] = imagename + " updated in Tableau server"
        # subject = "{} Auction Result - FIM Forward Inventory & MTM PNL and FPT O.I and Collateral Status"
        # msg["Subject"] = " ".join(viewname_list) + "_LIVE_TABLEAU_REPORT"
        suject = viewname_list[0] + "_LIVE_TABLEAU_REPORT"
        msg["Subject"]  = suject
        url_list_new = [url.split('views/')[1] for url in url_list]
        
        st = datetime.now().strftime('%m/%d/%Y')
        # Email body
        # body = "\n\nThe " + extractlist + " extract refresh on Tableau Server has been completed. Here is a screenshot of " + imagename + '\n\n'
        # body += 'For an interactive view of this table and the rest of the Workbook, go to: ' + viewlink + '\n\n'
        # body += "This was an automated e-mail generated at " + st + '.'
        url_list = '<br>'.join(url_list)
        body = f'''<html>  <font size="+.5"> Hello,<br> {viewname_list[0]} were released for {st} <br>{viewname_list[0]} LIVE report for {url_list_new[0]} update on Tableau Server has been completed.<br> List of Live Report, view and image are attached.</font> <br>{url_list}<br>'''
        images_name_list = [x.replace("./", "") for x in images_name_list]
        # viewname_list = [for x in viewname_list]
        # body += "List of extracts, views and images are attached.\n\n"
        # count = 1
        # for pd in images_name_list:
        # body += "{}".format(images_name_list)
            # count +=1
        # body_mimed  = '<html><body><h1>Schlagzeilen</h1><br>'
        # for img in images_name_list:
        #     image_cid = make_msgid()
            
        #     body_mimed += '<img src="cid:{image_cid}"><br>'.format(image_cid=image_cid[1:-1])
    
        # body_mimed = MIMEText(body,'plain')
            # fp = open('ice_power.gif', 'rb')
            # msgImage = MIMEImage(fp.read())
            # fp.close()

            # # Define the image's ID as referenced above
            # msgImage.add_header('Content-ID', '<image1>')
            # msgRoot.attach(msgImage)
            # # This example assumes the image is in the current directory
            # fp = open('ice_power.gif', 'rb')
            # msgImage = MIMEImage(fp.read())
            # fp.close()
        # Attach desired image
        # body  = '<html><body><h1>Schlagzeilen</h1><br>'
        for pdf in images_name_list:
            image_cid = make_msgid()
            body += '<img src="cid:{image_cid}"><br>'.format(image_cid=image_cid[1:-1])
            binary_pdf  = open(pdf, 'rb')
            payload = MIMEImage(binary_pdf.read())
            # binary_pdf.close()
            # payload.set_payload((binary_pdf).read())
            #enconding the binary into base64
            # encoders.encode_base64(payload)
            # payload.add_header('Content-Decomposition', '', filename=pdf)
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
        logging.info(f'got error while sending mail {e}')
    finally:
        s.quit()
def query_execution_view_updation(query):
    try:
        conn = get_connection(role='OWNER_{}'.format(config.DATABASE),
                              database=config.DATABASE, schema=config.SCHEMA)
        cursor = conn.cursor()
        cursor.execute(query)
        # result = cursor.fetchall()
        # return result
    except Exception as e:
        print("Exception caught in query_execution_api(): ", e)
        logging.info(f"Exception caught in query_execution_api(): {e}")
        raise e
        
def query_tableau_refresh(result):
    try:
        conn = get_connection(role='OWNER_{}'.format(config.DATABASE),
                            database=config.DATABASE, schema=config.SCHEMA)
        result =f""""UPDATE POWERDB_DEV.PTEST.TABLEAU_final SET FLAG_TABLEAU_REFRESH= 'NULL'"""
        cursor = conn.cursor()
        cursor.execute(result)
        # result = cursor.fetchall()
        # return result
    except Exception as e:
        print("Exception caught in query_execution_api(): ", e)
        logging.info(f"Exception caught in query_execution_api(): {e}")
        raise e
def fetech_records_to_tableau_refresh(query_fetech_data):
    try:
        conn = get_connection(role='OWNER_{}'.format(config.DATABASE),
                              database=config.DATABASE, schema=config.SCHEMA)
        query_fetech_data = f"""select * from POWERDB_DEV.PTEST.TABLEAU_FINAL """
        cursor = conn.cursor()
        cursor.execute(query_fetech_data)
        result_final = cursor.fetchall()
        return result_final
    except Exception as e:
        print(f"Exception caught {e} in refesh_tableau_data")
        logging.exception(f'Exception caught in refesh_tableau_data: {e}')
        raise e
        
# def query_execution(conn,tableau_refresh_flag):
#     try:
#         cs = conn.cursor()
#         records = cs.execute(tableau_refresh_flag).fetchall()
#         return records
#     except Exception as e:
#         print(f"Exception caught {e} in query_execution")
#         logging.exception(f'Exception caught in query_execution: {e}')
#         raise e


if __name__ == "__main__":
    try:
        job_id = np.random.randint(1000000, 9999999)
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        log_file_location = os.getcwd() + '\\' + 'logs' + '\\' + 'NEX_TABLEAU_REFRESH_DATA_CHECK.txt'
        if os.path.isfile(log_file_location):
            os.remove(log_file_location)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] - %(message)s',
            filename=log_file_location)

        logging.info('Execution Started')
        starttime = datetime.now()
        # to_addr = 'Priyanka.Solanki@biourja.com,radha.waswani@biourja.com'
        # database = "POWERDB_DEV"
        # schema = "PTEST"
        # table = "TABLEAU_final"
        query = ''
        result = ''
        query_fetech_data =''
        # query = query_execution_view_updation(query)
        # result = query_tableau_refresh(result)
        final_result = fetech_records_to_tableau_refresh(query_fetech_data)
        df =pd.DataFrame(final_result)
        for i,x in df.iterrows():
            # viewname_list = x[0]
            viewname_list = ['CASH/FORWARD']
            flag_true = x[2]
            tableau_refesh =x[3]
            to_addr = 'priyanka.solanki@biourja.com,radha.waswani@biourja.com'
            if flag_true == 'FALSE' and  tableau_refesh == '1' :
                username = "svc_tableauonline@biourja.com"
                password = "P@$$w0rd#BioU"
                project_id = "a78efd58-eda9-458a-90fa-e9e00dfb8b78"
                # group = 'Biourja'
                url_1 = "https://prod-useast-a.online.tableau.com/"
                url_2 = "https://prod-useast-a.online.tableau.com/#/site/biourja/views/{}"
                images_name_list = []
                url_list = []
                # viewname_list = ['ICE Power Fwd Curve','ICE Gas Basis Fwd Curve']
                logging.info('Function for extract tableau pdf')
                url_list,images_name_list,viewname_list= extract_tableau_pdf(username, password, project_id, viewname_list, images_name_list, url_list)
                # url_list,images_name_list,as_basis_fwd_curve = extract_tableau_pdf(username, password, project_id, "ICE Gas Basis Fwd Curve", images_name_list, url_list)
                logging.info('sending Image email for tableau refresh')
                SendImageEmail(url_list, images_name_list, viewname_list,to_addr, log)
                def fetech_records_to_tableau_after_refresh(query_fetech_data):
                    try:
                        conn = get_connection(role='OWNER_{}'.format(config.DATABASE),
                                                database=config.DATABASE, schema=config.SCHEMA)
                        query_fetech_data = f"""select * from POWERDB_DEV.PTEST.TABLEAU_FINAL """
                        cursor = conn.cursor()
                        cursor.execute(query_fetech_data)
                        result_final = cursor.fetchall()

                        return result_final
                    except Exception as e:
                        print(f"Exception caught {e} in refesh_tableau_data")
                        logging.exception(f'Exception caught in refesh_tableau_data: {e}')
                        raise e
        

            # tableau_flag = flag["FLAG_TABLEAU_REFRESH"]
                print("check flag")

        
        # tabe_result= query_execution(conn,query_fetech_data)
       
        bu_alerts.send_mail(
        receiver_email="priyanka.solanki@biourja.com,radha.waswani@biourja.com",
        mail_subject='JOB_SUCCESS - TEST: NEX TABLEAU REFRESH',
        mail_body='NEX TABLEAU REFRESH',
        attachment_location=log_file_location
    )     
        

    except Exception as e:
        print("Exception caught during execution: ", e)
        logging.exception(f'Exception caught during execution: {e}')
        log_json = '[{"JOB_ID": "' + \
            str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=config.PROCESS_NAME, database=config.DATABASE, status='Failed',
                        table_name=config.TABLENAME, row_count=0, log=log_json, warehouse='QUANT_WH', process_owner=config.PROCESS_OWNER)
        
    endtime = datetime.now()
    print('Complete work at {} ...'.format(
        endtime.strftime('%Y-%m-%d %H:%M:%S')))
    print('Total time taken: {} seconds'.format(
        (endtime-starttime).total_seconds()))
