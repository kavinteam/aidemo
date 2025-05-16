# tasks.py
from celery import shared_task
import traceback 
import json
import pandas as pd
from datetime import datetime
from django.db import connection
from iihmrapp.api_custom_functions import api_custom_functions
import traceback 
from django.conf import settings
import zipfile
import os 

@shared_task
def run_admin_raw_data_download(Qid, Json_request):
    ouputjson = {}
    datetime_obj_to_str_array = ['fld_sys_inserted_datetime', 'fld_form_start_time', 'fld_form_end_time']
    try:
        login_user_id = Json_request.get('login_user_id')
        request_tables = Json_request.get('request_tables', [])
        synceddatetime = Json_request.get('synceddatetime', '')

        if not login_user_id:
            raise ValueError("Invalid login user ID")
        
        webservice_code = api_custom_functions.web_service_code('admin_bc_raw_data_download')

        # Open database connection
        with connection.cursor() as cursor:
            cursor.execute("SET SESSION MAX_EXECUTION_TIME=60000")
            cursor.execute("CALL sp_hh_consolidated_report()")
            result_index = 1
            while True:
                data = cursor.fetchall()
                description = cursor.description
                if not data or not description:
                    if not cursor.nextset():
                        break
                    continue
                tablenames = description[0][0] or f'Table_{result_index}'
                columns = [col[0] for col in description]
                ouputjson[tablenames] = []
                for row in data:
                    table_data_dict = dict(zip(columns, row))
                    for datetime_obj_to_str in datetime_obj_to_str_array:
                        if datetime_obj_to_str in table_data_dict and table_data_dict[datetime_obj_to_str]:
                            if isinstance(table_data_dict[datetime_obj_to_str], (datetime, pd.Timestamp)):
                                table_data_dict[datetime_obj_to_str] = table_data_dict[datetime_obj_to_str].strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                table_data_dict[datetime_obj_to_str] = str(table_data_dict[datetime_obj_to_str])
                    ouputjson[tablenames].append(table_data_dict)
                result_index += 1
                if not cursor.nextset():
                    break
                
        # Log the ouputjson to see if data is generated
        # print("Generated Data:", ouputjson)

        # Task completed successfully, return output
        stringResponse = "Data successfully downloaded"
        api_custom_functions.UpdateQTable('', 1, stringResponse, 1, 0, 1, '', '', '', Qid)

        return ouputjson  # Return the output data here

    except Exception as e:
        error_trace = traceback.format_exc()
        stringResponse = f"Error occurred: {str(e)}"
        error_json = {
            "error_level": "1",
            "error_message": str(e),
            "error_file": "tasks.py",
            "traceback": error_trace,
            "serverdatetime": str(api_custom_functions.current_date_time_in_format())
        }
        api_custom_functions.error_log_insert(json.dumps(error_json), Qid or '', '', '', 'admin_bc_raw_data_download', '1', '1', '1')
        
        return {"error": str(e)}  # Return error details in case of failure

    finally:
        if connection:
            connection.close()


# from celery import shared_task

# @shared_task
# def run_admin_raw_data_download(login_user_id, request_tables, synceddatetime):
#     try:
#         # Your background task logic here
#         # Example: print arguments or log them for debugging
#         print(f"Processing data for User ID: {login_user_id} with tables: {request_tables} and sync date: {synceddatetime}")
        
#         # Add your logic to process data here, such as querying the database, processing the request, etc.
#         # For now, we'll just return a success message
#         result = f"Data download started for user {login_user_id}. Tables: {request_tables}, SyncedDatetime: {synceddatetime}"

#         # Example: You can store the result in a database, cache, or send a notification.
#         return result

#     except Exception as e:
#         # In case of error, log and return a failure message
#         return f"Error occurred during processing: {str(e)}"
"""
@shared_task
def run_admin_raw_data_download(Qid, Json_request):
    from iihmrapp.models import Trn_tbl_diet_and_nutrition, Trn_tbl_diet_and_nutrition,Trn_tbl_health_problem,Trn_tbl_family_details  
    # adjust this to your actual data source

    try:
        # Fetch your actual data based on Json_request and Qid
        # Example: This must return real data!
        ouputjson = api_custom_functions.fetch_data(Qid, Json_request)

        # Save to file
        filename = f"admin_data_{Qid}.json"
        filepath = os.path.join(settings.MEDIA_ROOT, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(ouputjson, f)

        # Compress it
        zip_path = filepath.replace('.json', '.zip')
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(filepath, arcname=filename)

        # Clean up JSON file
        os.remove(filepath)

        # Update task status
        api_custom_functions.UpdateQTable('', 1, "Data successfully downloaded", 1, 0, 1, '', '', '', Qid)

        # ✅ Correct: return only the download URL
        return {'file_url': settings.MEDIA_URL + os.path.basename(zip_path)}

    except Exception as e:
        error = traceback.format_exc()
        api_custom_functions.UpdateQTable('', 0, f"Error: {str(e)}", 1, 0, 0, '', '', error, Qid)
        raise e
"""