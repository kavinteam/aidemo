# task.py
from celery import shared_task
import traceback 
import json
import pandas as pd
from datetime import datetime
from django.db import connection
from iihmrapp.api_custom_functions import api_custom_functions
from django.conf import settings
import zipfile
import os
import uuid

@shared_task
def run_admin_raw_data_download(Qid, Json_request):
    ouputjson = {}
    datetime_fields = ['fld_sys_inserted_datetime', 'fld_form_start_time', 'fld_form_end_time']

    try:
        login_user_id = Json_request.get('login_user_id')
        request_tables = Json_request.get('request_tables', {})
        synceddatetime = Json_request.get('synceddatetime', '')

        if not login_user_id:
            raise ValueError("Invalid login user ID")

        webservice_code = api_custom_functions.web_service_code('admin_bc_raw_data_download')

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

                table_name = description[0][0] or f'Table_{result_index}'
                columns = [col[0] for col in description]
                ouputjson[table_name] = []

                for row in data:
                    row_dict = dict(zip(columns, row))
                    for dt_field in datetime_fields:
                        if dt_field in row_dict and row_dict[dt_field]:
                            if isinstance(row_dict[dt_field], (datetime, pd.Timestamp)):
                                row_dict[dt_field] = row_dict[dt_field].strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                row_dict[dt_field] = str(row_dict[dt_field])
                    ouputjson[table_name].append(row_dict)

                result_index += 1
                if not cursor.nextset():
                    break

        # ✅ Use MEDIA_ROOT as-is
        reports_dir = os.path.join(settings.MEDIA_ROOT, 'reports')
        os.makedirs(reports_dir, exist_ok=True)

        filename = f"admin_raw_data_{Qid}_{uuid.uuid4().hex[:6]}.json"
        json_path = os.path.join(reports_dir, filename)

        with open(json_path, 'w', encoding='utf-8') as f:
            # json.dump(ouputjson, f, ensure_ascii=False, indent=2)
            json.dump(ouputjson, f, ensure_ascii=False)

        zip_filename = filename.replace('.json', '.zip')
        zip_path = os.path.join(reports_dir, zip_filename)
        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_STORED) as zipf:
            zipf.write(json_path, arcname=filename)

        if os.path.exists(zip_path):
            os.remove(json_path)

        api_custom_functions.UpdateQTable('', 1, "Data successfully downloaded", 1, 0, 1, '', '', '', Qid)

        # ✅ Return relative URL
        print(f"file_url:{settings.MEDIA_URL}reports/{zip_filename}")
        return {'file_url': settings.MEDIA_URL + 'reports/' + zip_filename}


    except Exception as e:
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        error_trace = traceback.format_exc()
        error_json = {
            "error_level": "1",
            "error_message": str(e),
            "error_file": "tasks.py",
            "traceback": error_trace,
            "error_trace": error_message,
            "serverdatetime": str(api_custom_functions.current_date_time_in_format())
        }
        api_custom_functions.error_log_insert(json.dumps(error_json), Qid or '', '', '', 'admin_bc_raw_data_download', '1', '1', '1')
        return {"error": str(e)}

    finally:
        if connection:
            connection.close()
