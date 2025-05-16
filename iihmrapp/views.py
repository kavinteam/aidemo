from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
import json
from django.db import connection
# from django.conf import settings
from django.http import JsonResponse
import datetime
from django.db import transaction, connection
from iihmrapp.api_custom_functions import api_custom_functions
from iihmrapp.models import *
from datetime import datetime
import traceback
import pandas as pd
import time
from django.db import OperationalError, connection
from functools import wraps
from .hypertension_dashboard import hypertension_dashboard
from .diabetes_dashboard import diabeted_dashboard
from .diet_and_nutrition_dashboard import diet_and_nutrition_dashboard
from .health_problems import health_problems

# Create your views here.
def index(request):
    return render(request,'index.html')

# login for the web user based on the username and password method POST
@csrf_exempt    
def web_login(request):
    Qid = ''
    FormCode = ''
    valid = ''
    IsFullyProcessed = ''
    ReturnStatus = ''
    ReturnError_response = {}
    ReturnJson_response = {}
    stringResponse = ''
    ApiKey = ''
    AppTypeNo = ''
    AppVersion = ''
    receivedDate = api_custom_functions.current_date_time_in_format()
    try:
        if request.method == 'POST':
            json_body = request.body.decode('utf-8')
            request_json_validation = api_custom_functions.json_validation(json_body)  # Validating the Json
            if request_json_validation:
                json_request = json.loads(json_body)
                apikey_validation = api_custom_functions.apikey_validation(json_request)  # Validation the apikey
                parameters_validation = api_custom_functions.parameters_validation(json_request, 'web_login')  # validation parameters
                webservice_code = api_custom_functions.web_service_code('web_login')  # getting the webservice code
                if request_json_validation and apikey_validation and parameters_validation and webservice_code != '':
                    userid = json_request['userid']
                    password = json_request['password']
                    synceddatetime = json_request['synceddatetime']
                    FormCode = json_request['FormCode']
                    ApiKey = json_request['ApiKey']
                    AppTypeNo = json_request['AppTypeNo']
                    AppVersion = json_request['AppVersion']
                    jsonData_database = str(json.dumps(json_request))
                    receivedDate = api_custom_functions.current_date_time_in_format()
                    Qid = api_custom_functions.inserQtable_data(FormCode, jsonData_database, api_custom_functions.current_date_time_in_format(), synceddatetime)
                    # if userid == 'admin'
                    query = f"SELECT * FROM master_tbl_user where fld_user_id='{userid}' and fld_password='{password}' and fld_is_active='1'"
                    cursor = connection.cursor()
                    cursor.execute(query)
                    result = cursor.fetchall()
                    login_user_data = api_custom_functions.getting_data_in_dictionary_format(f"SELECT * FROM master_tbl_user WHERE  fld_user_id='{userid}' AND fld_password = '{password}' AND fld_is_active=1")
                    # else:
                    #     query = f"SELECT * FROM master_tbl_user where fld_useri_id ={userid}' and fld_password={password}'"
                    if not result:
                        valid = 0
                        IsFullyProcessed = 0
                        IsPartiallyProcessed = 1                    
                        ReturnStatus = 2
                        ReturnJson_response = {
                            "status": ReturnStatus,
                            "responsemessage": "Failed",
                            "serverdatetime": api_custom_functions.current_date_time_in_format(),
                        }
                    else:
                        valid = 1
                        ReturnStatus = 1
                        IsFullyProcessed = 1
                        IsPartiallyProcessed = 0
                        ReturnJson_response = {
                            "status": '1',
                            "responsemessage": "Loged In Successfully",
                            "loged_in_details": login_user_data,
                            "serverdatetime": api_custom_functions.current_date_time_in_format(),
                        }
                    stringResponse = str(json.dumps(ReturnJson_response))
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,
                                            ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                    return JsonResponse(ReturnJson_response)
                else:
                    if parameters_validation is False:
                        Json_response = json.dumps(json_body)
                        ReturnError_response = {
                            "error_level": "2",
                            "error_message": 'Parameter validation went wrong',
                            "error_file": "views.py",
                            "serverdatetime": api_custom_functions.current_date_time_in_format(),
                        }
                        stringResponse = str(json.dumps(ReturnError_response))
                        api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,
                                            ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                        api_custom_functions.error_log_insert(stringResponse, Qid, FormCode,
                                                        'sp_error_log_detials', 'web_login', '1', ReturnStatus, '1')
                    return JsonResponse(ReturnError_response)
            else:
                if not request_json_validation:
                    error_json = json.dumps(json_body)
                error_status = 4
                eroor_code = 4
                ReturnError_response = {
                    "error_level": "4",
                    "error_message": 'Invalid Json Request',
                    "error_file": "views.py",
                    "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                stringResponse = str(json.dumps(error_json))
                Qid = api_custom_functions.inserQtable_data(FormCode, error_json, api_custom_functions.current_date_time_in_format(), api_custom_functions.current_date_time_in_format())
                api_custom_functions.error_log_insert(str(json.dumps(error_json)), Qid, FormCode,'sp_error_log_detials', 'get_role_details_and_mapping_location', '1', error_status, eroor_code)
                api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                return JsonResponse(ReturnError_response)
    except Exception as e:
        valid = 1
        FormCode = json_request['FormCode'] if 'FormCode' in json_request else '' # Use get method to avoid KeyError
        ApiKey = json_request['ApiKey'] if 'FormCode' in json_request else ''
        AppTypeNo = json_request['AppTypeNo'] if 'AppType' in json_request else ''
        AppVersion = json_request['AppVersion'] if 'AppVersion' in json_request else ''
        IsFullyProcessed = 0
        IsPartiallyProcessed = 1
        ReturnStatus = 3

        # Construct error response
        ReturnError_response = {
            "error_level": "3",
            "error_message": str(e),
            "error_file": "views.py",
            "serverdatetime": api_custom_functions.current_date_time_in_format(),
        }
        stringResponse = str(json.dumps(ReturnError_response))

        # Log error details
        api_custom_functions.error_log_insert(stringResponse, Qid, FormCode,
                                            'sp_error_log_detials', 'web_login', '1', ReturnStatus, '1')
        api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,
                                          ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)

    # Return the constructed error response
    return JsonResponse(ReturnError_response)

# login for the web user based on the username and password method POST
@csrf_exempt    
def web_login(request):
    Qid = ''
    FormCode = ''
    valid = ''
    IsFullyProcessed = ''
    ReturnStatus = ''
    ReturnError_response = {}
    ReturnJson_response = {}
    stringResponse = ''
    ApiKey = ''
    AppTypeNo = ''
    AppVersion = ''
    receivedDate = api_custom_functions.current_date_time_in_format()
    try:
        if request.method == 'POST':
            json_body = request.body.decode('utf-8')
            request_json_validation = api_custom_functions.json_validation(json_body)  # Validating the Json
            if request_json_validation:
                json_request = json.loads(json_body)
                apikey_validation = api_custom_functions.apikey_validation(json_request)  # Validation the apikey
                parameters_validation = api_custom_functions.parameters_validation(json_request, 'web_login')  # validation parameters
                webservice_code = api_custom_functions.web_service_code('web_login')  # getting the webservice code
                if request_json_validation and apikey_validation and parameters_validation and webservice_code != '':
                    userid = json_request['userid']
                    password = json_request['password']
                    synceddatetime = json_request['synceddatetime']
                    FormCode = json_request['FormCode']
                    ApiKey = json_request['ApiKey']
                    AppTypeNo = json_request['AppTypeNo']
                    AppVersion = json_request['AppVersion']
                    jsonData_database = str(json.dumps(json_request))
                    receivedDate = api_custom_functions.current_date_time_in_format()
                    Qid = api_custom_functions.inserQtable_data(FormCode, jsonData_database, api_custom_functions.current_date_time_in_format(), synceddatetime)
                    # if userid == 'admin'
                    query = f"SELECT * FROM master_tbl_user where fld_user_id='{userid}' and fld_password='{password}' and fld_is_active='1'"
                    cursor = connection.cursor()
                    cursor.execute(query)
                    result = cursor.fetchall()
                    login_user_data = api_custom_functions.getting_data_in_dictionary_format(f"SELECT * FROM master_tbl_user WHERE  fld_user_id='{userid}' AND fld_password = '{password}' AND fld_is_active=1")
                    # else:
                    #     query = f"SELECT * FROM master_tbl_user where fld_useri_id ={userid}' and fld_password={password}'"
                    if not result:
                        valid = 0
                        IsFullyProcessed = 0
                        IsPartiallyProcessed = 1                    
                        ReturnStatus = 2
                        ReturnJson_response = {
                            "status": ReturnStatus,
                            "responsemessage": "Failed",
                            "serverdatetime": api_custom_functions.current_date_time_in_format(),
                        }
                    else:
                        valid = 1
                        ReturnStatus = 1
                        IsFullyProcessed = 1
                        IsPartiallyProcessed = 0
                        ReturnJson_response = {
                            "status": '1',
                            "responsemessage": "Loged In Successfully",
                            "loged_in_details": login_user_data,
                            "serverdatetime": api_custom_functions.current_date_time_in_format(),
                        }
                    stringResponse = str(json.dumps(ReturnJson_response))
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,
                                            ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                    return JsonResponse(ReturnJson_response)
                else:
                    if parameters_validation is False:
                        Json_response = json.dumps(json_body)
                        ReturnError_response = {
                            "error_level": "2",
                            "error_message": 'Parameter validation went wrong',
                            "error_file": "views.py",
                            "serverdatetime": api_custom_functions.current_date_time_in_format(),
                        }
                        stringResponse = str(json.dumps(ReturnError_response))
                        api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,
                                            ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                        api_custom_functions.error_log_insert(stringResponse, Qid, FormCode,
                                                        'sp_error_log_detials', 'web_login', '1', ReturnStatus, '1')
                    return JsonResponse(ReturnError_response)
            else:
                if not request_json_validation:
                    error_json = json.dumps(json_body)
                error_status = 4
                eroor_code = 4
                ReturnError_response = {
                    "error_level": "4",
                    "error_message": 'Invalid Json Request',
                    "error_file": "views.py",
                    "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                stringResponse = str(json.dumps(error_json))
                Qid = api_custom_functions.inserQtable_data(FormCode, error_json, api_custom_functions.current_date_time_in_format(), api_custom_functions.current_date_time_in_format())
                api_custom_functions.error_log_insert(str(json.dumps(error_json)), Qid, FormCode,'sp_error_log_detials', 'get_role_details_and_mapping_location', '1', error_status, eroor_code)
                api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                return JsonResponse(ReturnError_response)
    except Exception as e:
        valid = 1
        FormCode = json_request['FormCode'] if 'FormCode' in json_request else '' # Use get method to avoid KeyError
        ApiKey = json_request['ApiKey'] if 'FormCode' in json_request else ''
        AppTypeNo = json_request['AppTypeNo'] if 'AppType' in json_request else ''
        AppVersion = json_request['AppVersion'] if 'AppVersion' in json_request else ''
        IsFullyProcessed = 0
        IsPartiallyProcessed = 1
        ReturnStatus = 3

        # Construct error response
        ReturnError_response = {
            "error_level": "3",
            "error_message": str(e),
            "error_file": "views.py",
            "serverdatetime": api_custom_functions.current_date_time_in_format(),
        }
        stringResponse = str(json.dumps(ReturnError_response))

        # Log error details
        api_custom_functions.error_log_insert(stringResponse, Qid, FormCode,
                                            'sp_error_log_detials', 'web_login', '1', ReturnStatus, '1')
        api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,
                                          ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)

    # Return the constructed error response
    return JsonResponse(ReturnError_response)
  
@csrf_exempt
def admin_raw_data_download(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
        formcode = ''
        valid = 0
        IsFullyProcessed = 0
        IsPartiallyProcessed = 1                    
        ReturnStatus = 2
        ReturnError_response = {}
        ReturnJson_response = {}
        stringResponse = ''
        ApiKey = ''
        AppTypeNo = ''
        AppVersion = ''
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'admin_all_raw_data_download') # validation th
                webservice_code = api_custom_functions.web_service_code('admin_all_raw_data_download')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
            if apikey_validation == True and parameters_validation == True and webservice_code !='':
                Json_request = json.loads(request.body)
                datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                # get the json data
                Json_request = json.loads(request.body)
                login_user_id=Json_request['login_user_id']
                request_tables = Json_request['request_tables']
                synceddatetime = Json_request['synceddatetime']
                jsonData_database = str(json.dumps(Json_request))
                Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                for tablenames in request_tables:
                    ouputjson[tablenames] = []
                    max_rn = request_tables[tablenames]
                    master_or_transaction_tbl = tablenames.split('_')[0]
                    if tablenames == "trn_tbl_chornic_illnesses": 
                        # table_published_check = f"SELECT * FROM master_tbl_state "
                        table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT * FROM trn_tbl_chornic_illnesses where fld_is_active='1';""")
                    elif tablenames == "trn_tbl_diabets": 
                        # table_published_check = f"SELECT * FROM master_tbl_state "
                        table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT * FROM trn_tbl_diabets where fld_is_active='1'; """)
                    elif tablenames == "trn_tbl_diet_and_nutrition": 
                        # table_published_check = f"SELECT * FROM master_tbl_state "
                        table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT * FROM trn_tbl_diet_and_nutrition where fld_is_active='1'; """)
                    elif tablenames == "trn_tbl_family_details": 
                        # table_published_check = f"SELECT * FROM master_tbl_state "
                        table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT * FROM trn_tbl_family_details where fld_is_active='1';""")
                    elif tablenames == "trn_tbl_gi_house_hold_detail": 
                        # table_published_check = f"SELECT * FROM master_tbl_state "
                        table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT * FROM trn_tbl_gi_house_hold_detail where fld_is_active='1';""")  
                    elif tablenames == "trn_tbl_health_problem": 
                        # table_published_check = f"SELECT * FROM master_tbl_state "
                        table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT * FROM trn_tbl_health_problem where fld_is_active='1';""")    
                    elif tablenames == "trn_tbl_hh_consent_form": 
                        # table_published_check = f"SELECT * FROM master_tbl_state "
                        table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT * FROM trn_tbl_hh_consent_form where fld_is_active='1';""")
                    elif tablenames == "trn_tbl_hospitalization": 
                        # table_published_check = f"SELECT * FROM master_tbl_state "
                        table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT * FROM trn_tbl_hospitalization where fld_is_active='1';""") 
                    elif tablenames == "trn_tbl_hypertension": 
                        # table_published_check = f"SELECT * FROM master_tbl_state "
                        table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT * FROM trn_tbl_hypertension where fld_is_active='1';""") 
                    elif tablenames == "trn_tbl_respondent_detail_form": 
                        # table_published_check = f"SELECT * FROM master_tbl_state "
                        table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT * FROM trn_tbl_respondent_detail_form where fld_is_active='1';""") 
                    elif tablenames == "trn_tbl_standred_of_living": 
                        # table_published_check = f"SELECT * FROM master_tbl_state "
                        table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT * FROM trn_tbl_standred_of_living where fld_is_active='1';""")    
                    for table_data_dict in table_data: 
                        # check the datetime_obj_to_str_array is in the table_data_dict or not
                        if datetime_obj_to_str_array:
                            for datetime_obj_to_str in datetime_obj_to_str_array:
                                # check the datetime_obj_to_str is in the table_data_dict or not
                                if datetime_obj_to_str in table_data_dict:
                                    # convert the datetime object to string
                                    table_data_dict[datetime_obj_to_str] = str(table_data_dict[datetime_obj_to_str])
                        ouputjson[tablenames].append(table_data_dict)
                # convertin the ouputjson to json
                valid = 1
                ReturnStatus = 1
                IsFullyProcessed = 1
                IsPartiallyProcessed = 0
                Json_response = {
                    "status": "1",
                    "responsemessage": ouputjson,
                    "serverdatetime": str(api_custom_functions.current_date_time_in_format())
                }
                stringResponse = str(json.dumps(Json_response))
                api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                return JsonResponse(Json_response)
            else:
                if request_json_validation == True:
                    Json_request = json.dumps(json.loads(request.body), default=str) # convert the json to string
                else:
                    Json_request = request.body # getting the json data
                Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
                Json_response ={
                "error_level": "2",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','admin_bc_raw_data_download','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
            return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "1",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','admin_bc_raw_data_download','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
# Update new apk in the folder as well as in the database to provide the new Mobile Updated APK version to the client 
@csrf_exempt
def admin_IIHMR_mobile_apk_upload(request):
    Qid = ''
    FormCode = ''
    valid = ''
    IsFullyProcessed = ''
    IsPartiallyProcessed = ''
    ReturnStatus = ''
    ReturnError_response = {}
    ReturnJson_response = {}
    stringResponse = ''
    ApiKey = ''
    AppTypeNo = ''
    AppVersion = ''
    receivedDate = api_custom_functions.current_date_time_in_format()
    if request.method == 'POST':
        try:
            # Check if the 'apk' file is present in the request
            if 'apk' not in request.FILES:
                return JsonResponse({'error': 'APK file not provided.'}, status=400)
            webservice_code = api_custom_functions.web_service_code('admin_IIHMR_mobile_apk_upload')  # getting the webservice code
            synced_datetime = api_custom_functions.current_date_time_in_format()
            rf_id = ''  # Initialize rf_id as empty string
            folder = 'IIHMR_mobile_apk_uploads'  # Replace with the desired folder name for APKs
            apk_file = request.FILES['apk']
            fldmobversioncode = request.POST.get('fld_mob_version_code')
            fldmobversionname = request.POST.get('fld_mob_version_name')
            remark = request.POST.get('remarks')
            # Create a dictionary to hold the data
            jsonData_database = {
                "apk_file": apk_file.name,
                "fld_mob_version_code": fldmobversioncode,
                "fld_mob_version_name": fldmobversionname,
                "remarks": remark
            }
            # Convert the dictionary to a JSON string
            jsonData_database_str = json.dumps(jsonData_database)
            Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database_str,receivedDate,synced_datetime)
            # Validate that all required fields are provided
            if not all([fldmobversioncode, fldmobversionname, remark]):
                Json_response = {
                    "status": "2",
                    "responsemessage": f"Please Update the mobile version code and mobile version_name and remakrs",
                    "serverdatetime": receivedDate
                }
                stringResponse = str(json.dumps(Json_response))
                api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
            checks = [
                (master_trn_updatecheck.objects.filter(fld_mob_version_code=fldmobversioncode).exists(), f"Version code {fldmobversioncode} already exists, please change it."),
                (master_trn_updatecheck.objects.filter(fld_mob_version_name=fldmobversionname).exists(), f"Version name {fldmobversionname} already exists, please change it."),
                (master_trn_updatecheck.objects.filter(fld_file_name=apk_file).exists(), f"File name {apk_file} already exists, please change it.")
            ]
            for exists, message in checks:
                if exists:
                    Json_response = {
                        "status": "3",
                        "responsemessage": message,
                        "serverdatetime": receivedDate
                    }
                    stringResponse = json.dumps(Json_response)
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                    return JsonResponse(Json_response)                
            cursor = connection.cursor()
            fldfilename = apk_file.name  # Save the file name, not the file object
            # Save the uploaded APK file
            # Insert data into Q table (assuming this function handles its own database operations)
            Qid = api_custom_functions.inserQtable_data(webservice_code, apk_file, api_custom_functions.current_date_time_in_format(), synced_datetime)
            # Execute stored procedure
            query = "CALL sp_updatecheck(%s, %s, %s, %s, %s)"
            values_need_to_insert = (rf_id, fldmobversioncode, fldmobversionname, fldfilename, remark)
            cursor.execute(query, values_need_to_insert)
            file_path = api_custom_functions.save_uploaded_image(apk_file, folder)
            # Update Q table with additional information           
            api_custom_functions.UpdateQTable(FormCode,valid,stringResponse,IsFullyProcessed,IsPartiallyProcessed,ReturnStatus,ApiKey,AppTypeNo,AppVersion,Qid)
            if file_path:
                FormCode = webservice_code,
                valid = 1,
                IsFullyProcessed=1,
                IsPartiallyProcessed=1,
                ReturnStatus=1,
                ApiKey = 'kavin',
                AppTypeNo='Mobile_apk',
                AppVersion = fldmobversioncode, 
                Json_response = {
                    "status": "1",
                    "responsemessage": f"IIHMR Mobile APK updated successfully.[file_path {file_path}]",
                    "serverdatetime": receivedDate
                }
                stringResponse = str(json.dumps(Json_response))
                api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                return JsonResponse(Json_response)
            else:
                FormCode = webservice_code,
                valid = 2,
                IsFullyProcessed= 2,
                IsPartiallyProcessed=1,
                ReturnStatus=2,
                ApiKey = 'kavin',
                AppTypeNo='Mobile_apk',
                AppVersion = fldmobversioncode, 
                Json_response = {
                    "status": "4",
                    "responsemessage": f"IIHMR Mobile APK updation not successful.Please try again",
                    "serverdatetime": receivedDate
                }
                stringResponse = str(json.dumps(Json_response))
                api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
    return JsonResponse({'error': 'Invalid request method.'}, status=400)

# Retry decorator
def retry_on_db_errors(max_attempts=3, delay=0.2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except OperationalError as e:
                    code = e.args[0] if isinstance(e.args[0], int) else None
                    if code in [1213, 2006]:  # Deadlock or Server Gone Away
                        attempts += 1
                        connection.close()
                        time.sleep(delay)
                    else:
                        raise
            raise OperationalError("Max retry attempts reached.")
        return wrapper
    return decorator

# Download the requested raw data in the excel return the data in the json format 
# from iihmrapp.api_custom_functions import retry_on_db_errors
# @csrf_exempt
# @retry_on_db_errors(max_attempts=5, delay=0.5)
# def admin_all_raw_data_download(request):
#     if request.method != 'POST':
#         return JsonResponse({
#             "status": "3",
#             "responsemessage": "Invalid request method",
#             "serverdatetime": str(api_custom_functions.current_date_time_in_format())
#         }, status=405)

#     Qid = ''
#     valid = 0
#     IsFullyProcessed = 0
#     IsPartiallyProcessed = 1
#     ReturnStatus = 2
#     stringResponse = ''
#     FormCode = ''
#     ApiKey = ''
#     AppTypeNo = ''
#     AppVersion = ''
#     try:
#         json_body = request.body
#         if not api_custom_functions.json_validation(json_body):
#             raise ValueError("Invalid JSON format")

#         Json_request = json.loads(request.body)
#         api_custom_functions.apikey_validation(Json_request)
#         api_custom_functions.parameters_validation(Json_request, 'admin_bc_raw_data_download')
#         webservice_code = api_custom_functions.web_service_code('admin_bc_raw_data_download')

#         login_user_id = Json_request.get('login_user_id')
#         request_tables = Json_request.get('request_tables', [])
#         synceddatetime = Json_request.get('synceddatetime', '')
#         check_login_user_id = Master_tbl_user.objects.filter(fld_user_id=login_user_id).exists()
#         if not check_login_user_id:
#             raise ValueError("Invalid login user ID")

#         jsonData_database = json.dumps(Json_request)
#         Qid = api_custom_functions.inserQtable_data(webservice_code, jsonData_database, api_custom_functions.current_date_time_in_format(), synceddatetime)

#         # Prepare to collect output
#         ouputjson = {}
#         datetime_obj_to_str_array = ['fld_sys_inserted_datetime', 'fld_form_start_time', 'fld_form_end_time']
#         result_index = 1

#         with connection.cursor() as cursor:
#             cursor.execute("SET SESSION MAX_EXECUTION_TIME=60000")
#             cursor.execute("CALL sp_hh_consolidated_report()")
#             while True:
#                 data = cursor.fetchall()
#                 description = cursor.description
#                 if not data or not description:
#                     if not cursor.nextset():
#                         break
#                     continue
#                 tablenames = description[0][0] or f'Table_{result_index}'
#                 columns = [col[0] for col in description]
#                 ouputjson[tablenames] = []
#                 for row in data:
#                     table_data_dict = dict(zip(columns, row))
#                     for datetime_obj_to_str in datetime_obj_to_str_array:
#                         if datetime_obj_to_str in table_data_dict and table_data_dict[datetime_obj_to_str]:
#                             if isinstance(table_data_dict[datetime_obj_to_str], (datetime, pd.Timestamp)):
#                                 table_data_dict[datetime_obj_to_str] = table_data_dict[datetime_obj_to_str].strftime('%Y-%m-%d %H:%M:%S')
#                             else:
#                                 table_data_dict[datetime_obj_to_str] = str(table_data_dict[datetime_obj_to_str])
#                     ouputjson[tablenames].append(table_data_dict)
#                 result_index += 1
#                 if not cursor.nextset():
#                     break
#         # Final success response
#         valid = 1
#         ReturnStatus = 1
#         IsFullyProcessed = 1
#         IsPartiallyProcessed = 0
#         stringResponse = "successfully data is downloaded"
#         Json_response = {
#             "status": "1",
#             "responsemessage": ouputjson,
#             "serverdatetime": str(api_custom_functions.current_date_time_in_format())
#         }

#     except Exception as e:
#         error_trace = traceback.format_exc()
#         stringResponse = f"Error occurred: {str(e)}"
#         Json_response = {
#             "status": "0",
#             "responsemessage": stringResponse,
#             "trace": error_trace,
#             "serverdatetime": str(api_custom_functions.current_date_time_in_format())
#         }

#         error_json = {
#             "error_level": "1",
#             "error_message": str(e),
#             "error_file": "views.py",
#             "traceback": error_trace,
#             "serverdatetime": Json_response["serverdatetime"]
#         }

#         api_custom_functions.error_log_insert(json.dumps(error_json), Qid or '', '', '','admin_bc_raw_data_download', '1', '1', '1')
#     finally:
#         if connection:
#             connection.close()

#         api_custom_functions.UpdateQTable(FormCode, valid, stringResponse,IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
#     return JsonResponse(Json_response, safe=False)

# ************************************************************************************************************************************
# Run the celry as below 
# celery -A IIHMR worker -l info
# or celery -A IIHMR worker --loglevel=info

from .tasks import run_admin_raw_data_download
from celery.result import AsyncResult

from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import json
import traceback
from celery.result import AsyncResult
from iihmrapp.api_custom_functions import api_custom_functions

@csrf_exempt
@retry_on_db_errors(max_attempts=5, delay=0.5)
def admin_all_raw_data_download(request):
    if request.method != 'POST':
        return JsonResponse({
            "status": "3",
            "responsemessage": "Invalid request method",
            "serverdatetime": str(api_custom_functions.current_date_time_in_format())
        }, status=405)
    Qid = ''
    try:
        # Validate and parse the request body
        json_body = request.body
        if not api_custom_functions.json_validation(json_body):
            raise ValueError("Invalid JSON format")

        Json_request = json.loads(json_body)

        # Validate API key and parameters
        json_data = api_custom_functions.apikey_validation(Json_request)
        api_key = api_custom_functions.parameters_validation(Json_request, 'admin_all_raw_data_download')
        webservice_code = api_custom_functions.web_service_code('admin_all_raw_data_download')
        if json_data or api_key or webservice_code != '': 
            # Extract the necessary fields from the request
            login_user_id = Json_request.get('login_user_id')
            request_tables = Json_request.get('request_tables', [])
            synceddatetime = Json_request.get('synceddatetime', '')

            # Check if the user is valid
            check_login_user_id = Master_tbl_user.objects.filter(fld_user_id=login_user_id).exists()
            if not check_login_user_id:
                raise ValueError("Invalid login user ID")

            # Log the request data
            jsonData_database = json.dumps(Json_request)
            Qid = api_custom_functions.inserQtable_data(webservice_code, jsonData_database, api_custom_functions.current_date_time_in_format(), synceddatetime)

            # Trigger the Celery background task and pass the necessary parameters
            task = run_admin_raw_data_download.apply_async(args=[Qid, Json_request])

            # Return the task ID to the client so they can track the status later
            return JsonResponse({
                "status": "1",
                "responsemessage": "Data download is in progress. Please check later.",
                "task_id": task.id,    # Return the task_id for tracking
                "serverdatetime": str(api_custom_functions.current_date_time_in_format())
            })
        else:
            # Return the task ID to the client so they can track the status later
            return JsonResponse({
                "status": "2",
                "responsemessage": "Parameter validation went wrong.",
                "serverdatetime": str(api_custom_functions.current_date_time_in_format())
            })
    except Exception as e:
        # Handle any exceptions, log them, and return an error response
        error_trace = traceback.format_exc()
        stringResponse = f"Error occurred: {str(e)}"
        Json_response = {
            "status": "0",
            "responsemessage": stringResponse,
            "trace": error_trace,
            "serverdatetime": str(api_custom_functions.current_date_time_in_format())
        }

        # Log the error details for further analysis
        error_json = {
            "error_level": "1",
            "error_message": str(e),
            "error_file": "views.py",
            "traceback": error_trace,
            "serverdatetime": Json_response["serverdatetime"]
        }

        api_custom_functions.error_log_insert(json.dumps(error_json), Qid or '', '', '', 'admin_bc_raw_data_download', '1', '1', '1')

        return JsonResponse(Json_response, safe=False)
    

@csrf_exempt
def check_task_status(request):
    if request.method != 'POST':
        return JsonResponse({
            "status": "3",
            "responsemessage": "Invalid request method",
            "serverdatetime": str(api_custom_functions.current_date_time_in_format())
        }, status=405)

    try:
        # Get task_id depending on content type
        if request.content_type == 'application/json':
            json_data = json.loads(request.body)
            task_id = json_data.get('task_id')
        else:
            task_id = request.POST.get('task_id')

        if not task_id:
            raise ValueError("task_id is required and cannot be empty.")

        print(f"Received task_id: {task_id}")
        task = AsyncResult(task_id)
        print(f"Task state: {task.state}")

        # Default response values
        status_code = "1"
        file_url = None
        message = "Unknown status"

        if task.state == 'PENDING':
            message = "Task is in progress"
        elif task.state == 'SUCCESS':
            result = task.result
            if isinstance(result, dict):
                file_url = result.get("file_url")
            message = "Task completed successfully"
        elif task.state == 'FAILURE':
            message = f"Task failed: {str(task.result)}"
            status_code = "0"
        else:
            message = f"Task status: {task.state}"

        return JsonResponse({
            "status": status_code,
            "responsemessage": message,
            "task_state": task.state,
            "file_url": file_url,
            "serverdatetime": str(api_custom_functions.current_date_time_in_format())
        })

    except Exception as e:
        error_trace = traceback.format_exc()
        return JsonResponse({
            "status": "0",
            "responsemessage": f"Exception occurred: {str(e)}",
            "trace": error_trace,
            "serverdatetime": str(api_custom_functions.current_date_time_in_format())
        }, safe=False)

# ************************************************************************************************************************************
@csrf_exempt
def gender_hypertension(request):
    return hypertension_dashboard.gender_hypertension_report(request)

@csrf_exempt
def hypertension_months_screening(request):
    return hypertension_dashboard.hypertension_months_screening_report(request)

@csrf_exempt
def current_hyperteion_medicine(request):
    return hypertension_dashboard.current_hyperteion_medicine_report(request)


@csrf_exempt
def hyperteion_blood_preasur_range(request):
    return hypertension_dashboard.hyperteion_blood_preasur_range_report(request)

@csrf_exempt
def preventive_measures_hypertension(request):
    return hypertension_dashboard.preventive_measures_hypertension_report(request)

@csrf_exempt
def hypertensive_medicine_currently_used(request):
    return hypertension_dashboard.hypertensive_medicine_currently_used_report(request)

@csrf_exempt
def hypertension_member_age(request):
    return hypertension_dashboard.hypertension_member_age_report(request)

@csrf_exempt
def last_recorded_blood_pressure(request):
    return hypertension_dashboard.last_recorded_blood_pressure_report(request)

@csrf_exempt
def signs_and_symptoms_of_high_blood_pressure(request):
    return hypertension_dashboard.signs_and_symptoms_of_high_blood_pressure_report(request)

@csrf_exempt
def frequency_of_currently_used_medicine(request):
    return hypertension_dashboard.frequency_of_currently_used_medicine_report(request)

@csrf_exempt
def management_measures_of_hypertension(request):
    return hypertension_dashboard.management_measures_of_hypertension_report(request)

@csrf_exempt
def place_of_last_screening(request):
    return hypertension_dashboard.place_of_last_screening_report(request)

@csrf_exempt
def causes_of_high_blood_pressure(request):
    return hypertension_dashboard.causes_of_high_blood_pressure_report(request)

@csrf_exempt
def frequency_of_doctor_visit_for_bp_follow_up(request):
    return hypertension_dashboard.frequency_of_doctor_visit_for_bp_follow_up_report(request)

@csrf_exempt
def reasons_for_not_taking_medicine_currently(request):
    return hypertension_dashboard.reasons_for_not_taking_medicine_currently_report(request)


@csrf_exempt
def non_pharmacological_methods_blood_pressure_management(request):
    return hypertension_dashboard.non_pharmacological_methods_blood_pressure_management_report(request)

@csrf_exempt
def duration_of_hypertension(request):
    return hypertension_dashboard.duration_of_hypertension_report(request)

@csrf_exempt
def complications_of_untreated_hypertension(request):
    return hypertension_dashboard.complications_of_untreated_hypertension_report(request)

@csrf_exempt
def preferred_follow_up_location_for_hypertension(request):
    return hypertension_dashboard.preferred_follow_up_location_for_hypertension_report(request)

@csrf_exempt
def source_of_information_about_hypertension(request):
    return hypertension_dashboard.source_of_information_about_hypertension_report(request)

#diebetes Dashboard

@csrf_exempt
def gender_diebetes(request):
    return diabeted_dashboard.gender_diebetes_report(request)

@csrf_exempt
def diebetes_months_screening(request):
    return diabeted_dashboard.diebetes_months_screening_report(request) 

@csrf_exempt
def current_diebetes_medicine(request):
    return diabeted_dashboard.current_diebetes_medicine_report(request)

@csrf_exempt
def diebetes_blood_preasur_range(request):
    return diabeted_dashboard.diebetes_blood_preasur_range_report(request)

@csrf_exempt
def preventive_measures_diebetes(request):
    return diabeted_dashboard.preventive_measures_diebetes_report(request)

@csrf_exempt
def diebetes_member_age(request):
    return diabeted_dashboard.diebetes_member_age_report(request)

@csrf_exempt
def diebetes_last_recorded_blood_glucose_range(request):
    return diabeted_dashboard.diebetes_last_recorded_blood_glucose_range_report(request)

@csrf_exempt
def diabetic_medicine_currently_used(request):
    return diabeted_dashboard.diabetic_medicine_currently_used_report(request)

@csrf_exempt
def signs_and_symptoms_of_diabetes(request):
    return diabeted_dashboard.signs_and_symptoms_of_diabetes_report(request)

@csrf_exempt
def diabetic_frequency_of_currently_used_medicine(request):
    return diabeted_dashboard.diabetic_frequency_of_currently_used_medicine_report(request)

@csrf_exempt
def management_measures_of_diabetes(request):
    return diabeted_dashboard.management_measures_of_diabetes_report(request)

@csrf_exempt
def diabetic_place_of_last_screening(request):
    return diabeted_dashboard.place_of_last_screening_report(request)

@csrf_exempt
def causes_of_diabetes(request):
    return diabeted_dashboard.causes_of_diabetes_report(request)

@csrf_exempt
def diabetic_reasons_for_not_taking_medicine(request):
    return diabeted_dashboard.diabetic_reasons_for_not_taking_medicine_report(request)

@csrf_exempt
def doctor_visit_for_diabetic(request):
    return diabeted_dashboard.doctor_visit_for_diabetic_report(request)

@csrf_exempt
def duration_of_diabetes(request):
    return diabeted_dashboard.duration_of_diabetes_report(request)

@csrf_exempt
def non_pharmacological_methods_for_diabetes_management(request):
    return diabeted_dashboard.non_pharmacological_methods_for_diabetes_management_report(request)

@csrf_exempt
def complications_of_uncontrolled_diabetes(request):
    return diabeted_dashboard.complications_of_uncontrolled_diabetes_report(request)

@csrf_exempt
def follow_up_location_for_diabetic_management(request):
    return diabeted_dashboard.follow_up_location_for_diabetic_management_report(request)

@csrf_exempt
def source_of_information_about_diabetes(request):
    return diabeted_dashboard.source_of_information_about_diabetes_report(request)

#Diet and Nutrition
@csrf_exempt
def type_of_diet_followed(request):
    return diet_and_nutrition_dashboard.type_of_diet_followed_report(request)

@csrf_exempt
def daily_meal_consumption(request):
    return diet_and_nutrition_dashboard.daily_meal_consumption_report(request)

@csrf_exempt
def frequently_missed_meals(request):
    return diet_and_nutrition_dashboard.frequently_missed_meals_report(request)

@csrf_exempt
def dietary_restrictions_followed(request):
    return diet_and_nutrition_dashboard.dietary_restrictions_followed_report(request)

@csrf_exempt
def cereals_and_millets(request):
    return diet_and_nutrition_dashboard.cereals_and_millets_report(request)

@csrf_exempt
def pulses(request):
    return diet_and_nutrition_dashboard.pulses_report(request)


@csrf_exempt
def green_leafy_vegetables(request):
    return diet_and_nutrition_dashboard.green_leafy_vegetables_report(request)


@csrf_exempt
def other_vegetables(request):
    return diet_and_nutrition_dashboard.other_vegetables_report(request)

@csrf_exempt
def milk_and_milk_products(request):
    return diet_and_nutrition_dashboard.milk_and_milk_products_report(request)

@csrf_exempt
def diet_and_nutr_fruits(request):
    return diet_and_nutrition_dashboard.diet_and_nutr_fruits_report(request)

@csrf_exempt
def diet_and_nutr_egg(request):
    return diet_and_nutrition_dashboard.diet_and_nutr_egg_report(request)

@csrf_exempt
def meat_chicken(request):
    return diet_and_nutrition_dashboard.meat_chicken_report(request)

@csrf_exempt
def fast_foods(request):
    return diet_and_nutrition_dashboard.fast_foods_report(request)

@csrf_exempt
def Soft_cold_drinks(request):
    return diet_and_nutrition_dashboard.Soft_cold_drinks_report(request)

@csrf_exempt
def cereals_and_millets_percentage(request):
    return diet_and_nutrition_dashboard.cereals_and_millets_percentage_report(request)


#Health Problems
@csrf_exempt
def health_nothing(request):
    return health_problems.health_nothing_report(request)

@csrf_exempt
def heart_disease(request):
    return health_problems.heart_disease_report(request)

@csrf_exempt
def stroke(request):
    return health_problems.stroke_report(request)

@csrf_exempt
def asthma(request):
    return health_problems.asthma_report(request)

@csrf_exempt
def pcos(request):
    return health_problems.pcos_report(request)

@csrf_exempt
def thyroid(request):
    return health_problems.thyroid_report(request)

@csrf_exempt
def teeth_or_gum(request):
    return health_problems.teeth_or_gum_report(request)

@csrf_exempt
def retinopathy(request):
    return health_problems.retinopathy_report(request)

@csrf_exempt
def sexual_dysfunction(request):
    return health_problems.sexual_dysfunction_report(request)

@csrf_exempt
def moderate_physical_activity(request):
    return health_problems.moderate_physical_activity_report(request)

@csrf_exempt
def frequency_of_moderate_physical_activity(request):
    return health_problems.frequency_of_moderate_physical_activity_report(request)

@csrf_exempt
def vigorous_physical_activity(request):
    return health_problems.vigorous_physical_activity_report(request)

@csrf_exempt
def Vigorous_of_moderate_physical_activity (request):
    return health_problems.Vigorous_of_moderate_physical_activity_report(request)

@csrf_exempt
def yoga_meditation_sessions_in_the_past_3_months (request):
    return health_problems.yoga_meditation_sessions_in_the_past_3_months_report(request)

@csrf_exempt
def current_habits (request):
    return health_problems.current_habits_report(request)

@csrf_exempt
def past_habits(request):
    return health_problems.past_habits_report(request)