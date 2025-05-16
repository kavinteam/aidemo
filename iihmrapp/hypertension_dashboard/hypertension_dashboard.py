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
from decimal import Decimal

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def gender_hypertension_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'gender_hypertension_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('gender_hypertension_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT 
                            b.fld_gender_name,
                            COUNT(*) AS count
                        FROM 
                            trn_tbl_hypertension AS a
                        INNER JOIN 
                            trn_tbl_family_details AS b 
                            ON a.fld_member_id = b.fld_member_id
                        WHERE 
                            a.fld_is_active = '1' and b.fld_is_active='1'
                        GROUP BY 
                            b.fld_gender_name;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','gender_hypertension_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','gender_hypertension_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def hypertension_months_screening_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'hypertension_months_screening_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('hypertension_months_screening_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT fld_lst_scrnd_hypertnsn_name, COUNT(*) AS count 
                            FROM trn_tbl_hypertension where fld_is_active='1' 
                            and fld_lst_scrnd_hypertnsn_name!='-999' group by fld_lst_scrnd_hypertnsn_name;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','hypertension_months_screening_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','hypertension_months_screening_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def current_hyperteion_medicine_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'current_hyperteion_medicine_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('current_hyperteion_medicine_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT fld_tak_medcn_for_hyprtnsn_name, COUNT(*) AS count 
                            FROM trn_tbl_hypertension where fld_is_active='1' 
                            and fld_lst_scrnd_hypertnsn_name!='-999' group by fld_tak_medcn_for_hyprtnsn_name;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','current_hyperteion_medicine_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','current_hyperteion_medicine_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def hyperteion_blood_preasur_range_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'hyperteion_blood_preasur_range_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('hyperteion_blood_preasur_range_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT fld_norm_bp_range_name, COUNT(*) AS count 
                                FROM trn_tbl_hypertension where fld_is_active='1' 
                                and fld_lst_scrnd_hypertnsn_name!='-999' group by fld_norm_bp_range_name;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','hyperteion_blood_preasur_range_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','hyperteion_blood_preasur_range_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def preventive_measures_hypertension_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'preventive_measures_hypertension_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('preventive_measures_hypertension_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT 
                            option_name,
                            COUNT(*) AS count
                        FROM (
                            SELECT 
                                TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_oth_thng_doing_cont_bp_name, '$', numbers.n), '$', -1)) AS option_name
                            FROM 
                                trn_tbl_hypertension h
                            JOIN 
                                (
                                    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                    UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                                ) AS numbers
                                ON CHAR_LENGTH(h.fld_oth_thng_doing_cont_bp_name)
                                    - CHAR_LENGTH(REPLACE(h.fld_oth_thng_doing_cont_bp_name, '$', '')) >= numbers.n - 1
                            WHERE 
                                h.fld_is_active = '1'
                                AND h.fld_oth_thng_doing_cont_bp_name != '-999'
                        ) AS split_values
                        GROUP BY 
                            option_name
                        ORDER BY 
                            count DESC;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','preventive_measures_hypertension_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','preventive_measures_hypertension_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def hypertensive_medicine_currently_used_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'hypertensive_medicine_currently_used_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('hypertensive_medicine_currently_used_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT fld_typ_medcn_taking_name, COUNT(*) AS count 
                        FROM trn_tbl_hypertension where fld_is_active='1' 
                        and fld_typ_medcn_taking_name!='-999' group by fld_typ_medcn_taking_name;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','hypertensive_medicine_currently_used_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','hypertensive_medicine_currently_used_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def hypertension_member_age_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'hypertension_member_age_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('hypertension_member_age_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT 
                                CASE 
                                    WHEN b.fld_age BETWEEN 18 AND 25 THEN '18-25 years'
                                    WHEN b.fld_age BETWEEN 26 AND 44 THEN '26-44 years'
                                    WHEN b.fld_age BETWEEN 45 AND 59 THEN '45-59 years'
                                    WHEN b.fld_age >= 60 THEN '60 and above'
                                    ELSE 'Unknown'
                                END AS age_group,
                                COUNT(*) AS count
                            FROM 
                                trn_tbl_hypertension AS a
                            INNER JOIN 
                                trn_tbl_family_details AS b 
                                ON a.fld_member_id = b.fld_member_id
                            WHERE 
                                a.fld_is_active = '1' 
                                AND b.fld_is_active = '1'
                            GROUP BY 
                                age_group
                            ORDER BY 
                                age_group;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','hypertension_member_age_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','hypertension_member_age_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     


@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def last_recorded_blood_pressure_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'last_recorded_blood_pressure_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('last_recorded_blood_pressure_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT fld_norm_bp_range_name, COUNT(*) AS count 
                        FROM trn_tbl_hypertension where fld_is_active='1' 
                        and fld_norm_bp_range_name!='-999' group by fld_norm_bp_range_name;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','last_recorded_blood_pressure_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','last_recorded_blood_pressure_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def signs_and_symptoms_of_high_blood_pressure_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'signs_and_symptoms_of_high_blood_pressure_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('signs_and_symptoms_of_high_blood_pressure_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT 
                            option_name,
                            COUNT(*) AS count
                        FROM (
                            SELECT 
                                TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_sign_symp_high_bp_name, '$', numbers.n), '$', -1)) AS option_name
                            FROM 
                                trn_tbl_hypertension h
                            JOIN 
                                (
                                    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                    UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                                ) AS numbers
                                ON CHAR_LENGTH(h.fld_sign_symp_high_bp_name)
                                    - CHAR_LENGTH(REPLACE(h.fld_sign_symp_high_bp_name, '$', '')) >= numbers.n - 1
                            WHERE 
                                h.fld_is_active = '1'
                                AND h.fld_sign_symp_high_bp_name != '-999'
                        ) AS split_values
                        GROUP BY 
                            option_name
                        ORDER BY 
                        count DESC;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','signs_and_symptoms_of_high_blood_pressure_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','signs_and_symptoms_of_high_blood_pressure_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def frequency_of_currently_used_medicine_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'frequency_of_currently_used_medicine_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('frequency_of_currently_used_medicine_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT fld_freq_takng_medcn_name, COUNT(*) AS count 
                        FROM trn_tbl_hypertension where fld_is_active='1' 
                        and fld_freq_takng_medcn_name!='-999' group by fld_freq_takng_medcn_name;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','frequency_of_currently_used_medicine_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','frequency_of_currently_used_medicine_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def management_measures_of_hypertension_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'management_measures_of_hypertension_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('management_measures_of_hypertension_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT 
                            option_name,
                            COUNT(*) AS count
                        FROM (
                            SELECT 
                                TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_how_mng_hypertsiv_name, '$', numbers.n), '$', -1)) AS option_name
                            FROM 
                                trn_tbl_hypertension h
                            JOIN 
                                (
                                    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                    UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                                ) AS numbers
                                ON CHAR_LENGTH(h.fld_how_mng_hypertsiv_name)
                                    - CHAR_LENGTH(REPLACE(h.fld_how_mng_hypertsiv_name, '$', '')) >= numbers.n - 1
                            WHERE 
                                h.fld_is_active = '1'
                                AND h.fld_how_mng_hypertsiv_name != '-999'
                        ) AS split_values
                        GROUP BY 
                            option_name
                        ORDER BY 
                            count DESC;
                        """)
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','management_measures_of_hypertension_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','management_measures_of_hypertension_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def place_of_last_screening_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'place_of_last_screening_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('place_of_last_screening_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT fld_screning_plac_lst_tim_name, COUNT(*) AS count 
                            FROM trn_tbl_hypertension where fld_is_active='1' 
                            and fld_screning_plac_lst_tim_name!='-999' group by fld_screning_plac_lst_tim_name;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','place_of_last_screening_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','place_of_last_screening_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def causes_of_high_blood_pressure_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'causes_of_high_blood_pressure_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('causes_of_high_blood_pressure_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT 
                            option_name,
                            COUNT(*) AS count
                        FROM (
                            SELECT 
                                TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_causes_high_bp_name, '$', numbers.n), '$', -1)) AS option_name
                            FROM 
                                trn_tbl_hypertension h
                            JOIN 
                                (
                                    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                    UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                                ) AS numbers
                                ON CHAR_LENGTH(h.fld_causes_high_bp_name)
                                    - CHAR_LENGTH(REPLACE(h.fld_causes_high_bp_name, '$', '')) >= numbers.n - 1
                            WHERE 
                                h.fld_is_active = '1'
                                AND h.fld_causes_high_bp_name != '-999'
                        ) AS split_values
                        GROUP BY 
                            option_name
                        ORDER BY 
                            count DESC;
                        """)
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','causes_of_high_blood_pressure_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','causes_of_high_blood_pressure_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def frequency_of_doctor_visit_for_bp_follow_up_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'frequency_of_doctor_visit_for_bp_follow_up_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('frequency_of_doctor_visit_for_bp_follow_up_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT fld_doc_vist_bp_scrning_name, COUNT(*) AS count 
                        FROM trn_tbl_hypertension where fld_is_active='1' 
                        and fld_doc_vist_bp_scrning_name!='-999' group by fld_doc_vist_bp_scrning_name;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','frequency_of_doctor_visit_for_bp_follow_up_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','frequency_of_doctor_visit_for_bp_follow_up_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def reasons_for_not_taking_medicine_currently_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'reasons_for_not_taking_medicine_currently_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('reasons_for_not_taking_medicine_currently_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT fld_resn_for_not_takng_medcn_name, COUNT(*) AS count 
                        FROM trn_tbl_hypertension where fld_is_active='1' 
                        and fld_resn_for_not_takng_medcn_name!='-999' group by fld_resn_for_not_takng_medcn_name;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','reasons_for_not_taking_medicine_currently_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','reasons_for_not_taking_medicine_currently_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def non_pharmacological_methods_blood_pressure_management_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'non_pharmacological_methods_blood_pressure_management_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('non_pharmacological_methods_blood_pressure_management_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT 
                                            option_name,
                                            COUNT(*) AS count
                                        FROM (
                                            SELECT 
                                                TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_oth_thng_doing_cont_bp_name, '$', numbers.n), '$', -1)) AS option_name
                                            FROM 
                                                trn_tbl_hypertension h
                                            JOIN (
                                                SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                                UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                                UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                                            ) AS numbers
                                                ON numbers.n <= 1 + CHAR_LENGTH(h.fld_oth_thng_doing_cont_bp_name)
                                                            - CHAR_LENGTH(REPLACE(h.fld_oth_thng_doing_cont_bp_name, '$', ''))
                                            WHERE 
                                                h.fld_is_active = '1'
                                                AND h.fld_oth_thng_doing_cont_bp_name IS NOT NULL
                                                AND h.fld_oth_thng_doing_cont_bp_name NOT IN ('', '-999')
                                        ) AS split_values
                                        GROUP BY 
                                            option_name
                                        ORDER BY 
                                            count DESC;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','non_pharmacological_methods_blood_pressure_management_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','non_pharmacological_methods_blood_pressure_management_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def duration_of_hypertension_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'duration_of_hypertension_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('duration_of_hypertension_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT '<5 years' AS age_group, 
                            (SELECT COUNT(*) 
                                FROM trn_tbl_hypertension a
                                INNER JOIN trn_tbl_family_details b 
                                ON a.fld_member_id = b.fld_member_id
                                WHERE a.fld_is_active = '1' AND b.fld_is_active = '1' AND b.fld_age < 5) AS count
                        UNION ALL
                        SELECT '<10 years',
                            (SELECT COUNT(*) 
                                FROM trn_tbl_hypertension a
                                INNER JOIN trn_tbl_family_details b 
                                ON a.fld_member_id = b.fld_member_id
                                WHERE a.fld_is_active = '1' AND b.fld_is_active = '1' AND b.fld_age >= 5 AND b.fld_age < 10)
                        UNION ALL
                        SELECT '10 years and above',
                            (SELECT COUNT(*) 
                                FROM trn_tbl_hypertension a
                                INNER JOIN trn_tbl_family_details b 
                                ON a.fld_member_id = b.fld_member_id
                                WHERE a.fld_is_active = '1' AND b.fld_is_active = '1' AND b.fld_age >= 10)
                        UNION ALL
                        SELECT 'Don''t know/don''t remember',
                            (SELECT COUNT(*) 
                                FROM trn_tbl_hypertension a
                                INNER JOIN trn_tbl_family_details b 
                                ON a.fld_member_id = b.fld_member_id
                                WHERE a.fld_is_active = '1' AND b.fld_is_active = '1' AND b.fld_age IS NULL);""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','duration_of_hypertension_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','duration_of_hypertension_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def complications_of_untreated_hypertension_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'complications_of_untreated_hypertension_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('complications_of_untreated_hypertension_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT 
                            option_name,
                            COUNT(*) AS count
                        FROM (
                            SELECT 
                                TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_happn_bp_remns_untreatd_name, '$', numbers.n), '$', -1)) AS option_name
                            FROM 
                                trn_tbl_hypertension h
                            JOIN 
                                (
                                    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                    UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                                ) AS numbers
                                ON CHAR_LENGTH(h.fld_happn_bp_remns_untreatd_name)
                                    - CHAR_LENGTH(REPLACE(h.fld_happn_bp_remns_untreatd_name, '$', '')) >= numbers.n - 1
                            WHERE 
                                h.fld_is_active = '1'
                                AND h.fld_happn_bp_remns_untreatd_name != '-999'
                        ) AS split_values
                        GROUP BY 
                            option_name
                        ORDER BY 
                            count DESC;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','complications_of_untreated_hypertension_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','complications_of_untreated_hypertension_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def preferred_follow_up_location_for_hypertension_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'preferred_follow_up_location_for_hypertension_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('preferred_follow_up_location_for_hypertension_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT fld_follw_hyprtnsn_mgm_name, COUNT(*) AS count 
                        FROM trn_tbl_hypertension where fld_is_active='1' 
                        and fld_follw_hyprtnsn_mgm_name!='-999' group by fld_follw_hyprtnsn_mgm_name;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','preferred_follow_up_location_for_hypertension_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','preferred_follow_up_location_for_hypertension_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def source_of_information_about_hypertension_report(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
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
        FormCode = ''
        try:
            json_body = request.body
            request_json_validation = api_custom_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custom_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custom_functions.parameters_validation(Json_request, 'source_of_information_about_hypertension_report') # validation the parameters
                webservice_code = api_custom_functions.web_service_code('source_of_information_about_hypertension_report')  # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
                if apikey_validation == True and parameters_validation == True and webservice_code !='':
                    Json_request = json.loads(request.body)
                    datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                    # get the json data
                    Json_request = json.loads(request.body)
                    FormCode = Json_request['FormCode'] 
                    ApiKey = Json_request['ApiKey'] 
                    AppTypeNo = Json_request['AppTypeNo'] 
                    AppVersion = Json_request['AppVersion']
                    synceddatetime = Json_request['synceddatetime']
                    jsonData_database = str(json.dumps(Json_request))
                    Qid = api_custom_functions.inserQtable_data(webservice_code,jsonData_database,api_custom_functions.current_date_time_in_format(),synceddatetime)
                    # Assuming 'requested_id' should contain the user ID for querying master_tbl_attendance_child
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""SELECT fld_follw_hyprtnsn_mgm_name, COUNT(*) AS count 
                        FROM trn_tbl_hypertension where fld_is_active='1' 
                        and fld_follw_hyprtnsn_mgm_name!='-999' group by fld_follw_hyprtnsn_mgm_name;""")
                    # Custom JSON encoder to handle Decimal types
                    class DecimalEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, Decimal):
                                return float(obj)
                            return super(DecimalEncoder, self).default(obj)                
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": valid,
                        "responsemessage": table_data,
                        "serverdatetime": api_custom_functions.current_date_time_in_format()
                    }
                    # Convert the dictionary to a JSON string using the custom encoder
                    stringResponse = json.dumps(Json_response, cls=DecimalEncoder)
                    # Call the update function
                    api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)                    
                    return JsonResponse(Json_response, encoder=DecimalEncoder)
                    # return JsonResponse(Json_response)
                else:
                    valid = 2
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Parameter valid went wrong.",
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
                "error_level": "3",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','source_of_information_about_hypertension_report','1','1','2')
                api_custom_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
                return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custom_functions.inserQtable_data(webservice_code,str(Json_request),api_custom_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "4",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custom_functions.current_date_time_in_format()
                }
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','source_of_information_about_hypertension_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

