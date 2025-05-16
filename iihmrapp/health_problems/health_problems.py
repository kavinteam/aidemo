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
def heart_disease_report(request):
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
                            fld_oth_hlth_prob_nothng_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM iihmr_test.trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_oth_hlth_prob_nothng_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                            iihmr_test.trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_oth_hlth_prob_nothng_name!='-999'
                        GROUP BY 
                            fld_oth_hlth_prob_nothng_name
                        ORDER BY 
                            percentage DESC;""")
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','heart_disease_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','heart_disease_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def health_nothing_report(request):
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
                            fld_heart_disease_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_heart_disease_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                            trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_heart_disease_name!='-999'
                        GROUP BY 
                            fld_heart_disease_name
                        ORDER BY 
                            percentage DESC;""")
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','health_nothing_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','health_nothing_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def stroke_report(request):
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
                            fld_stroke_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_stroke_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                            trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_stroke_name!='-999'
                        GROUP BY 
                            fld_stroke_name
                        ORDER BY 
                            percentage DESC;""")
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','stroke_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','stroke_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def asthma_report(request):
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
                            fld_asthma_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_asthma_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                            trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_asthma_name!='-999'
                        GROUP BY 
                            fld_asthma_name
                        ORDER BY 
                            percentage DESC;""")
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','asthma_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','asthma_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def pcos_report(request):
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
                            fld_pcos_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_pcos_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                            trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_pcos_name!='-999'
                        GROUP BY 
                            fld_pcos_name
                        ORDER BY 
                            percentage DESC;""")
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','pcos_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','pcos_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def thyroid_report(request):
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
                            fld_thyroid_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_thyroid_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                            trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_thyroid_name!='-999'
                        GROUP BY 
                            fld_thyroid_name
                        ORDER BY 
                            percentage DESC;""")
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','thyroid_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','thyroid_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def teeth_or_gum_report(request):
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
                            fld_teeth_or_gum_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_teeth_or_gum_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                            trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_teeth_or_gum_name!='-999'
                        GROUP BY 
                            fld_teeth_or_gum_name
                        ORDER BY 
                            percentage DESC;""")
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','teeth_or_gum_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','teeth_or_gum_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def retinopathy_report(request):
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
                            fld_retinopathy_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_retinopathy_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                            trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_retinopathy_name!='-999'
                        GROUP BY 
                            fld_retinopathy_name
                        ORDER BY 
                            percentage DESC;""")
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','retinopathy_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','retinopathy_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def sexual_dysfunction_report(request):
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
                            fld_sexul_dysfunction_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_sexul_dysfunction_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                            trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_sexul_dysfunction_name!='-999'
                        GROUP BY 
                            fld_sexul_dysfunction_name
                        ORDER BY 
                            percentage DESC;""")
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','sexual_dysfunction_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','sexual_dysfunction_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
    
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def moderate_physical_activity_report(request):
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
                            fld_any_mod_phys_actvt_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_any_mod_phys_actvt_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                            trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_any_mod_phys_actvt_name!='-999'
                        GROUP BY 
                            fld_any_mod_phys_actvt_name
                        ORDER BY 
                            percentage DESC;
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','moderate_physical_activity_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','moderate_physical_activity_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def frequency_of_moderate_physical_activity_report(request):
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
                            fld_mod_phys_actvt_fqcy_hrs_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_mod_phys_actvt_fqcy_hrs_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                                trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_mod_phys_actvt_fqcy_hrs_name!='-999'
                        GROUP BY 
                            fld_mod_phys_actvt_fqcy_hrs_name
                        ORDER BY 
                            percentage DESC;
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','frequency_of_moderate_physical_activity_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','frequency_of_moderate_physical_activity_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def vigorous_physical_activity_report(request):
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
                            fld_any_vigo_phys_actvt_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_any_vigo_phys_actvt_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                                trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_any_vigo_phys_actvt_name!='-999'
                        GROUP BY 
                            fld_any_vigo_phys_actvt_name
                        ORDER BY 
                            percentage DESC;
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','vigorous_physical_activity_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','vigorous_physical_activity_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def Vigorous_of_moderate_physical_activity_report(request):
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
                            fld_vigo_phys_actvt_fqcy_hrs_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_vigo_phys_actvt_fqcy_hrs_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                                trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_vigo_phys_actvt_fqcy_hrs_name!='-999'
                        GROUP BY 
                            fld_vigo_phys_actvt_fqcy_hrs_name
                        ORDER BY 
                            percentage DESC;
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','Vigorous_of_moderate_physical_activity_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','Vigorous_of_moderate_physical_activity_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def yoga_meditation_sessions_in_the_past_3_months_report(request):
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
                            fld_attnd_yoga_sesion_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_attnd_yoga_sesion_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                                trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_attnd_yoga_sesion_name!='-999'
                        GROUP BY 
                            fld_attnd_yoga_sesion_name
                        ORDER BY 
                            percentage DESC;
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','yoga_meditation_sessions_in_the_past_3_months_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','yoga_meditation_sessions_in_the_past_3_months_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def current_habits_report(request):
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
                            fld_at_present_do_you_have_the_following_habits_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_at_present_do_you_have_the_following_habits_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                                trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_at_present_do_you_have_the_following_habits_name!='-999'
                        GROUP BY 
                            fld_at_present_do_you_have_the_following_habits_name
                        ORDER BY 
                            percentage DESC;
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','current_habits_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','current_habits_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def past_habits_report(request):
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
                            fld_habit_of_consuming_the_following_in_the_past_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM trn_tbl_health_problem 
                                WHERE fld_is_active='1' AND fld_habit_of_consuming_the_following_in_the_past_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                                trn_tbl_health_problem
                        WHERE 
                            fld_is_active='1' 
                            AND fld_habit_of_consuming_the_following_in_the_past_name!='-999'
                        GROUP BY 
                            fld_habit_of_consuming_the_following_in_the_past_name
                        ORDER BY 
                            percentage DESC;
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','past_habits_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','past_habits_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")