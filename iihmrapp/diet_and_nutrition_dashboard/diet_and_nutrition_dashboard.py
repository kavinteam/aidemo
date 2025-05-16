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
def type_of_diet_followed_report(request):
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
                            fld_type_diet_follw_name,
                            COUNT(*) AS count,
                            ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM trn_tbl_diet_and_nutrition)), 2) AS percentage
                        FROM 
                            trn_tbl_diet_and_nutrition
                             where fld_is_active='1' and fld_type_diet_follw_name!='-999'
                        GROUP BY 
                            fld_type_diet_follw_name
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
def daily_meal_consumption_report(request):
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
                            fld_do_u_eat_folw_meal_name,
                            COUNT(*) AS count,
                            ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM trn_tbl_diet_and_nutrition)), 2) AS percentage
                        FROM 
                            trn_tbl_diet_and_nutrition
                            where fld_is_active='1' and fld_do_u_eat_folw_meal_name!='-999'
                        GROUP BY 
                            fld_do_u_eat_folw_meal_name
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','daily_meal_consumption_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','daily_meal_consumption_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def frequently_missed_meals_report(request):
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
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""WITH RECURSIVE split_meals AS (
                        SELECT 
                            fld_which_meal_miss_name,
                            SUBSTRING_INDEX(fld_which_meal_miss_name, '$', 1) AS meal,
                            SUBSTRING(fld_which_meal_miss_name, LENGTH(SUBSTRING_INDEX(fld_which_meal_miss_name, '$', 1)) + 2) AS rest
                        FROM trn_tbl_diet_and_nutrition
                        WHERE fld_is_active = '1' AND fld_which_meal_miss_name != '-999'

                        UNION ALL

                        SELECT
                            fld_which_meal_miss_name,
                            SUBSTRING_INDEX(rest, '$', 1) AS meal,
                            CASE 
                                WHEN rest LIKE '%$%' THEN SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest, '$', 1)) + 2)
                                ELSE NULL
                            END AS rest
                        FROM split_meals
                        WHERE rest IS NOT NULL AND rest != ''
                    ),
                    final AS (
                        SELECT meal
                        FROM split_meals
                        WHERE meal IS NOT NULL AND meal != ''
                    ),
                    total AS (
                        SELECT COUNT(*) AS total_count FROM final
                    )
                    SELECT 
                        meal AS fld_which_meal_miss_name,
                        COUNT(*) AS count,
                        ROUND(COUNT(*) * 100.0 / (SELECT total_count FROM total), 2) AS percentage
                    FROM final
                    GROUP BY meal
                    ORDER BY percentage DESC;""")
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','frequently_missed_meals_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','frequently_missed_meals_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def dietary_restrictions_followed_report(request):
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
                    table_data = api_custom_functions.getting_data_in_dictionary_format(f"""WITH RECURSIVE split_values AS (
                        SELECT 
                            SUBSTRING_INDEX(fld_kind_of_diet_restrct_follw_name, '$', 1) AS value,
                            SUBSTRING(fld_kind_of_diet_restrct_follw_name, LOCATE('$', fld_kind_of_diet_restrct_follw_name) + 1) AS rest
                        FROM (
                            SELECT 
                            fld_kind_of_diet_restrct_follw_name 
                            FROM trn_tbl_diet_and_nutrition
                            WHERE fld_is_active = '1' AND fld_kind_of_diet_restrct_follw_name != '-999'
                        ) AS base

                        UNION ALL

                        SELECT 
                            SUBSTRING_INDEX(rest, '$', 1) AS value,
                            SUBSTRING(rest, LOCATE('$', rest) + 1) AS rest
                        FROM split_values
                        WHERE rest != '' AND LOCATE('$', rest) > 0

                        UNION ALL

                        SELECT 
                            rest AS value,
                            '' AS rest
                        FROM split_values
                        WHERE rest != '' AND LOCATE('$', rest) = 0
                        )

                        SELECT 
                        TRIM(value) AS diet_type,
                        COUNT(*) AS count,
                        CONCAT(ROUND((COUNT(*) * 100 / (SELECT COUNT(*) FROM split_values)), 2), '%') AS percentage
                        FROM split_values
                        GROUP BY diet_type
                        ORDER BY count DESC;
                        ;""")
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','dietary_restrictions_followed_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','dietary_restrictions_followed_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def cereals_and_millets_report(request):
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
                        option_name,
                        COUNT(*) AS count
                    FROM (
                        SELECT 
                            TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_food_consupsn_cereals_name, '$', numbers.n), '$', -1)) AS option_name
                        FROM 
                            trn_tbl_diet_and_nutrition h
                        JOIN 
                            (
                                SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                            ) AS numbers
                            ON CHAR_LENGTH(h.fld_food_consupsn_cereals_name)
                                - CHAR_LENGTH(REPLACE(h.fld_food_consupsn_cereals_name, '$', '')) >= numbers.n - 1
                        WHERE 
                            h.fld_is_active = '1'
                            AND h.fld_food_consupsn_cereals_name != '-999'
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','cereals_and_millets_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','cereals_and_millets_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def pulses_report(request):
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
                        option_name,
                        COUNT(*) AS count
                    FROM (
                        SELECT 
                            TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_food_consupsn_pulses_name, '$', numbers.n), '$', -1)) AS option_name
                        FROM 
                            trn_tbl_diet_and_nutrition h
                        JOIN 
                            (
                                SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                            ) AS numbers
                            ON CHAR_LENGTH(h.fld_food_consupsn_pulses_name)
                                - CHAR_LENGTH(REPLACE(h.fld_food_consupsn_pulses_name, '$', '')) >= numbers.n - 1
                        WHERE 
                            h.fld_is_active = '1'
                            AND h.fld_food_consupsn_pulses_name != '-999'
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','pulses_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','pulses_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def green_leafy_vegetables_report(request):
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
                        option_name,
                        COUNT(*) AS count
                    FROM (
                        SELECT 
                            TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_food_consupsn_gren_leaf_vegt_name, '$', numbers.n), '$', -1)) AS option_name
                        FROM 
                            trn_tbl_diet_and_nutrition h
                        JOIN 
                            (
                                SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                            ) AS numbers
                            ON CHAR_LENGTH(h.fld_food_consupsn_gren_leaf_vegt_name)
                                - CHAR_LENGTH(REPLACE(h.fld_food_consupsn_gren_leaf_vegt_name, '$', '')) >= numbers.n - 1
                        WHERE 
                            h.fld_is_active = '1'
                            AND h.fld_food_consupsn_gren_leaf_vegt_name != '-999'
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','green_leafy_vegetables_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','green_leafy_vegetables_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def other_vegetables_report(request):
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
                        option_name,
                        COUNT(*) AS count
                    FROM (
                        SELECT 
                            TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_food_consupsn_other_vegt_name, '$', numbers.n), '$', -1)) AS option_name
                        FROM 
                            trn_tbl_diet_and_nutrition h
                        JOIN 
                            (
                                SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                            ) AS numbers
                            ON CHAR_LENGTH(h.fld_food_consupsn_other_vegt_name)
                                - CHAR_LENGTH(REPLACE(h.fld_food_consupsn_other_vegt_name, '$', '')) >= numbers.n - 1
                        WHERE 
                            h.fld_is_active = '1'
                            AND h.fld_food_consupsn_other_vegt_name != '-999'
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','other_vegetables_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','other_vegetables_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def milk_and_milk_products_report(request):
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
                        option_name,
                        COUNT(*) AS count
                    FROM (
                        SELECT 
                            TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_food_consupsn_milk_prodct_name, '$', numbers.n), '$', -1)) AS option_name
                        FROM 
                            trn_tbl_diet_and_nutrition h
                        JOIN 
                            (
                                SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                            ) AS numbers
                            ON CHAR_LENGTH(h.fld_food_consupsn_milk_prodct_name)
                                - CHAR_LENGTH(REPLACE(h.fld_food_consupsn_milk_prodct_name, '$', '')) >= numbers.n - 1
                        WHERE 
                            h.fld_is_active = '1'
                            AND h.fld_food_consupsn_milk_prodct_name != '-999'
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','milk_and_milk_products_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','milk_and_milk_products_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def diet_and_nutr_fruits_report(request):
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
                        option_name,
                        COUNT(*) AS count
                    FROM (
                        SELECT 
                            TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_food_consupsn_fruits_name, '$', numbers.n), '$', -1)) AS option_name
                        FROM 
                            trn_tbl_diet_and_nutrition h
                        JOIN 
                            (
                                SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                            ) AS numbers
                            ON CHAR_LENGTH(h.fld_food_consupsn_fruits_name)
                                - CHAR_LENGTH(REPLACE(h.fld_food_consupsn_fruits_name, '$', '')) >= numbers.n - 1
                        WHERE 
                            h.fld_is_active = '1'
                            AND h.fld_food_consupsn_fruits_name != '-999'
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','diet_and_nutr_fruits_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','diet_and_nutr_fruits_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def diet_and_nutr_egg_report(request):
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
                        option_name,
                        COUNT(*) AS count
                    FROM (
                        SELECT 
                            TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_food_consupsn_egg_name, '$', numbers.n), '$', -1)) AS option_name
                        FROM 
                            trn_tbl_diet_and_nutrition h
                        JOIN 
                            (
                                SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                            ) AS numbers
                            ON CHAR_LENGTH(h.fld_food_consupsn_egg_name)
                                - CHAR_LENGTH(REPLACE(h.fld_food_consupsn_egg_name, '$', '')) >= numbers.n - 1
                        WHERE 
                            h.fld_is_active = '1'
                            AND h.fld_food_consupsn_egg_name != '-999'
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','diet_and_nutr_egg_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','diet_and_nutr_egg_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def meat_chicken_report(request):
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
                        option_name,
                        COUNT(*) AS count
                    FROM (
                        SELECT 
                            TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_food_consupsn_meat_chikn_name, '$', numbers.n), '$', -1)) AS option_name
                        FROM 
                            trn_tbl_diet_and_nutrition h
                        JOIN 
                            (
                                SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                            ) AS numbers
                            ON CHAR_LENGTH(h.fld_food_consupsn_meat_chikn_name)
                                - CHAR_LENGTH(REPLACE(h.fld_food_consupsn_meat_chikn_name, '$', '')) >= numbers.n - 1
                        WHERE 
                            h.fld_is_active = '1'
                            AND h.fld_food_consupsn_meat_chikn_name != '-999'
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','meat_chicken_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','meat_chicken_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def fast_foods_report(request):
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
                        option_name,
                        COUNT(*) AS count
                    FROM (
                        SELECT 
                            TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_food_consupsn_fast_food_name, '$', numbers.n), '$', -1)) AS option_name
                        FROM 
                            trn_tbl_diet_and_nutrition h
                        JOIN 
                            (
                                SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                            ) AS numbers
                            ON CHAR_LENGTH(h.fld_food_consupsn_fast_food_name)
                                - CHAR_LENGTH(REPLACE(h.fld_food_consupsn_fast_food_name, '$', '')) >= numbers.n - 1
                        WHERE 
                            h.fld_is_active = '1'
                            AND h.fld_food_consupsn_fast_food_name != '-999'
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','fast_foods_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','fast_foods_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     

@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def Soft_cold_drinks_report(request):
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
                        option_name,
                        COUNT(*) AS count
                    FROM (
                        SELECT 
                            TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(h.fld_food_consupsn_soft_drink_name, '$', numbers.n), '$', -1)) AS option_name
                        FROM 
                            trn_tbl_diet_and_nutrition h
                        JOIN 
                            (
                                SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                                UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                                UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                            ) AS numbers
                            ON CHAR_LENGTH(h.fld_food_consupsn_soft_drink_name)
                                - CHAR_LENGTH(REPLACE(h.fld_food_consupsn_soft_drink_name, '$', '')) >= numbers.n - 1
                        WHERE 
                            h.fld_is_active = '1'
                            AND h.fld_food_consupsn_soft_drink_name != '-999'
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','Soft_cold_drinks_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','Soft_cold_drinks_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
# @api_custom_functions.retry_on_deadlock
def cereals_and_millets_percentage_report(request):
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
                            fld_food_consupsn_cereals_name, 
                            COUNT(*) AS count, 
                            ROUND((COUNT(*) * 100.0 / 
                                (SELECT COUNT(*) FROM iihmr_test.trn_tbl_diet_and_nutrition 
                                WHERE fld_is_active='1' AND fld_food_consupsn_cereals_name!='-999')
                            ), 2) AS percentage 
                        FROM 
                            iihmr_test.trn_tbl_diet_and_nutrition
                        WHERE 
                            fld_is_active='1' 
                            AND fld_food_consupsn_cereals_name!='-999'
                        GROUP BY 
                            fld_food_consupsn_cereals_name
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
                api_custom_functions.error_log_insert(str(Json_response),Qid,'','','cereals_and_millets_percentage_report','1','1','2')
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
            api_custom_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','cereals_and_millets_percentage_report','1','1','1')
            api_custom_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")