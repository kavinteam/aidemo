from django.http import JsonResponse
from django.db import connection
from django.views.decorators.csrf import csrf_exempt
import json
from mobileservices.api_custome_functions import api_custome_functions
from iihmrapp.models import *
from django.db.models import Max
from django.db.models import Q
import string
import random
from django.forms.models import model_to_dict

@csrf_exempt
def consent_form_insert_ajax(request):
    Qid = ''
    FormCode = ''  # Initialize FormCode variable outside of the try block
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
    try:
        cursor = connection.cursor()
        if request.method == 'POST':
            # Define request_json_validation here
            json_body = request.body.decode('utf-8')
            request_json_validation = api_custome_functions.json_validation(json_body)
            # If the JSON is valid
            if request_json_validation:
                Json_request = json.loads(json_body)
                apikey_validation = api_custome_functions.apikey_validation(Json_request)
                parameters_validation = api_custome_functions.parameters_validation(Json_request, 'syncData')
                web_service_code = api_custome_functions.webservice_code('syncData')
                receivedDate = api_custome_functions.current_date_time_in_format()
            # If JSON, API key, parameters are valid, and webservice code is not empty
            if request_json_validation and apikey_validation and parameters_validation and web_service_code !='':
                Json_request = json.loads(json_body)
                result_array = []
                synceddatetime = Json_request ['synceddatetime']
                FormCode = Json_request ['formcode']
                ApiKey = Json_request ['apikey']
                AppTypeNo = Json_request ['apptypeno']
                AppVersion = Json_request ['appversion']
                # Insert data into Qtable
                Qid = api_custome_functions.inserQtable_data(FormCode, json.dumps(Json_request), receivedDate, synceddatetime)
                # Check if the State is already exists or not
                if "trn_tbl_hh_consent_form" in Json_request:
                    child_attend_data = Json_request["trn_tbl_hh_consent_form"]
                    for child_attendance in child_attend_data:
                        fld_rf_id = child_attendance['fld_rf_id'] 
                        fld_user_swid = child_attendance['fld_user_swid'] 
                        fld_user_name = child_attendance['fld_user_name']
                        fld_state_swid = child_attendance['fld_state_swid']
                        fld_state_code = child_attendance['fld_state_code']
                        fld_state_name = child_attendance['fld_state_name']
                        fld_district_swid = child_attendance['fld_district_swid']
                        fld_district_code = child_attendance['fld_district_code']
                        fld_district_name = child_attendance['fld_district_name']
                        fld_taluk_swid = child_attendance['fld_taluk_swid']
                        fld_taluk_code = child_attendance['fld_taluk_code']
                        fld_taluk_name = child_attendance['fld_taluk_name']
                        fld_block_sw_Id = child_attendance['fld_block_sw_Id']
                        fld_block_code = child_attendance['fld_block_code']
                        fld_block_name = child_attendance['fld_block_name']
                        fld_panchyt_swid = child_attendance['fld_panchyt_swid']
                        fld_panchyt_code = child_attendance['fld_panchyt_code']
                        fld_panchyt_name = child_attendance['fld_panchyt_name']
                        fld_phc_swid = child_attendance['fld_phc_swid']
                        fld_phc_code = child_attendance['fld_phc_code']
                        fld_phc_name = child_attendance['fld_phc_name']
                        fld_hsc_hwc_swid = child_attendance['fld_hsc_hwc_swid']
                        fld_hsc_hwc_code = child_attendance['fld_hsc_hwc_code']
                        fld_hsc_hwc_name = child_attendance['fld_hsc_hwc_name']
                        fld_village_swid = child_attendance['fld_village_swid']
                        fld_village_code = child_attendance['fld_village_code']
                        fld_village_name = child_attendance['fld_village_name']
                        fld_hh_id = child_attendance['fld_hh_id']
                        fld_hh_number = child_attendance['fld_hh_number']
                        fld_respondent_name = child_attendance['fld_respondent_name']
                        fld_agreed_paricipate_survy_id = child_attendance['fld_agreed_paricipate_survy_id']
                        fld_agreed_paricipate_survy_name = child_attendance['fld_agreed_paricipate_survy_name']
                        data_source = child_attendance['fld_data_source']
                        loggedin_user_id = child_attendance['fld_loggedin_user_id']
                        is_full_form_completed_form = child_attendance['fld_is_full_form_completed_form']
                        form_start_time = child_attendance['fld_form_start_time']
                        form_end_time = child_attendance['fld_form_end_time']
                        fld_app_version = child_attendance['fld_app_version']
                        fld_date_of_interview = child_attendance['fld_date_of_interview']   
                        
                        
                       
                        
                        query = "CALL sp_hh_consent_form(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"#40
                        values_need_to_insert = (fld_rf_id,fld_user_swid,fld_user_name,fld_state_swid,fld_state_code,fld_state_name,fld_district_swid,fld_district_code,fld_district_name,fld_taluk_swid,fld_taluk_code,fld_taluk_name,fld_block_sw_Id,fld_block_code,fld_block_name,fld_panchyt_swid,fld_panchyt_code,fld_panchyt_name,fld_phc_swid,fld_phc_code,fld_phc_name,fld_hsc_hwc_swid,fld_hsc_hwc_code,fld_hsc_hwc_name,fld_village_swid,fld_village_code,fld_village_name,fld_hh_id,fld_hh_number,fld_respondent_name,fld_agreed_paricipate_survy_id,fld_agreed_paricipate_survy_name,Qid,data_source,loggedin_user_id,is_full_form_completed_form,form_start_time,form_end_time,fld_app_version,fld_date_of_interview) 
                        result = cursor.execute(query, values_need_to_insert)
                       

                    if result == True:
                        Json_response = {
                            "staus": 'True',
                            "Qid":Qid
                        }
                       
                    
                        return JsonResponse(Json_response)
                    else:
                         Json_response = {
                            "staus": 'False',
                            "Qid":Qid
                        }
                         return JsonResponse(Json_response)
                stringResponse=json.dumps(Json_response)
                api_custome_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,
                                    ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                # FormCode,valid,stringResponse,IsFullyProcessed,IsPartiallyProcessed,ReturnStatus,ApiKey,qId
                return JsonResponse(Json_response)
            else:
                        if request_json_validation is False:  # If the JSON is valid
                            Json_request = json.dumps(json.loads(json_body),default=str) # Convert the JSON data to a string
                            Json_request = json_body  # Get the JSON data
                            Qid = api_custome_functions.inserQtable_data(FormCode, str(Json_request),
                            api_custome_functions.current_date_time_in_format(), '')
                            Json_response = {
                            "error_level": "2",
                            "error_message": 'Invalid Json Request',
                            "error_file": "views.py",
                            "serverdatetime": current_date
                            }
                        return JsonResponse(Json_response)
    except Exception as e:
        Json_response = {
            "error_level": "1",
            "error_message": str(e),
            "error_file": "views.py",
            "serverdatetime": api_custome_functions.current_date_time_in_format()
        }
        api_custome_functions.error_log_insert(str(json.dumps(Json_response)), Qid, FormCode, '', 'attendance_master', '1', '1','1')
        api_custome_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,
                        ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
    return JsonResponse(Json_response)   

