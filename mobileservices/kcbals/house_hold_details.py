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
def house_hold_details_insert_ajax(request):
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
                if "trn_tbl_gi_house_hold_detail" in Json_request:
                    house_hold_detail_data = Json_request["trn_tbl_gi_house_hold_detail"]
                    for house_hold_detail in house_hold_detail_data:
                        fld_rf_id = house_hold_detail['fld_rf_id'] 
                        fld_state_swid= house_hold_detail['fld_state_swid'] 
                        fld_state_code= house_hold_detail['fld_state_code'] 
                        fld_state_name= house_hold_detail['fld_state_name'] 
                        fld_district_swid= house_hold_detail['fld_district_swid'] 
                        fld_district_code= house_hold_detail['fld_district_code'] 
                        fld_district_name= house_hold_detail['fld_district_name'] 
                        fld_taluk_swid= house_hold_detail['fld_taluk_swid'] 
                        fld_taluk_code= house_hold_detail['fld_taluk_code'] 
                        fld_taluk_name= house_hold_detail['fld_taluk_name'] 
                        fld_block_sw_Id = house_hold_detail['fld_block_sw_Id']
                        fld_block_code = house_hold_detail['fld_block_code']
                        fld_block_name = house_hold_detail['fld_block_name']
                        fld_panchyt_swid= house_hold_detail['fld_panchyt_swid'] 
                        fld_panchyt_code= house_hold_detail['fld_panchyt_code'] 
                        fld_panchyt_name= house_hold_detail['fld_panchyt_name'] 
                        fld_phc_swid= house_hold_detail['fld_phc_swid'] 
                        fld_phc_code= house_hold_detail['fld_phc_code'] 
                        fld_phc_name= house_hold_detail['fld_phc_name'] 
                        fld_hsc_hwc_swid= house_hold_detail['fld_hsc_hwc_swid'] 
                        fld_hsc_hwc_code= house_hold_detail['fld_hsc_hwc_code'] 
                        fld_hsc_hwc_name= house_hold_detail['fld_hsc_hwc_name'] 
                        fld_village_swid= house_hold_detail['fld_village_swid'] 
                        fld_village_code= house_hold_detail['fld_village_code'] 
                        fld_village_name= house_hold_detail['fld_village_name'] 
                        fld_respondent_name= house_hold_detail['fld_respondent_name'] 
                        fld_interviewer_name= house_hold_detail['fld_interviewer_name'] 
                        fld_hh_number= house_hold_detail['fld_hh_number'] 
                        fld_date_of_interview= house_hold_detail['fld_date_of_interview'] 
                        fld_hh_id= house_hold_detail['fld_hh_id'] 
                        fld_hh_head_name= house_hold_detail['fld_hh_head_name'] 
                        fld_address= house_hold_detail['fld_address'] 
                        fld_age_respondnt= house_hold_detail['fld_age_respondnt'] 
                        fld_gender_respndnt_id= house_hold_detail['fld_gender_respndnt_id'] 
                        fld_gender_respndent_name= house_hold_detail['fld_gender_respndent_name'] 
                        fld_educat_respondnt_id= house_hold_detail['fld_educat_respondnt_id'] 
                        fld_educat_respondnt_name= house_hold_detail['fld_educat_respondnt_name'] 
                        fld_maritlstats_respondnt_id= house_hold_detail['fld_maritlstats_respondnt_id'] 
                        fld_maritlstats_respondnt_name= house_hold_detail['fld_maritlstats_respondnt_name'] 
                        fld_family_membrs= house_hold_detail['fld_family_membrs'] 
                        fld_type_family_id= house_hold_detail['fld_type_family_id'] 
                        fld_type_family_name= house_hold_detail['fld_type_family_name'] 
                        fld_religin_id= house_hold_detail['fld_religin_id'] 
                        fld_religin_name= house_hold_detail['fld_religin_name'] 
                        fld_religin_others_specify= house_hold_detail['fld_religin_others_specify'] 
                        fld_cast_id= house_hold_detail['fld_cast_id'] 
                        fld_cast_name= house_hold_detail['fld_cast_name'] 
                        fld_data_source = house_hold_detail['fld_data_source'] 
                        fld_loggedin_user_id  = house_hold_detail['fld_loggedin_user_id'] 
                        fld_is_full_form_completed_form = house_hold_detail['fld_is_full_form_completed_form'] 
                        fld_form_start_time = house_hold_detail['fld_form_start_time'] 
                        fld_form_end_time = house_hold_detail['fld_form_end_time'] 
                        fld_app_version = house_hold_detail['fld_app_version']
                        
                        
                        query = "CALL sp_house_hold_detail(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"#50
                        values_need_to_insert = (fld_rf_id,fld_state_swid,fld_state_code,fld_state_name,fld_district_swid,fld_district_code,fld_district_name,fld_taluk_swid,fld_taluk_code,fld_taluk_name,fld_block_sw_Id,fld_block_code,fld_block_name,fld_panchyt_swid,fld_panchyt_code,fld_panchyt_name,fld_phc_swid,fld_phc_code,fld_phc_name,fld_hsc_hwc_swid,fld_hsc_hwc_code,fld_hsc_hwc_name,fld_village_swid,fld_village_code,fld_village_name,fld_respondent_name,fld_interviewer_name,fld_hh_number,fld_date_of_interview,fld_hh_id,fld_hh_head_name,fld_address,fld_age_respondnt,fld_gender_respndnt_id,fld_gender_respndent_name,fld_educat_respondnt_id,fld_educat_respondnt_name,fld_maritlstats_respondnt_id,fld_maritlstats_respondnt_name,fld_family_membrs,fld_type_family_id,fld_type_family_name,fld_religin_id,fld_religin_name,fld_religin_others_specify,fld_cast_id,fld_cast_name,Qid,fld_data_source,fld_loggedin_user_id,fld_is_full_form_completed_form,fld_form_start_time,fld_form_end_time,fld_app_version) 
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

