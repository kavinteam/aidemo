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
def diet_and_nutrition_insert_ajax(request):
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
                if "trn_tbl_diet_and_nutrition" in Json_request:
                    diet_and_nutrition_data = Json_request["trn_tbl_diet_and_nutrition"]
                    for diet_and_nutrition_details in diet_and_nutrition_data:
                        fld_rf_id = diet_and_nutrition_details['fld_rf_id'] 
                        fld_state_swid = diet_and_nutrition_details['fld_state_swid'] 
                        fld_state_code = diet_and_nutrition_details['fld_state_code'] 
                        fld_state_name = diet_and_nutrition_details['fld_state_name'] 
                        fld_district_swid = diet_and_nutrition_details['fld_district_swid'] 
                        fld_district_code = diet_and_nutrition_details['fld_district_code'] 
                        fld_district_name = diet_and_nutrition_details['fld_district_name'] 
                        fld_taluk_swid = diet_and_nutrition_details['fld_taluk_swid'] 
                        fld_taluk_code = diet_and_nutrition_details['fld_taluk_code'] 
                        fld_taluk_name = diet_and_nutrition_details['fld_taluk_name'] 
                        fld_block_sw_Id = diet_and_nutrition_details['fld_block_sw_Id']
                        fld_block_code = diet_and_nutrition_details['fld_block_code']
                        fld_block_name = diet_and_nutrition_details['fld_block_name']
                        fld_panchyt_swid = diet_and_nutrition_details['fld_panchyt_swid'] 
                        fld_panchyt_code = diet_and_nutrition_details['fld_panchyt_code'] 
                        fld_panchyt_name = diet_and_nutrition_details['fld_panchyt_name'] 
                        fld_phc_swid = diet_and_nutrition_details['fld_phc_swid'] 
                        fld_phc_code = diet_and_nutrition_details['fld_phc_code'] 
                        fld_phc_name = diet_and_nutrition_details['fld_phc_name'] 
                        fld_hsc_hwc_swid = diet_and_nutrition_details['fld_hsc_hwc_swid'] 
                        fld_hsc_hwc_code = diet_and_nutrition_details['fld_hsc_hwc_code'] 
                        fld_hsc_hwc_name = diet_and_nutrition_details['fld_hsc_hwc_name'] 
                        fld_village_swid = diet_and_nutrition_details['fld_village_swid'] 
                        fld_village_code = diet_and_nutrition_details['fld_village_code'] 
                        fld_village_name = diet_and_nutrition_details['fld_village_name'] 
                        fld_hh_id = diet_and_nutrition_details['fld_hh_id'] 
                        fld_member_swid = diet_and_nutrition_details['fld_member_swid'] 
                        fld_member_id = diet_and_nutrition_details['fld_member_id'] 
                        fld_name = diet_and_nutrition_details['fld_name'] 
                        fld_type_diet_follw_id = diet_and_nutrition_details['fld_type_diet_follw_id'] 
                        fld_type_diet_follw_name = diet_and_nutrition_details['fld_type_diet_follw_name'] 
                        fld_do_u_eat_folw_meal_id = diet_and_nutrition_details['fld_do_u_eat_folw_meal_id'] 
                        fld_do_u_eat_folw_meal_name = diet_and_nutrition_details['fld_do_u_eat_folw_meal_name'] 
                        fld_which_meal_miss_id = diet_and_nutrition_details['fld_which_meal_miss_id'] 
                        fld_which_meal_miss_name = diet_and_nutrition_details['fld_which_meal_miss_name'] 
                        fld_why_miss_meal_id = diet_and_nutrition_details['fld_why_miss_meal_id'] 
                        fld_why_miss_meal_meal = diet_and_nutrition_details['fld_why_miss_meal_meal'] 
                        fld_miss_meal_other_reasons = diet_and_nutrition_details['fld_miss_meal_other_reasons'] 
                        fld_kind_of_diet_restrct_follw_id = diet_and_nutrition_details['fld_kind_of_diet_restrct_follw_id'] 
                        fld_kind_of_diet_restrct_follw_name = diet_and_nutrition_details['fld_kind_of_diet_restrct_follw_name'] 
                        fld_diet_restrct_others_specify = diet_and_nutrition_details['fld_diet_restrct_others_specify'] 
                        fld_food_consupsn_cereals_id = diet_and_nutrition_details['fld_food_consupsn_cereals_id'] 
                        fld_food_consupsn_cereals_name = diet_and_nutrition_details['fld_food_consupsn_cereals_name'] 
                        fld_food_consupsn_pulses_id = diet_and_nutrition_details['fld_food_consupsn_pulses_id'] 
                        fld_food_consupsn_pulses_name = diet_and_nutrition_details['fld_food_consupsn_pulses_name'] 
                        fld_food_consupsn_gren_leaf_vegt_id = diet_and_nutrition_details['fld_food_consupsn_gren_leaf_vegt_id'] 
                        fld_food_consupsn_gren_leaf_vegt_name = diet_and_nutrition_details['fld_food_consupsn_gren_leaf_vegt_name'] 
                        fld_food_consupsn_other_vegt_id = diet_and_nutrition_details['fld_food_consupsn_other_vegt_id'] 
                        fld_food_consupsn_other_vegt_name = diet_and_nutrition_details['fld_food_consupsn_other_vegt_name'] 
                        fld_food_consupsn_milk_prodct_id = diet_and_nutrition_details['fld_food_consupsn_milk_prodct_id'] 
                        fld_food_consupsn_milk_prodct_name = diet_and_nutrition_details['fld_food_consupsn_milk_prodct_name'] 
                        fld_food_consupsn_fruits_id = diet_and_nutrition_details['fld_food_consupsn_fruits_id'] 
                        fld_food_consupsn_fruits_name = diet_and_nutrition_details['fld_food_consupsn_fruits_name'] 
                        fld_food_consupsn_egg_id = diet_and_nutrition_details['fld_food_consupsn_egg_id'] 
                        fld_food_consupsn_egg_name = diet_and_nutrition_details['fld_food_consupsn_egg_name'] 
                        fld_food_consupsn_meat_chikn_id = diet_and_nutrition_details['fld_food_consupsn_meat_chikn_id'] 
                        fld_food_consupsn_meat_chikn_name = diet_and_nutrition_details['fld_food_consupsn_meat_chikn_name'] 
                        fld_food_consupsn_fast_food_id = diet_and_nutrition_details['fld_food_consupsn_fast_food_id'] 
                        fld_food_consupsn_fast_food_name = diet_and_nutrition_details['fld_food_consupsn_fast_food_name'] 
                        fld_food_consupsn_soft_drink_id = diet_and_nutrition_details['fld_food_consupsn_soft_drink_id'] 
                        fld_food_consupsn_soft_drink_name = diet_and_nutrition_details['fld_food_consupsn_soft_drink_name'] 
                        fld_data_source = diet_and_nutrition_details['fld_data_source'] 
                        fld_loggedin_user_id  = diet_and_nutrition_details['fld_loggedin_user_id'] 
                        fld_is_full_form_completed_form = diet_and_nutrition_details['fld_is_full_form_completed_form'] 
                        fld_form_start_time = diet_and_nutrition_details['fld_form_start_time'] 
                        fld_form_end_time = diet_and_nutrition_details['fld_form_end_time']   
                        fld_app_version = diet_and_nutrition_details['fld_app_version']  
                        fld_date_of_interview = diet_and_nutrition_details['fld_date_of_interview']   
                        fld_interviewer_name = diet_and_nutrition_details['fld_interviewer_name']
                        
                        
                        
                        values_need_to_insert = (fld_rf_id,fld_state_swid,fld_state_code,fld_state_name,fld_district_swid,fld_district_code,fld_district_name,fld_taluk_swid,fld_taluk_code,fld_taluk_name,fld_block_sw_Id,fld_block_code,fld_block_name,fld_panchyt_swid,fld_panchyt_code,fld_panchyt_name,fld_phc_swid,fld_phc_code,fld_phc_name,fld_hsc_hwc_swid,fld_hsc_hwc_code,fld_hsc_hwc_name,fld_village_swid,fld_village_code,fld_village_name,fld_hh_id,fld_member_swid,fld_member_id,fld_name,fld_type_diet_follw_id,fld_type_diet_follw_name,fld_do_u_eat_folw_meal_id,fld_do_u_eat_folw_meal_name,fld_which_meal_miss_id,fld_which_meal_miss_name,fld_why_miss_meal_id,fld_why_miss_meal_meal,fld_miss_meal_other_reasons,fld_kind_of_diet_restrct_follw_id,fld_kind_of_diet_restrct_follw_name,fld_diet_restrct_others_specify,fld_food_consupsn_cereals_id,fld_food_consupsn_cereals_name,fld_food_consupsn_pulses_id,fld_food_consupsn_pulses_name,fld_food_consupsn_gren_leaf_vegt_id,fld_food_consupsn_gren_leaf_vegt_name,fld_food_consupsn_other_vegt_id,fld_food_consupsn_other_vegt_name,fld_food_consupsn_milk_prodct_id,fld_food_consupsn_milk_prodct_name,fld_food_consupsn_fruits_id,fld_food_consupsn_fruits_name,fld_food_consupsn_egg_id,fld_food_consupsn_egg_name,fld_food_consupsn_meat_chikn_id,fld_food_consupsn_meat_chikn_name,fld_food_consupsn_fast_food_id,fld_food_consupsn_fast_food_name,fld_food_consupsn_soft_drink_id,fld_food_consupsn_soft_drink_name,Qid,fld_data_source,fld_loggedin_user_id,fld_is_full_form_completed_form,fld_form_start_time,fld_form_end_time,fld_app_version,fld_date_of_interview,fld_interviewer_name)

                        # Prepare the query
                        query = "CALL sp_diet_and_nutrition(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"#66

                        # Execute the query
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
                            "serverdatetime": api_custome_functions.current_date_time_in_format()
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

