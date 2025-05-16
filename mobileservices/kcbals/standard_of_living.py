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
def standard_of_living_insert_ajax(request):
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
                if "trn_tbl_standred_of_living" in Json_request:
                    standard_of_living_data = Json_request["trn_tbl_standred_of_living"]
                    for standard_of_living_details in standard_of_living_data:
                        fld_rf_id = standard_of_living_details['fld_rf_id'] 
                        fld_state_swid = standard_of_living_details['fld_state_swid']
                        fld_state_code = standard_of_living_details['fld_state_code']
                        fld_state_name = standard_of_living_details['fld_state_name']
                        fld_district_swid = standard_of_living_details['fld_district_swid']
                        fld_district_code = standard_of_living_details['fld_district_code']
                        fld_district_name = standard_of_living_details['fld_district_name']
                        fld_taluk_swid = standard_of_living_details['fld_taluk_swid']
                        fld_taluk_code = standard_of_living_details['fld_taluk_code']
                        fld_taluk_name = standard_of_living_details['fld_taluk_name']
                        fld_block_sw_Id = standard_of_living_details['fld_block_sw_Id']
                        fld_block_code = standard_of_living_details['fld_block_code']
                        fld_block_name = standard_of_living_details['fld_block_name']
                        fld_panchyt_swid = standard_of_living_details['fld_panchyt_swid']
                        fld_panchyt_code = standard_of_living_details['fld_panchyt_code']
                        fld_panchyt_name = standard_of_living_details['fld_panchyt_name']
                        fld_phc_swid = standard_of_living_details['fld_phc_swid']
                        fld_phc_code = standard_of_living_details['fld_phc_code']
                        fld_phc_name = standard_of_living_details['fld_phc_name']
                        fld_hsc_hwc_swid = standard_of_living_details['fld_hsc_hwc_swid']
                        fld_hsc_hwc_code = standard_of_living_details['fld_hsc_hwc_code']
                        fld_hsc_hwc_name = standard_of_living_details['fld_hsc_hwc_name']
                        fld_village_swid = standard_of_living_details['fld_village_swid']
                        fld_village_code = standard_of_living_details['fld_village_code']
                        fld_village_name = standard_of_living_details['fld_village_name']
                        fld_hh_id = standard_of_living_details['fld_hh_id']
                        fld_ownership_house_id = standard_of_living_details['fld_ownership_house_id']
                        fld_ownership_house_name = standard_of_living_details['fld_ownership_house_name']
                        fld_typ_housing_structre_id = standard_of_living_details['fld_typ_housing_structre_id']
                        fld_typ_housing_structre_name = standard_of_living_details['fld_typ_housing_structre_name']
                        fld_main_sourc_drink_water_id = standard_of_living_details['fld_main_sourc_drink_water_id']
                        fld_main_sourc_drink_water_name = standard_of_living_details['fld_main_sourc_drink_water_name']
                        fld_Drink_watr_other_specify = standard_of_living_details['fld_Drink_watr_other_specify']
                        fld_make_safe_water_drink_id = standard_of_living_details['fld_make_safe_water_drink_id']
                        fld_make_safe_water_drink_name = standard_of_living_details['fld_make_safe_water_drink_name']
                        fld_toilet_faclty_hh_mem_use_id = standard_of_living_details['fld_toilet_faclty_hh_mem_use_id']
                        fld_toilet_faclty_hh_mem_use_name = standard_of_living_details['fld_toilet_faclty_hh_mem_use_name']
                        fld_tolit_fac_other_specify = standard_of_living_details['fld_tolit_fac_other_specify']
                        fld_do_all_family_mem_use_toilet_id = standard_of_living_details['fld_do_all_family_mem_use_toilet_id']
                        fld_do_all_family_mem_use_toilet_name = standard_of_living_details['fld_do_all_family_mem_use_toilet_name']
                        fld_if_no_why_id = standard_of_living_details['fld_if_no_why_id']
                        fld_if_no_why_name = standard_of_living_details['fld_if_no_why_name']
                        fld_no_why_other_specify = standard_of_living_details['fld_no_why_other_specify']
                        fld_does_electricity_connection_id = standard_of_living_details['fld_does_electricity_connection_id']
                        fld_does_electricity_connection_name = standard_of_living_details['fld_does_electricity_connection_name']
                        fld_typ_fuel_for_cooking_id = standard_of_living_details['fld_typ_fuel_for_cooking_id']
                        fld_typ_fuel_for_cooking_name = standard_of_living_details['fld_typ_fuel_for_cooking_name']
                        fld_fule_other_specify = standard_of_living_details['fld_fule_other_specify']
                        fld_do_u_hav_ration_card_id = standard_of_living_details['fld_do_u_hav_ration_card_id']
                        fld_do_u_hav_ration_card_name = standard_of_living_details['fld_do_u_hav_ration_card_name']
                        fld_if_yes_verify_ration_card_id = standard_of_living_details['fld_if_yes_verify_ration_card_id']
                        fld_if_yes_verify_ration_card_name = standard_of_living_details['fld_if_yes_verify_ration_card_name']
                        fld_u_any_family_mem_hav_health_ins_policy_id = standard_of_living_details['fld_u_any_family_mem_hav_health_ins_policy_id']
                        fld_u_any_family_mem_hav_health_ins_policy_name = standard_of_living_details['fld_u_any_family_mem_hav_health_ins_policy_name']
                        fld_if_yes_verify_insu_card_id = standard_of_living_details['fld_if_yes_verify_insu_card_id']
                        fld_if_yes_verify_insu_card_name = standard_of_living_details['fld_if_yes_verify_insu_card_name']
                        fld_privat_ins_scheme_name = standard_of_living_details['fld_privat_ins_scheme_name']
                        fld_privat_in_other_specify = standard_of_living_details['fld_privat_in_other_specify']
                        fld_typ_salt_prepare_food_id = standard_of_living_details['fld_typ_salt_prepare_food_id']
                        fld_typ_salt_prepare_food_name = standard_of_living_details['fld_typ_salt_prepare_food_name']
                        fld_transport_facilty_public_trans_bus_id = standard_of_living_details['fld_transport_facilty_public_trans_bus_id']
                        fld_transport_facilty_public_trans_bus_name = standard_of_living_details['fld_transport_facilty_public_trans_bus_name']
                        fld_transport_facilty_rickshaw_id = standard_of_living_details['fld_transport_facilty_rickshaw_id']
                        fld_transport_facilty_rickshaw_name = standard_of_living_details['fld_transport_facilty_rickshaw_name']
                        fld_transport_facilty_auto_rickshaw_id = standard_of_living_details['fld_transport_facilty_auto_rickshaw_id']
                        fld_transport_facilty_auto_rickshaw_name = standard_of_living_details['fld_transport_facilty_auto_rickshaw_name']
                        fld_transport_facilty_taxi_van_jeep_id = standard_of_living_details['fld_transport_facilty_taxi_van_jeep_id']
                        fld_transport_facilty_taxi_van_jeep_name = standard_of_living_details['fld_transport_facilty_taxi_van_jeep_name']
                        fld_transport_facilty_any_other_specfy_id = standard_of_living_details['fld_transport_facilty_any_other_specfy_id']
                        fld_transport_facilty_any_other_specfy_name = standard_of_living_details['fld_transport_facilty_any_other_specfy_name']
                        fld_transport_facility_other_specify = standard_of_living_details['fld_transport_facility_other_specify']
                        fld_does_vill_hav_mobil_net_id = standard_of_living_details['fld_does_vill_hav_mobil_net_id']
                        fld_does_vill_hav_mobil_net_name = standard_of_living_details['fld_does_vill_hav_mobil_net_name']
                        fld_commnity_bas_org_in_vill_id = standard_of_living_details['fld_commnity_bas_org_in_vill_id']
                        fld_commnity_bas_org_in_vill_name = standard_of_living_details['fld_commnity_bas_org_in_vill_name']
                        fld_any_cbo_other_specify = standard_of_living_details['fld_any_cbo_other_specify']
                        fld_wht_hapng_area_id = standard_of_living_details['fld_wht_hapng_area_id']
                        fld_wht_hapng_area_name = standard_of_living_details['fld_wht_hapng_area_name']
                        fld_wht_hapn_others_specify = standard_of_living_details['fld_wht_hapn_others_specify']
                        fld_wht_ent_pop_id = standard_of_living_details['fld_wht_ent_pop_id']
                        fld_wht_ent_pop_name = standard_of_living_details['fld_wht_ent_pop_name']
                        fld_entermntt_any_other_specify = standard_of_living_details['fld_entermntt_any_other_specify']
                        fld_regulr_sorc_med_care_id = standard_of_living_details['fld_regulr_sorc_med_care_id']
                        fld_regulr_sorc_med_care_name = standard_of_living_details['fld_regulr_sorc_med_care_name']
                        fld_other_response_specify = standard_of_living_details['fld_other_response_specify']
                        fld_dist_to_phc_id = standard_of_living_details['fld_dist_to_phc_id']
                        fld_dist_to_phc = standard_of_living_details['fld_dist_to_phc']
                        fld_phc_distance = standard_of_living_details['fld_phc_distance']
                        fld_dist_to_shc_frm_village_id = standard_of_living_details['fld_dist_to_shc_frm_village_id']
                        fld_dist_to_shc_frm_village = standard_of_living_details['fld_dist_to_shc_frm_village']
                        fld_shc_distance = standard_of_living_details['fld_shc_distance']
                        fld_visit_phc_id = standard_of_living_details['fld_visit_phc_id']
                        fld_visit_phc_name = standard_of_living_details['fld_visit_phc_name']
                        fld_reason_not_visit_phc_id = standard_of_living_details['fld_reason_not_visit_phc_id']
                        fld_reason_not_visit_phc_name = standard_of_living_details['fld_reason_not_visit_phc_name']
                        fld_visit_phc_any_other_specify = standard_of_living_details['fld_visit_phc_any_other_specify']
                        fld_difficlt_in_reaching_phc_id = standard_of_living_details['fld_difficlt_in_reaching_phc_id']
                        fld_difficlt_in_reaching_phc_name = standard_of_living_details['fld_difficlt_in_reaching_phc_name']
                        fld_difficult_faced_any_other_specify = standard_of_living_details['fld_difficult_faced_any_other_specify']
                        fld_data_source = standard_of_living_details['fld_data_source'] 
                        fld_loggedin_user_id  = standard_of_living_details['fld_loggedin_user_id'] 
                        fld_is_full_form_completed_form = standard_of_living_details['fld_is_full_form_completed_form'] 
                        fld_form_start_time = standard_of_living_details['fld_form_start_time'] 
                        fld_form_end_time = standard_of_living_details['fld_form_end_time'] 
                        fld_app_version = standard_of_living_details['fld_app_version']
                        fld_date_of_interview = standard_of_living_details['fld_date_of_interview']   
                        fld_interviewer_name = standard_of_living_details['fld_interviewer_name']
                        
                        
                        
                        query = "CALL sp_standard_of_living(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"#104
                        values_need_to_insert = (fld_rf_id,fld_state_swid,fld_state_code,fld_state_name,fld_district_swid,fld_district_code,fld_district_name,fld_taluk_swid,fld_taluk_code,fld_taluk_name,fld_block_sw_Id,fld_block_code,fld_block_name,fld_panchyt_swid,fld_panchyt_code,fld_panchyt_name,fld_phc_swid,fld_phc_code,fld_phc_name,fld_hsc_hwc_swid,fld_hsc_hwc_code,fld_hsc_hwc_name,fld_village_swid,fld_village_code,fld_village_name,fld_hh_id,fld_ownership_house_id,fld_ownership_house_name,fld_typ_housing_structre_id,fld_typ_housing_structre_name,fld_main_sourc_drink_water_id,fld_main_sourc_drink_water_name,fld_Drink_watr_other_specify,fld_make_safe_water_drink_id,fld_make_safe_water_drink_name,fld_toilet_faclty_hh_mem_use_id,fld_toilet_faclty_hh_mem_use_name,fld_tolit_fac_other_specify,fld_do_all_family_mem_use_toilet_id,fld_do_all_family_mem_use_toilet_name,fld_if_no_why_id,fld_if_no_why_name,fld_no_why_other_specify,fld_does_electricity_connection_id,fld_does_electricity_connection_name,fld_typ_fuel_for_cooking_id,fld_typ_fuel_for_cooking_name,fld_fule_other_specify,fld_do_u_hav_ration_card_id,fld_do_u_hav_ration_card_name,fld_if_yes_verify_ration_card_id,fld_if_yes_verify_ration_card_name,fld_u_any_family_mem_hav_health_ins_policy_id,fld_u_any_family_mem_hav_health_ins_policy_name,fld_if_yes_verify_insu_card_id,fld_if_yes_verify_insu_card_name,fld_privat_ins_scheme_name,fld_privat_in_other_specify,fld_typ_salt_prepare_food_id,fld_typ_salt_prepare_food_name,fld_transport_facilty_public_trans_bus_id,fld_transport_facilty_public_trans_bus_name,fld_transport_facilty_rickshaw_id,fld_transport_facilty_rickshaw_name,fld_transport_facilty_auto_rickshaw_id,fld_transport_facilty_auto_rickshaw_name,fld_transport_facilty_taxi_van_jeep_id,fld_transport_facilty_taxi_van_jeep_name,fld_transport_facilty_any_other_specfy_id,fld_transport_facilty_any_other_specfy_name,fld_transport_facility_other_specify,fld_does_vill_hav_mobil_net_id,fld_does_vill_hav_mobil_net_name,fld_commnity_bas_org_in_vill_id,fld_commnity_bas_org_in_vill_name,fld_any_cbo_other_specify,fld_wht_hapng_area_id,fld_wht_hapng_area_name,fld_wht_hapn_others_specify,fld_wht_ent_pop_id,fld_wht_ent_pop_name,fld_entermntt_any_other_specify,fld_regulr_sorc_med_care_id,fld_regulr_sorc_med_care_name,fld_other_response_specify,fld_dist_to_phc_id,fld_dist_to_phc,fld_phc_distance,fld_dist_to_shc_frm_village_id,fld_dist_to_shc_frm_village,fld_shc_distance,fld_visit_phc_id,fld_visit_phc_name,fld_reason_not_visit_phc_id,fld_reason_not_visit_phc_name,fld_visit_phc_any_other_specify,fld_difficlt_in_reaching_phc_id,fld_difficlt_in_reaching_phc_name,fld_difficult_faced_any_other_specify,Qid,fld_data_source,fld_loggedin_user_id,fld_is_full_form_completed_form,fld_form_start_time,fld_form_end_time,fld_app_version,fld_date_of_interview,fld_interviewer_name) 
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

