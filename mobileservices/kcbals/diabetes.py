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
def diabetes_insert_ajax(request):
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
                if "trn_tbl_diabets" in Json_request:
                    diabets_data = Json_request["trn_tbl_diabets"]
                    for diabets_data_details_data_details in diabets_data:
                        fld_rf_id = diabets_data_details_data_details['fld_rf_id'] 
                        fld_state_swid = diabets_data_details_data_details['fld_state_swid'] 
                        fld_state_code = diabets_data_details_data_details['fld_state_code'] 
                        fld_state_name = diabets_data_details_data_details['fld_state_name'] 
                        fld_district_swid = diabets_data_details_data_details['fld_district_swid'] 
                        fld_district_code = diabets_data_details_data_details['fld_district_code'] 
                        fld_district_name = diabets_data_details_data_details['fld_district_name'] 
                        fld_taluk_swid = diabets_data_details_data_details['fld_taluk_swid'] 
                        fld_taluk_code = diabets_data_details_data_details['fld_taluk_code'] 
                        fld_taluk_name = diabets_data_details_data_details['fld_taluk_name'] 
                        fld_block_sw_Id = diabets_data_details_data_details['fld_block_sw_Id']
                        fld_block_code = diabets_data_details_data_details['fld_block_code']
                        fld_block_name = diabets_data_details_data_details['fld_block_name']
                        fld_panchyt_swid = diabets_data_details_data_details['fld_panchyt_swid'] 
                        fld_panchyt_code = diabets_data_details_data_details['fld_panchyt_code'] 
                        fld_panchyt_name = diabets_data_details_data_details['fld_panchyt_name'] 
                        fld_phc_swid = diabets_data_details_data_details['fld_phc_swid'] 
                        fld_phc_code = diabets_data_details_data_details['fld_phc_code'] 
                        fld_phc_name = diabets_data_details_data_details['fld_phc_name'] 
                        fld_hsc_hwc_swid = diabets_data_details_data_details['fld_hsc_hwc_swid'] 
                        fld_hsc_hwc_code = diabets_data_details_data_details['fld_hsc_hwc_code'] 
                        fld_hsc_hwc_name = diabets_data_details_data_details['fld_hsc_hwc_name'] 
                        fld_village_swid = diabets_data_details_data_details['fld_village_swid'] 
                        fld_village_code = diabets_data_details_data_details['fld_village_code'] 
                        fld_village_name = diabets_data_details_data_details['fld_village_name'] 
                        fld_hh_id = diabets_data_details_data_details['fld_hh_id'] 
                        fld_member_swid = diabets_data_details_data_details['fld_member_swid'] 
                        fld_member_id = diabets_data_details_data_details['fld_member_id'] 
                        fld_name = diabets_data_details_data_details['fld_name'] 
                        fld_since_how_long_diabtes_id = diabets_data_details_data_details['fld_since_how_long_diabtes_id'] 
                        fld_since_how_long_diabtes_name = diabets_data_details_data_details['fld_since_how_long_diabtes_name'] 
                        fld_num_of_years = diabets_data_details_data_details['fld_num_of_years'] 
                        fld_whn_lst_scrnd_for_diabtes_id = diabets_data_details_data_details['fld_whn_lst_scrnd_for_diabtes_id'] 
                        fld_whn_lst_scrnd_for_diabtes_name = diabets_data_details_data_details['fld_whn_lst_scrnd_for_diabtes_name'] 
                        fld_rang_diabtes_chekd_lst_id = diabets_data_details_data_details['fld_rang_diabtes_chekd_lst_id'] 
                        fld_rang_diabtes_chekd_lst_name = diabets_data_details_data_details['fld_rang_diabtes_chekd_lst_name'] 
                        fld_place_scrning_lst_id = diabets_data_details_data_details['fld_place_scrning_lst_id'] 
                        fld_place_scrning_lst_name = diabets_data_details_data_details['fld_place_scrning_lst_name'] 
                        fld_screening_other_specify = diabets_data_details_data_details['fld_screening_other_specify'] 
                        fld_doc_vist_dibtes_scrning_id = diabets_data_details_data_details['fld_doc_vist_dibtes_scrning_id'] 
                        fld_doc_vist_dibtes_scrning_name = diabets_data_details_data_details['fld_doc_vist_dibtes_scrning_name'] 
                        fld_reson_not_visit_doc_id = diabets_data_details_data_details['fld_reson_not_visit_doc_id'] 
                        fld_reson_not_visit_doc_name = diabets_data_details_data_details['fld_reson_not_visit_doc_name'] 
                        fld_not_visit_doc_other_specify = diabets_data_details_data_details['fld_not_visit_doc_other_specify'] 
                        fld_refer_to_faclty_dibtes_issue_id = diabets_data_details_data_details['fld_refer_to_faclty_dibtes_issue_id'] 
                        fld_refer_to_faclty_dibtes_issue_name = diabets_data_details_data_details['fld_refer_to_faclty_dibtes_issue_name'] 
                        fld_if_yes_where_id = diabets_data_details_data_details['fld_if_yes_where_id'] 
                        fld_if_yes_where = diabets_data_details_data_details['fld_if_yes_where'] 
                        fld_others_specify = diabets_data_details_data_details['fld_others_specify'] 
                        fld_visit_refrd_faclty_id = diabets_data_details_data_details['fld_visit_refrd_faclty_id'] 
                        fld_visit_refrd_faclty_name = diabets_data_details_data_details['fld_visit_refrd_faclty_name'] 
                        fld_if_no_why = diabets_data_details_data_details['fld_if_no_why'] 
                        fld_visit_other_faclty_id = diabets_data_details_data_details['fld_visit_other_faclty_id'] 
                        fld_visit_other_faclty_name = diabets_data_details_data_details['fld_visit_other_faclty_name'] 
                        fld_specify_faclty_name = diabets_data_details_data_details['fld_specify_faclty_name'] 
                        fld_follw_dibtes_mgm_id = diabets_data_details_data_details['fld_follw_dibtes_mgm_id'] 
                        fld_follw_dibtes_mgm_name = diabets_data_details_data_details['fld_follw_dibtes_mgm_name'] 
                        fld_dibts_other_specify = diabets_data_details_data_details['fld_dibts_other_specify'] 
                        fld_reson_for_prefernc = diabets_data_details_data_details['fld_reson_for_prefernc'] 
                        fld_tak_medcn_for_dibates_id = diabets_data_details_data_details['fld_tak_medcn_for_dibates_id'] 
                        fld_tak_medcn_for_dibates_name = diabets_data_details_data_details['fld_tak_medcn_for_dibates_name'] 
                        fld_resn_for_not_takng_medcn_id = diabets_data_details_data_details['fld_resn_for_not_takng_medcn_id'] 
                        fld_resn_for_not_takng_medcn_name = diabets_data_details_data_details['fld_resn_for_not_takng_medcn_name'] 
                        fld_med_not_tkn_other_spicfy = diabets_data_details_data_details['fld_med_not_tkn_other_spicfy'] 
                        fld_typ_medcn_taking_id = diabets_data_details_data_details['fld_typ_medcn_taking_id'] 
                        fld_typ_medcn_taking_name = diabets_data_details_data_details['fld_typ_medcn_taking_name'] 
                        fld_med_typ_other_spicfy = diabets_data_details_data_details['fld_med_typ_other_spicfy'] 
                        fld_what_medcn_taking = diabets_data_details_data_details['fld_what_medcn_taking'] 
                        fld_what_dosage = diabets_data_details_data_details['fld_what_dosage'] 
                        fld_freq_takng_medcn_id = diabets_data_details_data_details['fld_freq_takng_medcn_id'] 
                        fld_freq_takng_medcn_name = diabets_data_details_data_details['fld_freq_takng_medcn_name'] 
                        fld_freq_med_tak_other_spicfy = diabets_data_details_data_details['fld_freq_med_tak_other_spicfy'] 
                        fld_resn_for_not_consum_medicn_id = diabets_data_details_data_details['fld_resn_for_not_consum_medicn_id'] 
                        fld_resn_for_not_consum_medicn_name = diabets_data_details_data_details['fld_resn_for_not_consum_medicn_name'] 
                        fld_reson_med_other_specify = diabets_data_details_data_details['fld_reson_med_other_specify'] 
                        fld_oth_thng_doing_cont_dibtes_id = diabets_data_details_data_details['fld_oth_thng_doing_cont_dibtes_id'] 
                        fld_oth_thng_doing_cont_dibtes_name = diabets_data_details_data_details['fld_oth_thng_doing_cont_dibtes_name'] 
                        fld_cont_diabts_other_specify = diabets_data_details_data_details['fld_cont_diabts_other_specify'] 
                        fld_norm_fast_blod_glucos_level_id = diabets_data_details_data_details['fld_norm_fast_blod_glucos_level_id'] 
                        fld_norm_fast_blod_glucos_level_name = diabets_data_details_data_details['fld_norm_fast_blod_glucos_level_name'] 
                        fld_sign_symp_dibtes_id = diabets_data_details_data_details['fld_sign_symp_dibtes_id'] 
                        fld_sign_symp_dibtes_name = diabets_data_details_data_details['fld_sign_symp_dibtes_name'] 
                        fld_sypt_diabt_other_specify = diabets_data_details_data_details['fld_sypt_diabt_other_specify'] 
                        fld_causes_dibtes_id = diabets_data_details_data_details['fld_causes_dibtes_id'] 
                        fld_causes_dibtes_name = diabets_data_details_data_details['fld_causes_dibtes_name'] 
                        fld_caus_diabt_other_specify = diabets_data_details_data_details['fld_caus_diabt_other_specify'] 
                        fld_complicasn_uncont_dibtes_id = diabets_data_details_data_details['fld_complicasn_uncont_dibtes_id'] 
                        fld_complicasn_uncont_dibtes_name = diabets_data_details_data_details['fld_complicasn_uncont_dibtes_name'] 
                        fld_unctrl_diabtes_other_specify = diabets_data_details_data_details['fld_unctrl_diabtes_other_specify'] 
                        fld_how_prevnt_dibtes_id = diabets_data_details_data_details['fld_how_prevnt_dibtes_id'] 
                        fld_how_prevnt_dibtes_name = diabets_data_details_data_details['fld_how_prevnt_dibtes_name'] 
                        fld_prev_diabts_other_specify = diabets_data_details_data_details['fld_prev_diabts_other_specify'] 
                        fld_how_manage_dibtes_id = diabets_data_details_data_details['fld_how_manage_dibtes_id'] 
                        fld_how_manage_dibtes_name = diabets_data_details_data_details['fld_how_manage_dibtes_name'] 
                        fld_manag_dibts_other_specify = diabets_data_details_data_details['fld_manag_dibts_other_specify'] 
                        fld_sourc_awarnss_dibtes_id = diabets_data_details_data_details['fld_sourc_awarnss_dibtes_id'] 
                        fld_sourc_awarnss_dibtes_name = diabets_data_details_data_details['fld_sourc_awarnss_dibtes_name'] 
                        fld_awrns_ht_other_specify = diabets_data_details_data_details['fld_awrns_ht_other_specify']     
                        fld_data_source = diabets_data_details_data_details['fld_data_source'] 
                        fld_loggedin_user_id  = diabets_data_details_data_details['fld_loggedin_user_id'] 
                        fld_is_full_form_completed_form = diabets_data_details_data_details['fld_is_full_form_completed_form'] 
                        fld_form_start_time = diabets_data_details_data_details['fld_form_start_time'] 
                        fld_form_end_time = diabets_data_details_data_details['fld_form_end_time'] 
                        fld_app_version = diabets_data_details_data_details['fld_app_version']  
                        fld_date_of_interview = diabets_data_details_data_details['fld_date_of_interview']   
                        fld_interviewer_name = diabets_data_details_data_details['fld_interviewer_name']
                        
                        
                        values_need_to_insert = (fld_rf_id, fld_state_swid,fld_state_code,fld_state_name,fld_district_swid,fld_district_code,fld_district_name,fld_taluk_swid,fld_taluk_code,fld_taluk_name,fld_block_sw_Id,fld_block_code,fld_block_name,fld_panchyt_swid,fld_panchyt_code,fld_panchyt_name,fld_phc_swid,fld_phc_code,fld_phc_name,fld_hsc_hwc_swid,fld_hsc_hwc_code,fld_hsc_hwc_name,fld_village_swid,fld_village_code,fld_village_name,fld_hh_id,fld_member_swid,fld_member_id,fld_name,fld_since_how_long_diabtes_id,fld_since_how_long_diabtes_name,fld_num_of_years,fld_whn_lst_scrnd_for_diabtes_id,fld_whn_lst_scrnd_for_diabtes_name,fld_rang_diabtes_chekd_lst_id,fld_rang_diabtes_chekd_lst_name,fld_place_scrning_lst_id,fld_place_scrning_lst_name,fld_screening_other_specify,fld_doc_vist_dibtes_scrning_id,fld_doc_vist_dibtes_scrning_name,fld_reson_not_visit_doc_id,fld_reson_not_visit_doc_name,fld_not_visit_doc_other_specify,fld_refer_to_faclty_dibtes_issue_id,fld_refer_to_faclty_dibtes_issue_name,fld_if_yes_where_id,fld_if_yes_where,fld_others_specify,fld_visit_refrd_faclty_id,fld_visit_refrd_faclty_name,fld_if_no_why,fld_visit_other_faclty_id,fld_visit_other_faclty_name,fld_specify_faclty_name,fld_follw_dibtes_mgm_id,fld_follw_dibtes_mgm_name,fld_dibts_other_specify,fld_reson_for_prefernc,fld_tak_medcn_for_dibates_id,fld_tak_medcn_for_dibates_name,fld_resn_for_not_takng_medcn_id,fld_resn_for_not_takng_medcn_name,fld_med_not_tkn_other_spicfy,fld_typ_medcn_taking_id,fld_typ_medcn_taking_name,fld_med_typ_other_spicfy,fld_what_medcn_taking,fld_what_dosage,fld_freq_takng_medcn_id,fld_freq_takng_medcn_name,fld_freq_med_tak_other_spicfy,fld_resn_for_not_consum_medicn_id,fld_resn_for_not_consum_medicn_name,fld_reson_med_other_specify,fld_oth_thng_doing_cont_dibtes_id,fld_oth_thng_doing_cont_dibtes_name,fld_cont_diabts_other_specify,fld_norm_fast_blod_glucos_level_id,fld_norm_fast_blod_glucos_level_name,fld_sign_symp_dibtes_id,fld_sign_symp_dibtes_name,fld_sypt_diabt_other_specify,fld_causes_dibtes_id,fld_causes_dibtes_name,fld_caus_diabt_other_specify,fld_complicasn_uncont_dibtes_id,fld_complicasn_uncont_dibtes_name,fld_unctrl_diabtes_other_specify,fld_how_prevnt_dibtes_id,fld_how_prevnt_dibtes_name,fld_prev_diabts_other_specify,fld_how_manage_dibtes_id,fld_how_manage_dibtes_name,fld_manag_dibts_other_specify,fld_sourc_awarnss_dibtes_id,fld_sourc_awarnss_dibtes_name,fld_awrns_ht_other_specify,Qid,fld_data_source,fld_loggedin_user_id,fld_is_full_form_completed_form,fld_form_start_time,fld_form_end_time,fld_app_version,fld_date_of_interview,fld_interviewer_name)

                        # Prepare the query
                        query = "CALL sp_diabets(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"#101

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

