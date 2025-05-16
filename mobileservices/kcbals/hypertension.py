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
def hypertension_insert_ajax(request):
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
    current_date = api_custome_functions.current_date_time_in_format()
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
                if "trn_tbl_hypertension" in Json_request:
                    hypertension_data = Json_request["trn_tbl_hypertension"]
                    for hypertension_data_details in hypertension_data:
                        fld_rf_id = hypertension_data_details['fld_rf_id'] 
                        fld_state_swid = hypertension_data_details['fld_state_swid'] 
                        fld_state_code = hypertension_data_details['fld_state_code'] 
                        fld_state_name = hypertension_data_details['fld_state_name'] 
                        fld_district_swid = hypertension_data_details['fld_district_swid'] 
                        fld_district_code = hypertension_data_details['fld_district_code'] 
                        fld_district_name = hypertension_data_details['fld_district_name'] 
                        fld_taluk_swid = hypertension_data_details['fld_taluk_swid'] 
                        fld_taluk_code = hypertension_data_details['fld_taluk_code'] 
                        fld_taluk_name = hypertension_data_details['fld_taluk_name'] 
                        fld_block_sw_Id = hypertension_data_details['fld_block_sw_Id']
                        fld_block_code = hypertension_data_details['fld_block_code']
                        fld_block_name = hypertension_data_details['fld_block_name']
                        fld_panchyt_swid = hypertension_data_details['fld_panchyt_swid'] 
                        fld_panchyt_code = hypertension_data_details['fld_panchyt_code'] 
                        fld_panchyt_name = hypertension_data_details['fld_panchyt_name'] 
                        fld_phc_swid = hypertension_data_details['fld_phc_swid'] 
                        fld_phc_code = hypertension_data_details['fld_phc_code'] 
                        fld_phc_name = hypertension_data_details['fld_phc_name'] 
                        fld_hsc_hwc_swid = hypertension_data_details['fld_hsc_hwc_swid'] 
                        fld_hsc_hwc_code = hypertension_data_details['fld_hsc_hwc_code'] 
                        fld_hsc_hwc_name = hypertension_data_details['fld_hsc_hwc_name'] 
                        fld_village_swid = hypertension_data_details['fld_village_swid'] 
                        fld_village_code = hypertension_data_details['fld_village_code'] 
                        fld_village_name = hypertension_data_details['fld_village_name'] 
                        fld_hh_id = hypertension_data_details['fld_hh_id'] 
                        fld_member_swid = hypertension_data_details['fld_member_swid'] 
                        fld_member_id = hypertension_data_details['fld_member_id'] 
                        fld_name = hypertension_data_details['fld_name'] 
                        fld_bp_duaration_id = hypertension_data_details['fld_bp_duaration_id'] 
                        fld_bp_duaration_name = hypertension_data_details['fld_bp_duaration_name'] 
                        fld_num_of_years = hypertension_data_details['fld_num_of_years'] 
                        fld_lst_scrnd_hypertnsn_id = hypertension_data_details['fld_lst_scrnd_hypertnsn_id'] 
                        fld_lst_scrnd_hypertnsn_name = hypertension_data_details['fld_lst_scrnd_hypertnsn_name'] 
                        fld_lst_time_bp_what_id = hypertension_data_details['fld_lst_time_bp_what_id'] 
                        fld_lst_time_bp_what_name = hypertension_data_details['fld_lst_time_bp_what_name'] 
                        fld_bp_value = hypertension_data_details['fld_bp_value'] 
                        fld_screning_plac_lst_tim_id = hypertension_data_details['fld_screning_plac_lst_tim_id'] 
                        fld_screning_plac_lst_tim_name = hypertension_data_details['fld_screning_plac_lst_tim_name'] 
                        fld_screen_place_other_specify = hypertension_data_details['fld_screen_place_other_specify'] 
                        fld_doc_vist_bp_scrning_id = hypertension_data_details['fld_doc_vist_bp_scrning_id'] 
                        fld_doc_vist_bp_scrning_name = hypertension_data_details['fld_doc_vist_bp_scrning_name'] 
                        fld_reson_not_visit_doc_id = hypertension_data_details['fld_reson_not_visit_doc_id'] 
                        fld_reson_not_visit_doc_name = hypertension_data_details['fld_reson_not_visit_doc_name'] 
                        fld_reason_visit_other_specify = hypertension_data_details['fld_reason_visit_other_specify'] 
                        fld_refer_to_faclty_bp_issue_id = hypertension_data_details['fld_refer_to_faclty_bp_issue_id'] 
                        fld_refer_to_faclty_bp_issue_name = hypertension_data_details['fld_refer_to_faclty_bp_issue_name'] 
                        fld_if_yes_where_id = hypertension_data_details['fld_if_yes_where_id'] 
                        fld_if_yes_where = hypertension_data_details['fld_if_yes_where'] 
                        fld_others_specify = hypertension_data_details['fld_others_specify'] 
                        fld_visit_refrd_faclty_id = hypertension_data_details['fld_visit_refrd_faclty_id'] 
                        fld_visit_refrd_faclty_name = hypertension_data_details['fld_visit_refrd_faclty_name'] 
                        fld_if_no_why = hypertension_data_details['fld_if_no_why'] 
                        fld_visit_other_faclty_id = hypertension_data_details['fld_visit_other_faclty_id'] 
                        fld_visit_other_faclty_name = hypertension_data_details['fld_visit_other_faclty_name'] 
                        fld_specify_faclty_name = hypertension_data_details['fld_specify_faclty_name'] 
                        fld_follw_hyprtnsn_mgm_id = hypertension_data_details['fld_follw_hyprtnsn_mgm_id'] 
                        fld_follw_hyprtnsn_mgm_name = hypertension_data_details['fld_follw_hyprtnsn_mgm_name'] 
                        fld_hyprtnsn_other_specify = hypertension_data_details['fld_hyprtnsn_other_specify'] 
                        fld_reson_for_prefernc = hypertension_data_details['fld_reson_for_prefernc'] 
                        fld_tak_medcn_for_hyprtnsn_id = hypertension_data_details['fld_tak_medcn_for_hyprtnsn_id'] 
                        fld_tak_medcn_for_hyprtnsn_name = hypertension_data_details['fld_tak_medcn_for_hyprtnsn_name'] 
                        fld_resn_for_not_takng_medcn_id = hypertension_data_details['fld_resn_for_not_takng_medcn_id'] 
                        fld_resn_for_not_takng_medcn_name = hypertension_data_details['fld_resn_for_not_takng_medcn_name'] 
                        fld_med_not_tkn_reson_other_spicfy = hypertension_data_details['fld_med_not_tkn_reson_other_spicfy'] 
                        fld_typ_medcn_taking_id = hypertension_data_details['fld_typ_medcn_taking_id'] 
                        fld_typ_medcn_taking_name = hypertension_data_details['fld_typ_medcn_taking_name'] 
                        fld_typ_med_other_spicfy = hypertension_data_details['fld_typ_med_other_spicfy'] 
                        fld_what_medcn_taking = hypertension_data_details['fld_what_medcn_taking'] 
                        fld_what_dosage = hypertension_data_details['fld_what_dosage'] 
                        fld_freq_takng_medcn_id = hypertension_data_details['fld_freq_takng_medcn_id'] 
                        fld_freq_takng_medcn_name = hypertension_data_details['fld_freq_takng_medcn_name'] 
                        fld_resn_for_not_takng_medicn_id = hypertension_data_details['fld_resn_for_not_takng_medicn_id'] 
                        fld_resn_for_not_takng_medicn_name = hypertension_data_details['fld_resn_for_not_takng_medicn_name'] 
                        fld_med_daly_other_specify = hypertension_data_details['fld_med_daly_other_specify'] 
                        fld_oth_thng_doing_cont_bp_id = hypertension_data_details['fld_oth_thng_doing_cont_bp_id'] 
                        fld_oth_thng_doing_cont_bp_name = hypertension_data_details['fld_oth_thng_doing_cont_bp_name'] 
                        fld_control_bp_other_specify = hypertension_data_details['fld_control_bp_other_specify'] 
                        fld_norm_bp_range_id = hypertension_data_details['fld_norm_bp_range_id'] 
                        fld_norm_bp_range_name = hypertension_data_details['fld_norm_bp_range_name'] 
                        fld_sign_symp_high_bp_id = hypertension_data_details['fld_sign_symp_high_bp_id'] 
                        fld_sign_symp_high_bp_name = hypertension_data_details['fld_sign_symp_high_bp_name'] 
                        fld_signsypt_other_specify = hypertension_data_details['fld_signsypt_other_specify'] 
                        fld_causes_high_bp_id = hypertension_data_details['fld_causes_high_bp_id'] 
                        fld_causes_high_bp_name = hypertension_data_details['fld_causes_high_bp_name'] 
                        fld_hbp_other_specify = hypertension_data_details['fld_hbp_other_specify'] 
                        fld_happn_bp_remns_untreatd_id = hypertension_data_details['fld_happn_bp_remns_untreatd_id'] 
                        fld_happn_bp_remns_untreatd_name = hypertension_data_details['fld_happn_bp_remns_untreatd_name'] 
                        fld_bp_other_specify = hypertension_data_details['fld_bp_other_specify'] 
                        fld_how_prevnt_hyprtnsn_id = hypertension_data_details['fld_how_prevnt_hyprtnsn_id'] 
                        fld_how_prevnt_hyprtnsn_name = hypertension_data_details['fld_how_prevnt_hyprtnsn_name'] 
                        fld_hypertnsn_other_specify = hypertension_data_details['fld_hypertnsn_other_specify'] 
                        fld_how_mng_hypertsiv_id = hypertension_data_details['fld_how_mng_hypertsiv_id'] 
                        fld_how_mng_hypertsiv_name = hypertension_data_details['fld_how_mng_hypertsiv_name'] 
                        fld_how_mng_hypertsiv_other = hypertension_data_details['fld_how_mng_hypertsiv_other'] 
                        fld_sourc_awarnss_hypertnsn_id = hypertension_data_details['fld_sourc_awarnss_hypertnsn_id'] 
                        fld_sourc_awarnss_hypertnsn_name = hypertension_data_details['fld_sourc_awarnss_hypertnsn_name'] 
                        fld_awarn_ht_other_specify = hypertension_data_details['fld_awarn_ht_other_specify']    
                        fld_data_source = hypertension_data_details['fld_data_source'] 
                        fld_loggedin_user_id  = hypertension_data_details['fld_loggedin_user_id'] 
                        fld_is_full_form_completed_form = hypertension_data_details['fld_is_full_form_completed_form'] 
                        fld_form_start_time = hypertension_data_details['fld_form_start_time'] 
                        fld_form_end_time = hypertension_data_details['fld_form_end_time'] 
                        fld_app_version = hypertension_data_details['fld_app_version']  
                        fld_date_of_interview = hypertension_data_details['fld_date_of_interview']   
                        fld_interviewer_name = hypertension_data_details['fld_interviewer_name']
                        
                        
                        values_need_to_insert = (fld_rf_id,fld_state_swid,fld_state_code,fld_state_name,fld_district_swid,fld_district_code,fld_district_name,fld_taluk_swid,fld_taluk_code,fld_taluk_name,fld_block_sw_Id,fld_block_code,fld_block_name,fld_panchyt_swid,fld_panchyt_code,fld_panchyt_name,fld_phc_swid,fld_phc_code,fld_phc_name,fld_hsc_hwc_swid,fld_hsc_hwc_code,fld_hsc_hwc_name,fld_village_swid,fld_village_code,fld_village_name,fld_hh_id,fld_member_swid,fld_member_id,fld_name,fld_bp_duaration_id,fld_bp_duaration_name,fld_num_of_years,fld_lst_scrnd_hypertnsn_id,fld_lst_scrnd_hypertnsn_name,fld_lst_time_bp_what_id,fld_lst_time_bp_what_name,fld_bp_value,fld_screning_plac_lst_tim_id,fld_screning_plac_lst_tim_name,fld_screen_place_other_specify,fld_doc_vist_bp_scrning_id,fld_doc_vist_bp_scrning_name,fld_reson_not_visit_doc_id,fld_reson_not_visit_doc_name,fld_reason_visit_other_specify,fld_refer_to_faclty_bp_issue_id,fld_refer_to_faclty_bp_issue_name,fld_if_yes_where_id,fld_if_yes_where,fld_others_specify,fld_visit_refrd_faclty_id,fld_visit_refrd_faclty_name,fld_if_no_why,fld_visit_other_faclty_id,fld_visit_other_faclty_name,fld_specify_faclty_name,fld_follw_hyprtnsn_mgm_id,fld_follw_hyprtnsn_mgm_name,fld_hyprtnsn_other_specify,fld_reson_for_prefernc,fld_tak_medcn_for_hyprtnsn_id,fld_tak_medcn_for_hyprtnsn_name,fld_resn_for_not_takng_medcn_id,fld_resn_for_not_takng_medcn_name,fld_med_not_tkn_reson_other_spicfy,fld_typ_medcn_taking_id,fld_typ_medcn_taking_name,fld_typ_med_other_spicfy,fld_what_medcn_taking,fld_what_dosage,fld_freq_takng_medcn_id,fld_freq_takng_medcn_name,fld_resn_for_not_takng_medicn_id,fld_resn_for_not_takng_medicn_name,fld_med_daly_other_specify,fld_oth_thng_doing_cont_bp_id,fld_oth_thng_doing_cont_bp_name,fld_control_bp_other_specify,fld_norm_bp_range_id,fld_norm_bp_range_name,fld_sign_symp_high_bp_id,fld_sign_symp_high_bp_name,fld_signsypt_other_specify,fld_causes_high_bp_id,fld_causes_high_bp_name,fld_hbp_other_specify,fld_happn_bp_remns_untreatd_id,fld_happn_bp_remns_untreatd_name,fld_bp_other_specify,fld_how_prevnt_hyprtnsn_id,fld_how_prevnt_hyprtnsn_name,fld_hypertnsn_other_specify,fld_how_mng_hypertsiv_id,fld_how_mng_hypertsiv_name,fld_how_mng_hypertsiv_other,fld_sourc_awarnss_hypertnsn_id,fld_sourc_awarnss_hypertnsn_name,fld_awarn_ht_other_specify,Qid,fld_data_source,fld_loggedin_user_id,fld_is_full_form_completed_form,fld_form_start_time,fld_form_end_time,fld_app_version,fld_date_of_interview,fld_interviewer_name)

                        # Prepare the query
                        query = "CALL sp_hypertension(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"#101

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

