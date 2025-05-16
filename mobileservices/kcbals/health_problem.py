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
def health_problem_insert_ajax(request):
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
                if "trn_tbl_health_problem" in Json_request:
                    health_problem_data = Json_request["trn_tbl_health_problem"]
                    for health_problem_data_details in health_problem_data:
                        if health_problem_data_details['fld_rf_id']=='-99':
                            fld_rf_id=''
                        else:
                            fld_rf_id = health_problem_data_details['fld_rf_id'] 
                        fld_state_swid = health_problem_data_details['fld_state_swid'] 
                        fld_state_code = health_problem_data_details['fld_state_code'] 
                        fld_state_name = health_problem_data_details['fld_state_name'] 
                        fld_district_swid = health_problem_data_details['fld_district_swid'] 
                        fld_district_code = health_problem_data_details['fld_district_code'] 
                        fld_district_name = health_problem_data_details['fld_district_name'] 
                        fld_taluk_swid = health_problem_data_details['fld_taluk_swid'] 
                        fld_taluk_code = health_problem_data_details['fld_taluk_code'] 
                        fld_taluk_name = health_problem_data_details['fld_taluk_name'] 
                        fld_block_sw_Id = health_problem_data_details['fld_block_sw_Id']
                        fld_block_code = health_problem_data_details['fld_block_code']
                        fld_block_name = health_problem_data_details['fld_block_name']
                        fld_panchyt_swid = health_problem_data_details['fld_panchyt_swid'] 
                        fld_panchyt_code = health_problem_data_details['fld_panchyt_code'] 
                        fld_panchyt_name = health_problem_data_details['fld_panchyt_name'] 
                        fld_phc_swid = health_problem_data_details['fld_phc_swid'] 
                        fld_phc_code = health_problem_data_details['fld_phc_code'] 
                        fld_phc_name = health_problem_data_details['fld_phc_name'] 
                        fld_hsc_hwc_swid = health_problem_data_details['fld_hsc_hwc_swid'] 
                        fld_hsc_hwc_code = health_problem_data_details['fld_hsc_hwc_code'] 
                        fld_hsc_hwc_name = health_problem_data_details['fld_hsc_hwc_name'] 
                        fld_village_swid = health_problem_data_details['fld_village_swid'] 
                        fld_village_code = health_problem_data_details['fld_village_code'] 
                        fld_village_name = health_problem_data_details['fld_village_name'] 
                        fld_hh_swid = health_problem_data_details['fld_hh_swid'] 
                        fld_hh_id = health_problem_data_details['fld_hh_id'] 
                        fld_member_swid = health_problem_data_details['fld_member_swid'] 
                        fld_member_id = health_problem_data_details['fld_member_id'] 
                        fld_name = health_problem_data_details['fld_name'] 
                        fld_oth_hlth_prob_nothng_id = health_problem_data_details['fld_oth_hlth_prob_nothng_id'] 
                        fld_oth_hlth_prob_nothng_name = health_problem_data_details['fld_oth_hlth_prob_nothng_name'] 
                        fld_high_bp_id = health_problem_data_details['fld_high_bp_id'] 
                        fld_high_bp_name = health_problem_data_details['fld_high_bp_name'] 
                        fld_yes_taking_bp_trtmnt_id = health_problem_data_details['fld_yes_taking_bp_trtmnt_id'] 
                        fld_yes_taking_bp_trtmnt_name = health_problem_data_details['fld_yes_taking_bp_trtmnt_name'] 
                        fld_place_of_bp_trtmnt_id = health_problem_data_details['fld_place_of_bp_trtmnt_id'] 
                        fld_place_of_bp_trtmnt_name = health_problem_data_details['fld_place_of_bp_trtmnt_name'] 
                        fld_hbp_pt_bp_other_specify = health_problem_data_details['fld_hbp_pt_bp_other_specify'] 
                        fld_heart_disease_id = health_problem_data_details['fld_heart_disease_id'] 
                        fld_heart_disease_name = health_problem_data_details['fld_heart_disease_name'] 
                        fld_yes_taking_hd_trtmnt_id = health_problem_data_details['fld_yes_taking_hd_trtmnt_id'] 
                        fld_yes_taking_hd_trtmnt_name = health_problem_data_details['fld_yes_taking_hd_trtmnt_name'] 
                        fld_place_of_trtmnt_hd_id = health_problem_data_details['fld_place_of_trtmnt_hd_id'] 
                        fld_place_of_trtmnt_hd_name = health_problem_data_details['fld_place_of_trtmnt_hd_name'] 
                        fld_hd_pt_hd_other_specify = health_problem_data_details['fld_hd_pt_hd_other_specify'] 
                        fld_stroke_id = health_problem_data_details['fld_stroke_id'] 
                        fld_stroke_name = health_problem_data_details['fld_stroke_name'] 
                        fld_yes_taking_stok_trtmnt_id = health_problem_data_details['fld_yes_taking_stok_trtmnt_id'] 
                        fld_yes_taking_stok_trtmnt_name = health_problem_data_details['fld_yes_taking_stok_trtmnt_name'] 
                        fld_place_of_trtmnt_stok_id = health_problem_data_details['fld_place_of_trtmnt_stok_id'] 
                        fld_place_of_trtmnt_stok_name = health_problem_data_details['fld_place_of_trtmnt_stok_name'] 
                        fld_strok_pt_stok_other_specify = health_problem_data_details['fld_strok_pt_stok_other_specify'] 
                        fld_asthma_id = health_problem_data_details['fld_asthma_id'] 
                        fld_asthma_name = health_problem_data_details['fld_asthma_name'] 
                        fld_yes_taking_astma_trtmnt_id = health_problem_data_details['fld_yes_taking_astma_trtmnt_id'] 
                        fld_yes_taking_astma_trtmnt_name = health_problem_data_details['fld_yes_taking_astma_trtmnt_name'] 
                        fld_place_of_trtmnt_astma_id = health_problem_data_details['fld_place_of_trtmnt_astma_id'] 
                        fld_place_of_trtmnt_astma_name = health_problem_data_details['fld_place_of_trtmnt_astma_name'] 
                        fld_asthma_pt_astma_other_specify = health_problem_data_details['fld_asthma_pt_astma_other_specify'] 
                        fld_pcos_id = health_problem_data_details['fld_pcos_id'] 
                        fld_pcos_name = health_problem_data_details['fld_pcos_name'] 
                        fld_yes_taking_pcos_trtmnt_id = health_problem_data_details['fld_yes_taking_pcos_trtmnt_id'] 
                        fld_yes_taking_pcos_trtmnt_name = health_problem_data_details['fld_yes_taking_pcos_trtmnt_name'] 
                        fld_place_of_trtmnt_pcos_id = health_problem_data_details['fld_place_of_trtmnt_pcos_id'] 
                        fld_place_of_trtmnt_pcos_name = health_problem_data_details['fld_place_of_trtmnt_pcos_name'] 
                        fld_pcos_pt_pcos_other_specify = health_problem_data_details['fld_pcos_pt_pcos_other_specify'] 
                        fld_thyroid_id = health_problem_data_details['fld_thyroid_id'] 
                        fld_thyroid_name = health_problem_data_details['fld_thyroid_name'] 
                        fld_yes_taking_thy_trtmnt_id = health_problem_data_details['fld_yes_taking_thy_trtmnt_id'] 
                        fld_yes_taking_thy_trtmnt_name = health_problem_data_details['fld_yes_taking_thy_trtmnt_name'] 
                        fld_place_of_trtmnt_thy_id = health_problem_data_details['fld_place_of_trtmnt_thy_id'] 
                        fld_place_of_trtmnt_thy_name = health_problem_data_details['fld_place_of_trtmnt_thy_name'] 
                        fld_thyroid_pt_thy_other_specify = health_problem_data_details['fld_thyroid_pt_thy_other_specify'] 
                        fld_teeth_or_gum_id = health_problem_data_details['fld_teeth_or_gum_id'] 
                        fld_teeth_or_gum_name = health_problem_data_details['fld_teeth_or_gum_name'] 
                        fld_yes_taking_tgum_trtmnt_id = health_problem_data_details['fld_yes_taking_tgum_trtmnt_id'] 
                        fld_yes_taking_tgum_trtmnt_name = health_problem_data_details['fld_yes_taking_tgum_trtmnt_name'] 
                        fld_place_of_trtmnt_tgum_id = health_problem_data_details['fld_place_of_trtmnt_tgum_id'] 
                        fld_place_of_trtmnt_tgum_name = health_problem_data_details['fld_place_of_trtmnt_tgum_name'] 
                        fld_teethgum_pt_tgum_other_specify = health_problem_data_details['fld_teethgum_pt_tgum_other_specify'] 
                        fld_retinopathy_id = health_problem_data_details['fld_retinopathy_id'] 
                        fld_retinopathy_name = health_problem_data_details['fld_retinopathy_name'] 
                        fld_yes_taking_retpty_trtmnt_id = health_problem_data_details['fld_yes_taking_retpty_trtmnt_id'] 
                        fld_yes_taking_retpty_trtmnt_name = health_problem_data_details['fld_yes_taking_retpty_trtmnt_name'] 
                        fld_place_of_trtmnt_retpty_id = health_problem_data_details['fld_place_of_trtmnt_retpty_id'] 
                        fld_place_of_trtmnt_retpty_name = health_problem_data_details['fld_place_of_trtmnt_retpty_name'] 
                        fld_retino_pt_retpty_other_specify = health_problem_data_details['fld_retino_pt_retpty_other_specify'] 
                        fld_sexul_dysfunction_id = health_problem_data_details['fld_sexul_dysfunction_id'] 
                        fld_sexul_dysfunction_name = health_problem_data_details['fld_sexul_dysfunction_name'] 
                        fld_yes_taking_sexdys_trtmnt_id = health_problem_data_details['fld_yes_taking_sexdys_trtmnt_id'] 
                        fld_yes_taking_sexdys_trtmnt_name = health_problem_data_details['fld_yes_taking_sexdys_trtmnt_name'] 
                        fld_place_of_trtmnt_sexdys_id = health_problem_data_details['fld_place_of_trtmnt_sexdys_id'] 
                        fld_place_of_trtmnt_sexdys_name = health_problem_data_details['fld_place_of_trtmnt_sexdys_name'] 
                        fld_sd_pt_sexdys_other_specify = health_problem_data_details['fld_sd_pt_sexdys_other_specify'] 
                        fld_other_health_prob_id = health_problem_data_details['fld_other_health_prob_id'] 
                        fld_other_health_prob_name = health_problem_data_details['fld_other_health_prob_name'] 
                        fld_other_problem = health_problem_data_details['fld_other_problem'] 
                        fld_yes_taking_othersp_trtmnt_id = health_problem_data_details['fld_yes_taking_othersp_trtmnt_id'] 
                        fld_yes_taking_othersp_trtmnt_name = health_problem_data_details['fld_yes_taking_othersp_trtmnt_name'] 
                        fld_place_of_trtmnt_othersp_id = health_problem_data_details['fld_place_of_trtmnt_othersp_id'] 
                        fld_place_of_trtmnt_othersp_name = health_problem_data_details['fld_place_of_trtmnt_othersp_name'] 
                        fld_other_pt_othersp_other_specify = health_problem_data_details['fld_other_pt_othersp_other_specify'] 
                        fld_other_pt_other_specify = health_problem_data_details['fld_other_pt_other_specify'] 
                        fld_any_mod_phys_actvt_id = health_problem_data_details['fld_any_mod_phys_actvt_id'] 
                        fld_any_mod_phys_actvt_name = health_problem_data_details['fld_any_mod_phys_actvt_name'] 
                        fld_mod_phys_actvt_fqcy_hrs_id = health_problem_data_details['fld_mod_phys_actvt_fqcy_hrs_id'] 
                        fld_mod_phys_actvt_fqcy_hrs_name = health_problem_data_details['fld_mod_phys_actvt_fqcy_hrs_name'] 
                        fld_total_mod_hrs_spnd_week = health_problem_data_details['fld_total_mod_hrs_spnd_week'] 
                        fld_any_vigo_phys_actvt_id = health_problem_data_details['fld_any_vigo_phys_actvt_id'] 
                        fld_any_vigo_phys_actvt_name = health_problem_data_details['fld_any_vigo_phys_actvt_name'] 
                        fld_vigo_phys_actvt_fqcy_hrs_id = health_problem_data_details['fld_vigo_phys_actvt_fqcy_hrs_id'] 
                        fld_vigo_phys_actvt_fqcy_hrs_name = health_problem_data_details['fld_vigo_phys_actvt_fqcy_hrs_name'] 
                        fld_total_vigo_hrs_spnd_week = health_problem_data_details['fld_total_vigo_hrs_spnd_week'] 
                        fld_attnd_yoga_sesion_id = health_problem_data_details['fld_attnd_yoga_sesion_id'] 
                        fld_attnd_yoga_sesion_name = health_problem_data_details['fld_attnd_yoga_sesion_name'] 
                        fld_atnd_where = health_problem_data_details['fld_atnd_where'] 
                        fld_at_present_do_you_have_the_following_habits_id = health_problem_data_details['fld_at_present_do_you_have_the_following_habits_id'] 
                        fld_at_present_do_you_have_the_following_habits_name = health_problem_data_details['fld_at_present_do_you_have_the_following_habits_name'] 
                        fld_habit_drink_alcho_id = health_problem_data_details['fld_habit_drink_alcho_id'] 
                        fld_habit_drink_alcho_name = health_problem_data_details['fld_habit_drink_alcho_name'] 
                        fld_drink_alcho_fqcy_hrs_id = health_problem_data_details['fld_drink_alcho_fqcy_hrs_id'] 
                        fld_drink_alcho_fqcy_hrs_name = health_problem_data_details['fld_drink_alcho_fqcy_hrs_name'] 
                        fld_habit_smok_tobco_id = health_problem_data_details['fld_habit_smok_tobco_id'] 
                        fld_habit_smok_tobco_name = health_problem_data_details['fld_habit_smok_tobco_name'] 
                        fld_smok_tobco_fqcy_hrs_id = health_problem_data_details['fld_smok_tobco_fqcy_hrs_id'] 
                        fld_smok_tobco_fqcy_hrs_name = health_problem_data_details['fld_smok_tobco_fqcy_hrs_name'] 
                        fld_habit_smls_tobco_id = health_problem_data_details['fld_habit_smls_tobco_id'] 
                        fld_habit_smls_tobco_name = health_problem_data_details['fld_habit_smls_tobco_name'] 
                        fld_smls_tobco_fqcy_hrs_id = health_problem_data_details['fld_smls_tobco_fqcy_hrs_id'] 
                        fld_smls_tobco_fqcy_hrs_name = health_problem_data_details['fld_smls_tobco_fqcy_hrs_name'] 
                        fld_habit_of_consuming_the_following_in_the_past_id = health_problem_data_details['fld_habit_of_consuming_the_following_in_the_past_id'] 
                        fld_habit_of_consuming_the_following_in_the_past_name = health_problem_data_details['fld_habit_of_consuming_the_following_in_the_past_name'] 
                        fld_pst_habit_drink_alcho_id = health_problem_data_details['fld_pst_habit_drink_alcho_id'] 
                        fld_pst_habit_drink_alcho_name = health_problem_data_details['fld_pst_habit_drink_alcho_name'] 
                        fld_pst_drink_alcho_fqcy_hrs_id = health_problem_data_details['fld_pst_drink_alcho_fqcy_hrs_id'] 
                        fld_pst_drink_alcho_fqcy_hrs_name = health_problem_data_details['fld_pst_drink_alcho_fqcy_hrs_name'] 
                        fld_pst_habit_smok_tobco_id = health_problem_data_details['fld_pst_habit_smok_tobco_id'] 
                        fld_pst_habit_smok_tobco_name = health_problem_data_details['fld_pst_habit_smok_tobco_name'] 
                        fld_pst_smok_tobco_fqcy_hrs_id = health_problem_data_details['fld_pst_smok_tobco_fqcy_hrs_id'] 
                        fld_pst_smok_tobco_fqcy_hrs_name = health_problem_data_details['fld_pst_smok_tobco_fqcy_hrs_name'] 
                        fld_pst_habit_smls_tobco_id  = health_problem_data_details['fld_pst_habit_smls_tobco_id'] 
                        fld_pst_habit_smls_tobco_name  = health_problem_data_details['fld_pst_habit_smls_tobco_name'] 
                        fld_pst_smls_tobco_fqcy_hrs_id  = health_problem_data_details['fld_pst_smls_tobco_fqcy_hrs_id'] 
                        fld_pst_smls_tobco_fqcy_hrs_name  = health_problem_data_details['fld_pst_smls_tobco_fqcy_hrs_name'] 
                        fld_will_partcpet_prgm_id = health_problem_data_details['fld_will_partcpet_prgm_id'] 
                        fld_will_partcpet_prgm_name = health_problem_data_details['fld_will_partcpet_prgm_name'] 
                        fld_own_mobile_id = health_problem_data_details['fld_own_mobile_id'] 
                        fld_own_mobile_name = health_problem_data_details['fld_own_mobile_name'] 
                        fld_spicfy_mob_num = health_problem_data_details['fld_spicfy_mob_num'] 
                        fld_type_mob_own_id = health_problem_data_details['fld_type_mob_own_id'] 
                        fld_type_mob_own_name = health_problem_data_details['fld_type_mob_own_name'] 
                        fld_fm_own_mob_id = health_problem_data_details['fld_fm_own_mob_id'] 
                        fld_fm_own_mob_name = health_problem_data_details['fld_fm_own_mob_name'] 
                        fld_specfy_mob_num_rcv_call = health_problem_data_details['fld_specfy_mob_num_rcv_call'] 
                        fld_fam_mob_typ_id = health_problem_data_details['fld_fam_mob_typ_id'] 
                        fld_fam_mob_typ_name = health_problem_data_details['fld_fam_mob_typ_name'] 
                        fld_pref_time_call_msg = health_problem_data_details['fld_pref_time_call_msg'] 
                        fld_data_source = health_problem_data_details['fld_data_source'] 
                        fld_loggedin_user_id  = health_problem_data_details['fld_loggedin_user_id'] 
                        fld_is_full_form_completed_form = health_problem_data_details['fld_is_full_form_completed_form'] 
                        fld_form_start_time = health_problem_data_details['fld_form_start_time'] 
                        fld_form_end_time = health_problem_data_details['fld_form_end_time'] 
                        fld_app_version = health_problem_data_details['fld_app_version']  
                        fld_date_of_interview = health_problem_data_details['fld_date_of_interview']   
                        fld_interviewer_name = health_problem_data_details['fld_interviewer_name']
                        fld_qid = Qid
                        
                        
                        values_need_to_insert = (fld_rf_id,fld_state_swid,fld_state_code,fld_state_name,fld_district_swid,fld_district_code,fld_district_name,fld_taluk_swid,fld_taluk_code,fld_taluk_name,fld_block_sw_Id,fld_block_code,fld_block_name,fld_panchyt_swid,fld_panchyt_code,fld_panchyt_name,fld_phc_swid,fld_phc_code,fld_phc_name,fld_hsc_hwc_swid,fld_hsc_hwc_code,fld_hsc_hwc_name,fld_village_swid,fld_village_code,fld_village_name,fld_hh_swid,fld_hh_id,fld_member_swid,fld_member_id,fld_name,fld_oth_hlth_prob_nothng_id,fld_oth_hlth_prob_nothng_name,fld_high_bp_id,fld_high_bp_name,fld_yes_taking_bp_trtmnt_id,fld_yes_taking_bp_trtmnt_name,fld_place_of_bp_trtmnt_id,fld_place_of_bp_trtmnt_name,fld_hbp_pt_bp_other_specify,fld_heart_disease_id,fld_heart_disease_name,fld_yes_taking_hd_trtmnt_id,fld_yes_taking_hd_trtmnt_name,fld_place_of_trtmnt_hd_id,fld_place_of_trtmnt_hd_name,fld_hd_pt_hd_other_specify,fld_stroke_id,fld_stroke_name,fld_yes_taking_stok_trtmnt_id,fld_yes_taking_stok_trtmnt_name,fld_place_of_trtmnt_stok_id,fld_place_of_trtmnt_stok_name,fld_strok_pt_stok_other_specify,fld_asthma_id,fld_asthma_name,fld_yes_taking_astma_trtmnt_id,fld_yes_taking_astma_trtmnt_name,fld_place_of_trtmnt_astma_id,fld_place_of_trtmnt_astma_name,fld_asthma_pt_astma_other_specify,fld_pcos_id,fld_pcos_name,fld_yes_taking_pcos_trtmnt_id,fld_yes_taking_pcos_trtmnt_name,fld_place_of_trtmnt_pcos_id,fld_place_of_trtmnt_pcos_name,fld_pcos_pt_pcos_other_specify,fld_thyroid_id,fld_thyroid_name,fld_yes_taking_thy_trtmnt_id,fld_yes_taking_thy_trtmnt_name,fld_place_of_trtmnt_thy_id,fld_place_of_trtmnt_thy_name,fld_thyroid_pt_thy_other_specify,fld_teeth_or_gum_id,fld_teeth_or_gum_name,fld_yes_taking_tgum_trtmnt_id,fld_yes_taking_tgum_trtmnt_name,fld_place_of_trtmnt_tgum_id,fld_place_of_trtmnt_tgum_name,fld_teethgum_pt_tgum_other_specify,fld_retinopathy_id,fld_retinopathy_name,fld_yes_taking_retpty_trtmnt_id,fld_yes_taking_retpty_trtmnt_name,fld_place_of_trtmnt_retpty_id,fld_place_of_trtmnt_retpty_name,fld_retino_pt_retpty_other_specify,fld_sexul_dysfunction_id,fld_sexul_dysfunction_name,fld_yes_taking_sexdys_trtmnt_id,fld_yes_taking_sexdys_trtmnt_name,fld_place_of_trtmnt_sexdys_id,fld_place_of_trtmnt_sexdys_name,fld_sd_pt_sexdys_other_specify,fld_other_health_prob_id,fld_other_health_prob_name,fld_other_problem,fld_yes_taking_othersp_trtmnt_id,fld_yes_taking_othersp_trtmnt_name,fld_place_of_trtmnt_othersp_id,fld_place_of_trtmnt_othersp_name,fld_other_pt_othersp_other_specify,fld_other_pt_other_specify,fld_any_mod_phys_actvt_id,fld_any_mod_phys_actvt_name,fld_mod_phys_actvt_fqcy_hrs_id,fld_mod_phys_actvt_fqcy_hrs_name,fld_total_mod_hrs_spnd_week,fld_any_vigo_phys_actvt_id,fld_any_vigo_phys_actvt_name,fld_vigo_phys_actvt_fqcy_hrs_id,fld_vigo_phys_actvt_fqcy_hrs_name,fld_total_vigo_hrs_spnd_week,fld_attnd_yoga_sesion_id,fld_attnd_yoga_sesion_name,fld_atnd_where,fld_at_present_do_you_have_the_following_habits_id,fld_at_present_do_you_have_the_following_habits_name,fld_habit_drink_alcho_id,fld_habit_drink_alcho_name,fld_drink_alcho_fqcy_hrs_id,fld_drink_alcho_fqcy_hrs_name,fld_habit_smok_tobco_id,fld_habit_smok_tobco_name,fld_smok_tobco_fqcy_hrs_id,fld_smok_tobco_fqcy_hrs_name,fld_habit_smls_tobco_id,fld_habit_smls_tobco_name,fld_smls_tobco_fqcy_hrs_id,fld_smls_tobco_fqcy_hrs_name,fld_habit_of_consuming_the_following_in_the_past_id,fld_habit_of_consuming_the_following_in_the_past_name,fld_pst_habit_drink_alcho_id,fld_pst_habit_drink_alcho_name,fld_pst_drink_alcho_fqcy_hrs_id,fld_pst_drink_alcho_fqcy_hrs_name,fld_pst_habit_smok_tobco_id,fld_pst_habit_smok_tobco_name,fld_pst_smok_tobco_fqcy_hrs_id,fld_pst_smok_tobco_fqcy_hrs_name,fld_pst_habit_smls_tobco_id,fld_pst_habit_smls_tobco_name,fld_pst_smls_tobco_fqcy_hrs_id,fld_pst_smls_tobco_fqcy_hrs_name,fld_will_partcpet_prgm_id,fld_will_partcpet_prgm_name,fld_own_mobile_id,fld_own_mobile_name,fld_spicfy_mob_num,fld_type_mob_own_id,fld_type_mob_own_name,fld_fm_own_mob_id,fld_fm_own_mob_name,fld_specfy_mob_num_rcv_call,fld_fam_mob_typ_id,fld_fam_mob_typ_name,fld_pref_time_call_msg,fld_qid,fld_data_source,fld_loggedin_user_id,fld_is_full_form_completed_form,fld_form_start_time,fld_form_end_time,fld_app_version,fld_date_of_interview,fld_interviewer_name)

                        # Prepare the query
                        query = "CALL sp_health_problem(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"#163

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

