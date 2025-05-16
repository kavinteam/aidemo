import json,datetime
from django.db import connection
import MySQLdb as mdb
from django.conf import settings
import mysql.connector

# Function to check the requested json is valid or not
def json_validation(json_data):
    
    try:
        # cheking the json_data is having parametes or not
        if json.loads(json_data) != None:
            return True
        else:
            return False
    except:
        return False

# Function to check the api key is valid or not
def apikey_validation(json_data):
    #checking the api key from the json data
    try:
        # getting the jason data and fetching the api key from the request
        api_key_from_request = json_data['apikey']
        if api_key_from_request == 'kavin':
            return True
        else:
            return False
    except:
        return False

# Function to check the all the required parameters are available in the json data or not
def parameters_validation(json_data,parameters_of):
    try:
        if parameters_of == 'syncData':
            required_parameters = ['synceddatetime','formcode','appversion','apikey','apptypeno']
        elif parameters_of == 'getLatest': 
            required_parameters = ['user_id','role_id','requesttable','synceddatetime','formcode','appversion','apikey','apptypeno']
        elif parameters_of == 'fetch_login_information': 
            required_parameters = ['user_id','requesttable','synceddatetime','formcode','appversion','apikey','apptypeno']
        elif parameters_of == 'login':
            required_parameters = ['userid','userpassword','roll_id','android_id','synceddatetime','formcode','appversion','apikey','apptypeno']
        elif parameters_of == 'UpdateCheck':
            required_parameters = ['fldcurversionintno','synceddatetime','formcode','appversion','apikey','apptypeno']
        elif parameters_of == 'syncMobileLoginDetails':
            required_parameters = ['synceddatetime','formcode','appversion','apikey','apptypeno']
        elif parameters_of == 'SchemaDetails':
            required_parameters = ['synceddatetime','formcode','appversion','apikey','apptypeno']
        elif parameters_of == 'rollCheck':
            required_parameters = ['userid','userpassword','android_id','synceddatetime','formcode','appversion','apikey','apptypeno']
        elif parameters_of == 'rollCheck':
            required_parameters = ['synceddatetime','formcode','appversion','apikey','apptypeno']
            
        # fetching the keys from the json data
        keys_from_json = json_data.keys()
        for key in required_parameters:
                if key not in keys_from_json:
                    status = '0'
                    break
                else:
                    status = '1'
        if status == '1':
            return True
        else:
            return False
    except:
        return False

# Function to get the version code based on the api
def webservice_code(api_name):
    try:
        predefined_codes = {
            "rollCheck": "101",
            "login": "102",
            "syncMobileLoginDetails": "103",
            "getLatest": "104",
            "syncData": "105",
            "UpdateCheck": "106",
            "SchemaDetails":"107",
            "fetch_login_information":"108",
            "admin_iihmr_mobile_apk_upload":"109"
        }
        if api_name in predefined_codes:
            return predefined_codes[api_name]
        else:
            return ''
    except:
        return False

# Funtion to get the staed procedure name based on the table name
def sp_name_from_table_name(table_name):
    try:
        sp_predefined_dictionary ={
                    "tbl_mobile_login_details" : "sp_mobile_login_details",
                    "tbl_mobile_version_details" : "sp_mobile_version_detials",
                    "tbl_mobile_exception" : "sp_mobile_exception",
                    "trn_tbl_hh_details" : "sp_houseHoldDetails",
                    "trn_tbl_hh_details_meal_detail" : "sp_meal_detials",
                    "trn_tbl_hh_details_family_details" : "sp_family_detials",
                    "trn_tbl_hh_details_standard_of_living" : "sp_standard_of_leaving",
                    "trn_tbl_exclusion" : "sp_exclusion",
                    "trn_tbl_exclusioncriteria_child" : "sp_exclusioncriteria_child",
                    "trn_tbl_individual_consent" : "sp_individual_consent",
                    "trn_tbl_identify_house" : "sp_identify_house",
                    "trn_tbl_blood_sample" : "sp_blood_sample",
                    "trn_tbl_5y_to_9y" : "sp_5y_to_9y",
                    "trn_tbl_6m_to_59m" : "sp_6m_to_5y",
                    "trn_tbl_agab" : "sp_agab",
                    "trn_tbl_asha_follow_up" : "sp_asha_follow_up",
                    "trn_tbl_lacting" : "sp_lacting",
                    "trn_tbl_npnl_men" : "sp_npnl_men",
                    "trn_tbl_pregnant_women" : "sp_pregnant_women",
                    "trn_tbl_asha_supply_of_ifa" : "sp_asha_supply_of_ifa",
                    "trn_tbl_asha_follow_up_control_group" : "sp_asha_follow_up_control_group",
                    "trn_tbl_asha_follow_up_intervention_group" : "sp_asha_follow_up_intervention_group",
                    "trn_tbl_medication_disp" : "sp_medication_disp",
                    "trn_tbl_rescheduled_blood_sample" : "sp_reshedule_blood_sample",
                    "trn_tbl_asha_follow_up_intervention_group_second_part" : "sp_asha_follow_up_intervention_group_second_part",
                    "trn_tbl_endline_reporting_of_newpregnancies" : "sp_endline_reporting_of_newpregnancies",
                    "trn_tbl_endline_reporting_of_sideeffect" : "sp_endline_reporting_of_sideeffect",
                    "trn_tbl_endline_severely_anemic_fu_great_19" : "sp_endline_severely_anemic_fu_great_19",
                    "trn_tbl_endline_severely_anemic_fu_less_19" : "sp_endline_severely_anemic_fu_less_19",
                    "trn_tbl_endline_enactment_6_59m" : "sp_endline_enactment_6_59m",
                    "trn_tbl_endline_supervisoryvisit_flw" : "sp_endline_supervisoryvisit_flw",
                    "trn_tbl_endline_compliance_service_delivery" : "sp_endline_compliance_service_delivery",
                    "trn_tbl_endline_compliance_6to59month" : "sp_endline_compliance_6to59month",
                    "trn_tbl_endline_compliance_5to9year" : "sp_endline_compliance_5to9year",
                    "trn_tbl_endline_compliance_agab_npnl_preg_lact" : "sp_endline_compliance_agab_npnl_preg_lact",
                    "trn_tbl_endline_compliance_discontinuation_hh_3months" : "sp_endline_compliance_discontinuation_hh_3months",
                    "trn_tbl_endline_compliance_discontinuation_family_member_3months" : "sp_endline_compliance_discontinuation_family_member_3months",
                    "trn_tbl_endline_discontinuation_hh_6months" : "sp_endline_discontinuation_hh_6months",
                    "trn_tbl_endline_discontinuation_family_member_6months" : "sp_endline_discontinuation_family_member_6months",
                    "trn_tbl_endline_hh_details" : "sp_endline_hh_details",
                    "trn_tbl_endline_hh_family_details" : "sp_endline_hh_family_details",
                    "trn_tbl_endline_hh_details_meal_detail" : "sp_endline_hh_details_meal_detail",
                    "trn_tbl_endline_disease_reporting" : "sp_endline_disease_reporting",
                    "trn_tbl_endline_disease_reporting_child" : "sp_endline_disease_reporting_child",
                    "trn_tbl_endline_5_9_years_6m" : "sp_endline_5_9_years_6m",
                    "trn_tbl_endline_6_59_months6m" : "sp_endline_6_59_months6m",
                    "trn_tbl_endline_agab_6m" : "sp_endline_agab_6m",
                    "trn_tbl_endline_lactating_6m" : "sp_endline_lactating_6m",
                    "trn_tbl_endline_npnl_men_6m" : "sp_endline_npnl_men_6m",
                    "trn_tbl_endline_pregnant_6m" : "sp_endline_pregnant_6m",
                    "trn_tbl_endline_blood_sample" : "sp_endline_blood_sample",
                    "trn_tbl_endline_rescheduled_blood_sample" : "sp_endline_rescheduled_blood_sample",
        } 
        if table_name in sp_predefined_dictionary:
            return sp_predefined_dictionary[table_name]
        else:
            return table_name
    except:
        return False

# function to return the data in the dictionary format
def getting_data_in_dictionary_format(sql_query):
    mydb = mysql.connector.connect(
            host=settings.DATABASES['default']['HOST'], 
            user=settings.DATABASES['default']['USER'], 
            passwd=settings.DATABASES['default']['PASSWORD'], 
            database=settings.DATABASES['default']['NAME']
        )
  
    cursor = mydb.cursor(dictionary=True)
    cursor.execute(sql_query)
    result = cursor.fetchall()
        
    return result
# function to insert the Qtable data into the database
def inserQtable_data(WSFormCode,jsonData,receivedDate,syncDateTime):
    cursor = connection.cursor()
    Qid = ''
    sql = "call sp_q_mobile_detials(%s,%s,%s,%s)";
    values_need_to_insert = (WSFormCode,jsonData,receivedDate,syncDateTime)
    result = cursor.execute(sql,values_need_to_insert)
    if result != None and result != 0:
        Qid = cursor.fetchone()[0]
    else:
        Qid =''
    return Qid

# function to update the Qtable data into the database
def UpdateQTable(FormCode,valid,stringResponse,IsFullyProcessed,IsPartiallyProcessed,ReturnStatus,ApiKey,AppTypeNo,AppVersion,Qid):
    sql_to_update_q_table = """
        UPDATE trn_tbl_q_mobile_detials
        SET
            fld_form_code = %s,
            fld_is_json_valid = %s,
            fld_returned_json_text = %s,
            fld_is_fully_processed = %s,
            fld_is_partially_processed = %s,
            fld_return_status = %s,
            fld_returned_datetime = NOW(3),
            fld_process_time = TIMEDIFF(NOW(3), fld_server_rec_datetime),
            fld_api_key = %s,
            fld_app_type_no = %s,
            fld_app_version = %s
        WHERE fld_q_id = %s
    """
    values_to_update_q_table = (FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed, ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
    cursor = connection.cursor()
    result = cursor.execute(sql_to_update_q_table,values_to_update_q_table)
    if result != None and result != '':
        return True
    
    
# Function to insert the error into the database
def error_log_insert(error_json,Qid,formcode,spName,method_name,error_severity,error_status,error_code):
    error_log_upload_sql = "CALL sp_error_log_detials(%s,%s,%s,%s,%s,%s,%s,%s)"
    values_need_to_insert = (error_json,Qid,formcode,spName,method_name,error_severity,error_status,error_code)
    result = Query_excecution(error_log_upload_sql,values_need_to_insert)
    return result

# Function to get the current date time in the format of yyyy-mm-dd hh:mm:ss
def current_date_time_in_format():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Function to execute the query
def Query_excecution(query,parameters):
    cursor = connection.cursor()
    if parameters!='':
        result = cursor.execute(query,parameters)
    else:
        result = cursor.execute(query)
    return result

# Function to excecute and return the result
def Query_data_fetch(query,parameters):
    cursor = connection.cursor()
    if parameters!='':
        result = cursor.execute(query,parameters)
    else:
        result = cursor.execute(query)
    
    result = cursor.fetchall()
    return result

