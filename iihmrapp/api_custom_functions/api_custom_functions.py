import json,datetime
from django.conf import settings
from django.db import connection
import mysql.connector
from iihmrapp.models import *
import logging
import mysql.connector
import traceback
import os
#fucntion to check the requested json is valid or not 
def json_validation(json_data):
    try:
        #checking the json_data is having the parameters or not 
        if json.loads(json_data)!=None:
            return True
        else:
            return False
    except Exception as e:
        error_log_insert(str(json.dumps(json_data)),'','','','','1','1','1')
        UpdateQTable('',str(json.dumps(json_data)),'','','','','1','1','1')
    return False
    # except Exception as e:
    #     error_log_insert(str(json.dumps(json_data))),'','','','','1','1','1')
    #     UpdateQTable('',str(json.dumps(json_data))),'','','','','1','1','1')

# Function to get the current date time in the format of yyyy-mm-dd hh:mm:ss
def current_date_time_in_format():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Function to insert the error into the database
def error_log_insert(error_json, Qid, formcode, spName, method_name, error_severity, error_status, error_code):
    error_log_upload_sql = "CALL sp_error_log_detials(%s, %s, %s, %s, %s, %s, %s, %s)"
    values_need_to_insert = (error_json, Qid, formcode, spName, method_name, error_severity, error_status, error_code)
    result = Query_execution(error_log_upload_sql, values_need_to_insert)  # Corrected function name to Query_execution
    return result

# Function to check the api key is valid or not
def apikey_validation(json_data):
    #checking the api key from the json data
    try:
        # getting the jason data and fetching the api key from the request
        api_key_from_request = json_data['ApiKey']
        # Checking if the API key matches the expected value
        if api_key_from_request == 'kavin':
            return True
        else:
            return False
    except:
        return False

# Function to check the all the required parameters are available in the json data or not
def parameters_validation(json_data, parameters_of):
    try:
        # Define required parameters based on the specified type
        if parameters_of == 'web_login':
            required_parameters = ['userid', 'password', 'synceddatetime', 'FormCode', 'ApiKey', 'AppTypeNo', 'AppVersion']
        elif parameters_of =='admin_all_raw_data_download':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='gender_hypertension_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='hypertension_months_screening_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='current_hyperteion_medicine_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='hyperteion_blood_preasur_range_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='preventive_measures_hypertension_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='hypertensive_medicine_currently_used_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='hypertension_member_age_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='last_recorded_blood_pressure_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='signs_and_symptoms_of_high_blood_pressure_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='frequency_of_currently_used_medicine_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='management_measures_of_hypertension_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='place_of_last_screening_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='causes_of_high_blood_pressure_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='frequency_of_doctor_visit_for_bp_follow_up_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='reasons_for_not_taking_medicine_currently_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='non_pharmacological_methods_blood_pressure_management_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='duration_of_hypertension_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='complications_of_untreated_hypertension_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='preferred_follow_up_location_for_hypertension_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        elif parameters_of =='source_of_information_about_hypertension_report':
            required_parameters = ['ApiKey','AppTypeNo','AppVersion','FormCode','synceddatetime']
        
        # Add more elif blocks for other parameter types if needed
        # Check if all required parameters are present in json_data
        if all(param in json_data.get(parameters_of, {}) for param in required_parameters):
                return True
        # Check if all required parameters are present at the top level of json_data
        keys_from_json = json_data.keys()
        for key in required_parameters:
            if key not in keys_from_json:
                return False
        return True
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False

    
# Function to get the version code based on the api
def web_service_code(api_name):
    try:
        predefined_codes = {          
            "gender_hypertension_report": "101",
            "gender_hypertension_report": "102",
            "hypertension_months_screening_report":"104",
            "current_hyperteion_medicine_report":"105",
            "hyperteion_blood_preasur_range_report":"106",
            "preventive_measures_hypertension_report":"107",
            "hypertensive_medicine_currently_used_report":"108",
            "hypertension_member_age_report":"109",
            "last_recorded_blood_pressure_report":"110",
            "signs_and_symptoms_of_high_blood_pressure_report":"111",
            "frequency_of_currently_used_medicine_report":"112",
            "management_measures_of_hypertension_report":"113",
            "place_of_last_screening_report":"114",
            "causes_of_high_blood_pressure_report":"115",
            "frequency_of_doctor_visit_for_bp_follow_up_report":"116",
            "reasons_for_not_taking_medicine_currently_report":"117",
            "non_pharmacological_methods_blood_pressure_management_report":"118",
            "duration_of_hypertension_report":"119",
            "complications_of_untreated_hypertension_report":"120",
            "preferred_follow_up_location_for_hypertension_report":"121",
            "source_of_information_about_hypertension_report":"122",
            "admin_all_raw_data_download":"123",
        }
        if api_name != '' and api_name in predefined_codes:
            return predefined_codes[api_name]
        else:
            raise ValueError(f"Unknown React web service code for API: {api_name}")
    except:
        return False

# Funtion to get the staed procedure name based on the table name
def sp_name_from_table_name(table_name):
    try:
        sp_predefined_dictionary ={
        "tbl_mobile_login_details" : "sp_mobile_login_details",
        "tbl_mobile_version_details" : "sp_mobile_version_detials",
        "tbl_mobile_exception" : "sp_mobile_exception",
        } 
        if table_name in sp_predefined_dictionary:
            return sp_predefined_dictionary[table_name]
        else:
            return table_name
    except:
        return False

# Function to return the data in the dictionary format
def getting_data_in_dictionary_format(sql_query):
    try:
        # Establish a database connection
        mydb = mysql.connector.connect(
            host=settings.DATABASES['default']['HOST'],
            user=settings.DATABASES['default']['USER'],
            passwd=settings.DATABASES['default']['PASSWORD'],
            database=settings.DATABASES['default']['NAME']
        )
        # Fetching the data with column names from the database
        with mydb.cursor(dictionary=True) as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchall()
            return result
        # # Convert result to a JSON string
        # result_json = json.dumps(result)
        # return result_json
    except mysql.connector.Error as err:
        # Handle any database errors here
        print(f"Error: {err}")
        return None
    finally:
        # Close the database connection in the 'finally' block to ensure it's always closed
        if mydb.is_connected():
            mydb.close()

# function to insert the Qtable data into the database
def inserQtable_data(WSFormCode,jsonData,receivedDate,syncDateTime):
    cursor = connection.cursor()
    Qid = ''
    sql = "call sp_q_mobile_detials(%s,%s,%s,%s)"
    values_need_to_insert = (WSFormCode,jsonData,receivedDate,syncDateTime)
    result = cursor.execute(sql,values_need_to_insert)
    if result != None and result != 0:
        Qid = cursor.fetchone()[0]
    else:
        Qid =''
    return Qid

def save_uploaded_image(image_file, folder):
    # Create the target directory if it doesn't exist
    target_directory = os.path.join(settings.MEDIA_ROOT, folder)
    if not os.path.exists(target_directory):
        os.makedirs(target_directory)
    # Get the file name and extension
    file_name = image_file.name
    file_extension = file_name.split('.')[-1]
    # Generate a unique file name
    unique_file_name = file_name  # Replace this with your own logic for generating unique names if needed
    # Build the file path
    file_path = os.path.join(target_directory, unique_file_name)
    # Save the image file
    with open(file_path, 'wb') as file:
        for chunk in image_file.chunks():
            file.write(chunk)
    # Return the file path relative to the media root
    return os.path.join(folder, unique_file_name)

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

# Function to execute the query
def Query_execution(query,parameters):
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


def new_getting_data_in_dictionary_format(sql_query, params):
    try:
        mydb = mysql.connector.connect(
            host=settings.DATABASES['default']['HOST'],
            user=settings.DATABASES['default']['USER'],
            passwd=settings.DATABASES['default']['PASSWORD'],
            database=settings.DATABASES['default']['NAME']
        )
        with mydb.cursor(dictionary=True) as cursor:
            cursor.execute(sql_query, params)
            result = cursor.fetchall()
        return result
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
    finally:
        if mydb.is_connected():
            mydb.close()
