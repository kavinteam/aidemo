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
def mobile_version_insert_ajax(request):
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
                if "tbl_mobile_version_details" in Json_request:
                    chornic_illnesses_data = Json_request["tbl_mobile_version_details"]
                    for chornic_illnesses_details in chornic_illnesses_data:
                        fld_slno = chornic_illnesses_details['fld_slno'] 
                        fld_rn = chornic_illnesses_details['fld_rn']
                        fld_rf_id = chornic_illnesses_details['fld_rf_id']
                        fldqidtmp = chornic_illnesses_details['fld_rn']
                        fldchangeid = chornic_illnesses_details['fldchangeid']
                        fldpreversionname = chornic_illnesses_details['fldpreversionname']
                        fldpreversiondownloaddate = chornic_illnesses_details['fldpreversiondownloaddate']
                        fldpreversioninstalldate = chornic_illnesses_details['fldpreversioninstalldate']
                        fldpreversionintno = chornic_illnesses_details['fldpreversionintno']
                        fldcurversionname = chornic_illnesses_details['fldcurversionname']
                        fldcurversiondownloaddate = chornic_illnesses_details['fldcurversiondownloaddate']
                        fldcurversioninstalldate = chornic_illnesses_details['fldcurversioninstalldate']
                        fldcurversionintno = chornic_illnesses_details['fldcurversionintno']
                        fldappname = chornic_illnesses_details['fldappname']
                        fld_user_id = chornic_illnesses_details['fld_user_id']
                        fldsysinserteddatetime = chornic_illnesses_details['fldsysinserteddatetime']
                        fld_is_active = chornic_illnesses_details['fld_is_active']
                        fld_sent_to_server = chornic_illnesses_details['fld_sent_to_server']
                        
                        
                        values_need_to_insert = (Qid,fld_slno,fld_rn,fld_rf_id,fldqidtmp,fldchangeid,fldpreversionname,fldpreversiondownloaddate,fldpreversioninstalldate,fldpreversionintno,fldcurversionname,fldcurversiondownloaddate,fldcurversioninstalldate,fldcurversionintno,fldappname,fld_user_id,fldsysinserteddatetime,fld_is_active,fld_sent_to_server)

                        # Prepare the query
                        query = "CALL sp_mobile_version_detials(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

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

