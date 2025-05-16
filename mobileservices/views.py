from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
import json
from django.db import connection
from mobileservices.api_custome_functions import api_custome_functions
# from django.conf import settings
from django.http import JsonResponse
import datetime
from mobileservices.kcbals import consent_form,respondent_detail,house_hold_details,family_details,standard_of_living,hospitalization,chornic_illnesses,hypertension,diabetes,health_problem,diet_and_nutrition,mobile_version_details
from iihmrapp.models import *
from django.conf import settings
import os
# Create your views here.
#def index(request):
#    return render(request,'index.html')

@csrf_exempt
def login(request):
    if request.method == 'POST':
        Qid = ''
        try:
           
            json_body = request.body
            request_json_validation = api_custome_functions.json_validation(json_body) # Validating the Json is
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custome_functions.apikey_validation(Json_request) # Validation the apikey
                parameters_validation = api_custome_functions.parameters_validation(Json_request, 'login') # validation the parameters
                webservice_code = api_custome_functions.webservice_code('login') # getting the webservice code
            else: # if the json is not valid
                apikey_validation = ''
                parameters_validation =''
                webservice_code = api_custome_functions.webservice_code('login') # getting the webservice code
            if request_json_validation == True and apikey_validation == True and parameters_validation == True and webservice_code !='': # if the json is valid and parameters and valid and webservice code is not empty
                Json_request = json.loads(request.body)
                user_id = Json_request['userid']
                password = Json_request['userpassword']
                # role_id = Json_request['role_id']
                android_id = Json_request['android_id']
                synceddatetime = Json_request['synceddatetime']
                formcode = Json_request['formcode']
                appversion = Json_request['appversion']
                apikey = Json_request['apikey']
                apptypeno = Json_request['apptypeno']
                validjson='1'
                # convertin the json data to string so we can insert into the database without error
                jsonData_database = str(json.dumps(Json_request))
                login_query = "call sp_login(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                values_need_to_insert = (user_id,password,android_id,webservice_code,jsonData_database,api_custome_functions.current_date_time_in_format(),synceddatetime,formcode,validjson,apikey,apptypeno,appversion)
                cursor = connection.cursor()
                cursor.execute(login_query, values_need_to_insert)
                cursor.fetchall()
                cursor.nextset()
                result = cursor.fetchall()
                if result  == ():
                    Json_response = {
                        "status": "2",
                        "responsemessage": "Invalid User id or Password"
                        }
                else:
                    Qid = result[0][0]
                    Json_response = result[0][1]
                    # form_details = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * FROM master_tbl_user WHERE fld_user_id='{user_id}' and fld_password='{password}' and fld_is_active=1")
                    # Json_response = json.loads(Json_response)
                    #  # Add form_details to the JSON object
                    # Json_response['form_details'] = form_details
                    # # Convert the combined JSON object back to a JSON string
                    # json.dumps(Json_response)
                    # string to json format
                    Json_response = json.loads(Json_response)
            else:
                if request_json_validation == True: # if the json is valid
                    Json_request = json.dumps(json.loads(request.body), default=str) # converting the json data to string
                else:
                    Json_request = request.body # getting the json data
                Qid = api_custome_functions.inserQtable_data(webservice_code,str(Json_request),api_custome_functions.current_date_time_in_format(),'')
                Json_response ={
                "error_level": "2",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custome_functions.current_date_time_in_format()
                }
                api_custome_functions.error_log_insert(str(Json_response),Qid,'','','login','1','1','2')
                api_custome_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
            return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            Qid = api_custome_functions.inserQtable_data(webservice_code,str(Json_request),api_custome_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "1",
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custome_functions.current_date_time_in_format()
                }
            api_custome_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','login','1','1','1')
            api_custome_functions.UpdateQTable('',str(json.dumps(error_json)),'0','0','1','2','','','',Qid)
        return JsonResponse(error_json)
    
# Function to Check the syncMobileLoginDetails is valid or not
@csrf_exempt
def syncMobileLoginDetails(request):
    if request.method == 'POST':
        Qid = ''
        formcode = ''  # Initialize FormCode variable outside of the try block
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
            json_body = request.body
            request_json_validation = api_custome_functions.json_validation(json_body) # Validating the Json is 
            
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custome_functions.apikey_validation(Json_request) # Validation the apikey
                parameters_validation = api_custome_functions.parameters_validation(Json_request, 'syncMobileLoginDetails') # validation the parameters
                webservice_code = api_custome_functions.webservice_code('syncMobileLoginDetails') # getting the webservice code
            else: # if the json is not valid
                apikey_validation = ''
                parameters_validation =''
                webservice_code = api_custome_functions.webservice_code('syncMobileLoginDetails') # getting the webservice code
                
            if request_json_validation == True and apikey_validation == True and parameters_validation == True and webservice_code !='': # if the json is valid and parameters and valid and webservice code is not empty
                
                Json_request = json.loads(request.body)
                synceddatetime = Json_request['synceddatetime']
                FormCode = Json_request['formcode']
                appversion = Json_request['appversion']
                # imeino = Json_request['imeino']
                # simno = Json_request['simno']
                apikey = Json_request['apikey']
                apptypeno = Json_request['apptypeno']
                if 'incompletedata' in Json_request: 
                    incompletedata = Json_request['incompletedata']                    
                jsonData_database = str(json.dumps(Json_request))                
                Qid = api_custome_functions.inserQtable_data(webservice_code,jsonData_database,api_custome_functions.current_date_time_in_format(),synceddatetime)
                mobile_login_and_version_query = "call sp_mobile_login_detials_and_version_detials_insertion(%s,%s)";
                values_for_login_and_version = (jsonData_database,Qid)
                result = api_custome_functions.Query_data_fetch(mobile_login_and_version_query,values_for_login_and_version)
                if result != () and result != None:
                    IsFullyProcessed = 1
                    valid = 1
                    IsPartiallyProcessed = 0
                    ReturnStatus=result[0][0]
                    responseStatus = result[0][1]
                    Json_response = {
                        "status" : ReturnStatus,
                        "responsemessage" : responseStatus,
                        "serverdatetime" : api_custome_functions.current_date_time_in_format(),
                    }
                else:
                    valid = 0
                    IsFullyProcessed = 0
                    IsPartiallyProcessed = 1
                    ReturnStatus="0"
                    Json_response = {
                        "status": "2",
                        "responsemessage": "something went wrong"
                        }
                stringResponse = str(json.dumps(Json_response))
                api_custome_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                return JsonResponse(Json_response)
            else:
                if request_json_validation == True: # if the json is valid
                    Json_request = json.dumps(json.loads(request.body), default=str) # converting the json data to string
                else:
                    Json_request = request.body # getting the json data
                Qid = api_custome_functions.inserQtable_data(webservice_code,str(Json_request),api_custome_functions.current_date_time_in_format(),'')                
                Json_response ={
                "error_level": "2",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custome_functions.current_date_time_in_format()
                }
                api_custome_functions.error_log_insert(str(Json_response),Qid,'','','syncMobileLoginDetails','1','1','2')
                api_custome_functions.UpdateQTable(FormCode, valid, str(Json_response), IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, apikey, apptypeno, appversion, Qid)
            return JsonResponse(Json_response)        
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            
            Qid = api_custome_functions.inserQtable_data(webservice_code,str(Json_request),api_custome_functions.current_date_time_in_format(),'')
            
            error_json ={
                "error_level": "1",
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custome_functions.current_date_time_in_format()
                }
            api_custome_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','syncMobileLoginDetails','1','1','1')
            api_custome_functions.UpdateQTable(FormCode, valid, str(Json_response), IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, apikey, apptypeno, appversion, Qid)
        return JsonResponse(error_json)
    
@csrf_exempt
def getLatest(request):
     if request.method == 'POST':
        ouputjson = {}
        Qid = ''
        formcode = ''  # Initialize FormCode variable outside of the try block
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
            json_body = request.body
            request_json_validation = api_custome_functions.json_validation(json_body) # Validating the Json is valid or not
            if request_json_validation == True: # if the json is valid
                Json_request = json.loads(request.body)
                apikey_validation = api_custome_functions.apikey_validation(Json_request) # Validation the
                parameters_validation = api_custome_functions.parameters_validation(Json_request, 'getLatest') # validation th
                webservice_code = api_custome_functions.webservice_code('getLatest') # getting the webservice code
            else: # if the json is not valid
                apikey_validation = ''
                parameters_validation =''
                webservice_code = api_custome_functions.webservice_code('getLatest') # getting the webservice code
                # if the json is valid and parameters and valid and webservice code is not empty
            if request_json_validation == True and apikey_validation == True and parameters_validation == True and webservice_code !='':
                Json_request = json.loads(request.body)
                datetime_obj_to_str_array = ['fld_sys_inserted_datetime','fld_form_start_time','fld_form_end_time']
                # get the json data
                Json_request = json.loads(request.body)
                # user_id = Json_request['user_id']
                # role_id = Json_request['role_id']
            
                requested_tables = Json_request['requesttable']
                synceddatetime = Json_request['synceddatetime']
                FormCode = Json_request['formcode']
                appversion = Json_request['appversion']
                # imeino = Json_request['imeino']
                # simno = Json_request['simno']
                apikey = Json_request['apikey']
                apptypeno = Json_request['apptypeno']
                role_id = Json_request['role_id']
                user_id = Json_request['user_id']
                
                jsonData_database = str(json.dumps(Json_request))
                
                Qid = api_custome_functions.inserQtable_data(webservice_code,jsonData_database,api_custome_functions.current_date_time_in_format(),synceddatetime)
                for tablenames in requested_tables:
                    ouputjson[tablenames] = []
                    max_rn = requested_tables[tablenames]
                    master_or_transaction_tbl = tablenames.split('_')[0]
                    
                    #table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * FROM {tablenames} WHERE fld_rn > {max_rn} AND fld_is_active=1")
                    #table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * FROM  master_tbl_user_child where fld_is_active=1")
                    if tablenames == "master_tbl_state": 
                        # table_published_check = f"SELECT * FROM master_tbl_state "
                        table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * FROM master_tbl_state where fld_rn > {max_rn} and fld_is_active='1';")
                    elif tablenames == "master_tbl_district":
                        table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * FROM master_tbl_district where fld_rn > {max_rn} and fld_is_active='1';")
                    elif tablenames == "master_tbl_taluk":
                        table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * FROM master_tbl_taluk where fld_rn > {max_rn} and fld_is_active='1';")
                    elif tablenames == "master_tbl_block":
                        table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * FROM master_tbl_block where fld_rn > {max_rn} and fld_is_active='1';")
                    elif tablenames == "master_tbl_phc":
                        table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * FROM master_tbl_phc where fld_rn > {max_rn} and fld_is_active='1';")
                    elif tablenames == "master_tbl_hsc":
                        table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * FROM master_tbl_hsc where fld_rn > {max_rn} and fld_is_active='1';")
                    elif tablenames == "master_tbl_panchyat":  
                        table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * FROM master_tbl_panchyat where fld_rn > {max_rn} and fld_is_active='1';")
                    elif tablenames == "master_tbl_village":
                        table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * FROM master_tbl_village where fld_rn > {max_rn} and fld_is_active='1';")
                    elif tablenames == "master_tbl_dynamic":
                            table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * from master_tbl_dynamic where fld_is_active=1")
                    elif tablenames == "master_tbl_dynamicqnmaster_child_mobile":
                            table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * from master_tbl_dynamicqnmaster_child_mobile where fld_is_active=1")
                    elif tablenames == "bind_tbl_common":
                            table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * from bind_tbl_common where fld_is_active=1") 
                    elif tablenames == "master_tbl_user":  
                            table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT  * FROM master_tbl_user where fld_user_id = '{user_id}' and fld_is_active=1")
                    else:
                        table_data = api_custome_functions.getting_data_in_dictionary_format(f"SELECT * FROM {tablenames} where fld_loggedin_user_id = '{user_id}' and fld_rn >= {max_rn} and  fld_is_active=1")
                
                    for table_data_dict in table_data: 
                        # check the datetime_obj_to_str_array is in the table_data_dict or not
                        if datetime_obj_to_str_array:
                            for datetime_obj_to_str in datetime_obj_to_str_array:
                                # check the datetime_obj_to_str is in the table_data_dict or not
                                if datetime_obj_to_str in table_data_dict:
                                    # convert the datetime object to string
                                    table_data_dict[datetime_obj_to_str] = str(table_data_dict[datetime_obj_to_str])
                        ouputjson[tablenames].append(table_data_dict)
                    # convertin the ouputjson to json
                    valid = 1
                    ReturnStatus = 1
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                Json_response = {
                    "status": "1",
                    "responsemessage": "success",
                    "response" : ouputjson,
                    "serverdatetime": str(api_custome_functions.current_date_time_in_format())
                }
                stringResponse = str(json.dumps(Json_response))
                api_custome_functions.UpdateQTable(FormCode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,ReturnStatus, ApiKey, AppTypeNo, AppVersion, Qid)
                return JsonResponse(Json_response)
            else:
                if request_json_validation == True:
                    Json_request = json.dumps(json.loads(request.body), default=str) # convert the json to string
                else:
                    Json_request = request.body # getting the json data
                Qid = api_custome_functions.inserQtable_data(webservice_code,str(Json_request),api_custome_functions.current_date_time_in_format(),'')
                Json_response ={
                "error_level": "2",
                "error_message": 'Invalid Json Request',
                "error_file": "views.py",
                "serverdatetime": api_custome_functions.current_date_time_in_format()   
                }
                api_custome_functions.error_log_insert(str(Json_response),Qid,'','','getLatest','1','1','2')
                api_custome_functions.UpdateQTable('',str(json.dumps(Json_response)),'0','0','1','2','','','','','',Qid)
            return JsonResponse(Json_response)
        except Exception as e:
            if request_json_validation == True:
                Json_request = json.dumps(json.loads(request.body))
            else:
                Json_request = request.body
            # Qid = api_custome_functions.inserQtable_data(webservice_code,str(Json_request),api_custome_functions.current_date_time_in_format(),'')
            error_json ={
                "error_level": "1",                                     
                "error_message": str(e),
                "error_file": "views.py",
                "serverdatetime": api_custome_functions.current_date_time_in_format()
                }
            api_custome_functions.error_log_insert(str(json.dumps(error_json)),Qid,'','','getLatest','1','1','1')
            api_custome_functions.UpdateQTable('',str(Json_request),'0','0','1','2','','','',Qid)
        return HttpResponse(json.dumps(error_json), content_type="application/json")
     
@csrf_exempt
def syncData(request):
    json_body = request.body.decode('utf-8')
    Json_request = json.loads(json_body)
    
    hh_consent_form = False
    hh_respondent_detail = False
    house_hold_details_from = False
    hh_family_details = False
    hh_standard_of_living = False
    hh_hospitalization = False
    hh_chornic_illnesses = False
    hh_hypertension = False
    hh_diabetes = False
    found_health_problem = False
    hh_diet_and_nutrition = False
    found_fc_section10 = False
    found_fc_section11 = False
    found_fc_section12 = False
    found_fc_section13 = False
    found_fc_section14 = False
    found_fc_section15 = False
    found_fc_section16 = False
    found_fc_section17 = False
    found_fc_remarks   = False
    Qid = None
     
    for table_name in Json_request:
        if "trn_tbl_hh_consent_form" in table_name:
            hh_consent_form = True
            attendence_child_response = consent_form.consent_form_insert_ajax(request)
            attendence_child_json = attendence_child_response.content.decode('utf-8')
            attendence_child_dict = json.loads(attendence_child_json)
            attendence_child = attendence_child_dict.get('status')
            Qid = attendence_child_dict.get('Qid')
        elif "trn_tbl_respondent_detail_form" in table_name:
            hh_respondent_detail = True
            attendence_child_response = respondent_detail.respondent_detail_insert_ajax(request)
            attendence_child_json = attendence_child_response.content.decode('utf-8')
            attendence_child_dict = json.loads(attendence_child_json)
            attendence_child = attendence_child_dict.get('status')
            Qid = attendence_child_dict.get('Qid')
        elif "trn_tbl_gi_house_hold_detail" in table_name:
            house_hold_details_from = True
            attendence_child_response = house_hold_details.house_hold_details_insert_ajax(request)
            attendence_child_json = attendence_child_response.content.decode('utf-8')
            attendence_child_dict = json.loads(attendence_child_json)
            attendence_child = attendence_child_dict.get('status')
            Qid = attendence_child_dict.get('Qid')
        elif "trn_tbl_family_details" in table_name:
            hh_family_details = True
            attendence_child_response = family_details.family_details_insert_ajax(request)
            attendence_child_json = attendence_child_response.content.decode('utf-8')
            attendence_child_dict = json.loads(attendence_child_json)
            attendence_child = attendence_child_dict.get('status')
            Qid = attendence_child_dict.get('Qid')
        elif "trn_tbl_standred_of_living" in table_name:
            hh_standard_of_living = True
            attendence_child_response = standard_of_living.standard_of_living_insert_ajax(request)
            attendence_child_json = attendence_child_response.content.decode('utf-8')
            attendence_child_dict = json.loads(attendence_child_json)
            attendence_child = attendence_child_dict.get('status')
            Qid = attendence_child_dict.get('Qid')
        elif "trn_tbl_hospitalization" in table_name:
            hh_hospitalization = True
            attendence_child_response = hospitalization.hospitalizations_insert_ajax(request)
            attendence_child_json = attendence_child_response.content.decode('utf-8')
            attendence_child_dict = json.loads(attendence_child_json)
            attendence_child = attendence_child_dict.get('status')
            Qid = attendence_child_dict.get('Qid')
        elif "trn_tbl_chornic_illnesses" in table_name:
            hh_chornic_illnesses = True
            attendence_child_response = chornic_illnesses.chornic_illnesses_insert_ajax(request)
            attendence_child_json = attendence_child_response.content.decode('utf-8')
            attendence_child_dict = json.loads(attendence_child_json)
            attendence_child = attendence_child_dict.get('status') 
            Qid = attendence_child_dict.get('Qid')
        elif "trn_tbl_hypertension" in table_name:
            hh_hypertension = True
            attendence_child_response = hypertension.hypertension_insert_ajax(request)
            attendence_child_json = attendence_child_response.content.decode('utf-8')
            attendence_child_dict = json.loads(attendence_child_json)
            attendence_child = attendence_child_dict.get('status')  
            Qid = attendence_child_dict.get('Qid')
        elif "trn_tbl_diabets" in table_name:
            hh_diabetes = True
            attendence_child_response = diabetes.diabetes_insert_ajax(request)
            attendence_child_json = attendence_child_response.content.decode('utf-8')
            attendence_child_dict = json.loads(attendence_child_json)
            attendence_child = attendence_child_dict.get('status')  
            Qid = attendence_child_dict.get('Qid')
        elif "trn_tbl_diet_and_nutrition" in table_name:
            hh_diet_and_nutrition= True
            attendence_child_response = diet_and_nutrition.diet_and_nutrition_insert_ajax(request)
            attendence_child_json = attendence_child_response.content.decode('utf-8')
            attendence_child_dict = json.loads(attendence_child_json)
            attendence_child = attendence_child_dict.get('status')  
            Qid = attendence_child_dict.get('Qid')
        elif "trn_tbl_health_problem" in table_name:
            hh_health_problem = True
            attendence_child_response = health_problem.health_problem_insert_ajax(request)
            attendence_child_json = attendence_child_response.content.decode('utf-8')
            attendence_child_dict = json.loads(attendence_child_json)
            attendence_child = attendence_child_dict.get('status')  
            Qid = attendence_child_dict.get('Qid')   
        elif "tbl_mobile_version_details" in table_name:
            hh_health_problem = True
            attendence_child_response = mobile_version_details.mobile_version_insert_ajax(request)
            attendence_child_json = attendence_child_response.content.decode('utf-8')
            attendence_child_dict = json.loads(attendence_child_json)
            attendence_child = attendence_child_dict.get('status')  
            Qid = attendence_child_dict.get('Qid')                  
    Json_response = {
                        "status" :"1",
                        "responsemessage" : "Data is sync successfully",
                        "serverdatetime" : api_custome_functions.current_date_time_in_format()
                    }
    return JsonResponse(Json_response)

# Check the app version and app name in the master_tbl_updetcheck if any update please give the link to download the application (Moblie APK)
@csrf_exempt
def mobile_app_version_update_check(request):
    if request.method == 'POST':
        try:
            json_body = request.body
            Json_request = json.loads(json_body)
            fldcurversionintno = Json_request['fldcurversionintno']
            android_id = Json_request['android_id']
            synceddatetime = Json_request['synceddatetime']
            formcode = Json_request['formcode']
            appversion = Json_request['appversion']
            apikey = Json_request['apikey']
            apptypeno = Json_request['apptypeno']
            webservice_code='120'
            currentdatetime = api_custome_functions.current_date_time_in_format()
            jsonData_database = str(json.dumps(Json_request))    
            Qid = api_custome_functions.inserQtable_data(webservice_code,jsonData_database,api_custome_functions.current_date_time_in_format(),synceddatetime)
            # Check if the current version exists and is active
            check_vesion_details = master_trn_updatecheck.objects.filter(fld_mob_version_code=fldcurversionintno,fld_is_active=1).exists()            
            if check_vesion_details:
                valid = 0
                ReturnStatus = 2
                IsFullyProcessed = 1
                IsPartiallyProcessed = 0
                Json_response = {
                    "status": "2",
                    "responsemessage": "No update available.",
                    "serverdatetime": currentdatetime,
                }                
            else:
                cursor = connection.cursor()
                query = "SELECT fld_file_name FROM master_trn_updatecheck WHERE fld_is_active = '1';"
                cursor.execute(query)
                file_name = cursor.fetchone()
                if file_name is None:
                    valid = 0
                    ReturnStatus = 4
                    IsFullyProcessed = 1
                    IsPartiallyProcessed = 0
                    Json_response = {
                        "status": "4",
                        "responsemessage": "No APK uploaded yet! ,please upload your first APK",
                        "serverdatetime": currentdatetime,
                    }
                else:
                    file_name = file_name[0]
                    APK_DIRECTORY = 'IIHMR_mobile_apk_uploads'
                    file_path = os.path.join(settings.MEDIA_ROOT, APK_DIRECTORY, file_name)
                    if os.path.exists(file_path):
                        with open(file_path, 'rb') as f:
                            response = HttpResponse(f.read(), content_type='application/octet-stream')
                            response['Content-Disposition'] = f'attachment; filename="{file_name}"'
                            valid = 1
                            ReturnStatus = 1
                            IsFullyProcessed = 1
                            IsPartiallyProcessed = 0
                            Json_response = {
                            "status": "1",
                            "responsemessage": "Success",
                            "serverdatetime": currentdatetime,
                        }
                    else:
                        valid = 0
                        ReturnStatus = 3
                        IsFullyProcessed = 0
                        IsPartiallyProcessed = 1
                        Json_response = {
                            "status": "3",
                            "responsemessage": "File not found.",
                            "serverdatetime": currentdatetime,
                        }
            stringResponse = str(json.dumps(Json_response))
            api_custome_functions.UpdateQTable(formcode, valid, stringResponse, IsFullyProcessed, IsPartiallyProcessed,
                                            ReturnStatus, apikey, apptypeno, appversion, Qid)
            return JsonResponse(Json_response)
        except json.JSONDecodeError:
            return JsonResponse({"status": "4", "responsemessage": "Invalid JSON."}, status=400)
        except KeyError as e:
            return JsonResponse({"status": "5", "responsemessage": f"Missing key: {str(e)}."}, status=400)
        except Exception as e:
            return JsonResponse({"status": "6", "responsemessage": str(e)}, status=500)
    else:
        return JsonResponse({"status": "7", "responsemessage": "Invalid request method."}, status=405)

# Download the Uploaded APK from the MEDIA folder 
@csrf_exempt
def download_IIHMR_mobile_apk(request):
    folder = 'IIHMR_mobile_apk_uploads'
    cursor = connection.cursor()
    query =  (f"SELECT fld_file_name FROM master_trn_updatecheck where fld_is_active = 1;")
    cursor.execute(query)
    file_name = cursor.fetchone()[0]
    file_path = os.path.join(settings.MEDIA_ROOT, folder, file_name)
    with open(file_path, 'rb') as f:
        response = HttpResponse(f.read(), content_type='application/octet-stream')
        response['Content-Disposition'] = f'attachment; filename="{file_name}"'
    return response
    
