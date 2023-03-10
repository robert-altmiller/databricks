# Databricks notebook source
# DBTITLE 1,Library Imports
import requests, os

# COMMAND ----------

# DBTITLE 1,Environment Variables
# databricks instance address
databricks_instance = "adb-1138300894056801.1.azuredatabricks.net"
# databricks personal access token
databricks_pat = 

# COMMAND ----------

# DBTITLE 1,Rest API Post Requests Functions
# get requests parameters
def get_params():
    params = {}
    return params


# get requests headers
def get_headers(token = None):
    headers = {'Authorization': 'Bearer %s' % token}
    return headers


# post request
def post_request(url = None, headers = None, params = None, data = None):
    if params != None:
        return requests.post(url, params = params, headers = headers, json = data)
    else: return requests.post(url, headers = headers, json = data)


# get request
def get_request(url = None, headers = None, params = None, data = None):
    if params != None:
        return requests.get(url, params = params, headers = headers, json = data)
    else: return requests.get(url, headers = headers, json = data)

# COMMAND ----------

# DBTITLE 1,Databricks Groups Configuration
def get_api_config(dbricks_instance = None, api_topic = "groups", api_call_type = "list-members"):
    config = {
        # databricks workspace instance
        "databricks_ws_instance": dbricks_instance,
        # databricks rest api version
        "api_version": "api/2.0",
        # databricks rest api service call
        "api_topic": api_topic,
        # databricks api call type
        "api_call_type": api_call_type
    }
    config["databricks_host"] = "https://" + config["databricks_ws_instance"]
    config["api_full_url"] = config["databricks_host"] + "/" + config["api_version"] + "/" + config["api_topic"] + "/" + config["api_call_type"]
    return config

# COMMAND ----------

# DBTITLE 1,Get Databricks Rest 2.0 API Action Configurations - Groups
# groups - add member configuration
add_member_config = get_api_config(databricks_instance, "groups", "add-member")
print(f"add_member_config: {add_member_config}\n")

# groups - list member configuration
list_member_config = get_api_config(databricks_instance, "groups", "list-members")
print(f"list_member_config: {list_member_config}\n")

# groups - list all groups configuration
list_all_config = get_api_config(databricks_instance, "groups", "list-all")
print(f"list_all_config: {list_all_config}\n")

# COMMAND ----------

# DBTITLE 1,Execute Databricks Rest API 2.0 Call (Generic)
# execute rest api call
# call_type variable is 'get' or 'post'
def execute_rest_api_call(function_call_type, config = None, token = None, jsondata = None):
    headers = get_headers(token)
    response = function_call_type(url = config["api_full_url"], headers = headers, data = jsondata)
    return response

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - List Group Members
jsondata = {'group_name': 'dbricks-contributors'}
response = execute_rest_api_call(get_request, list_member_config, databricks_pat, jsondata)
print(response.text)

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Add Group Member (e.g. AAD Groups, Service Principle, or User)
jsondata = {'user_name': "robert.altmiller@databricks.com", 'parent_name': 'dbricks-readers'}
response = execute_rest_api_call(post_request, add_member_config, databricks_pat, jsondata)
print(response)
