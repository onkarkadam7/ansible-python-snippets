#!/usr/bin/python

import os 
import psycopg2 
from ansible.module_utils.basic import *

def pgsql_present(data):
    
    query  = data['pg_sql_file'] 
    dbname = data['pg_dbname']
    dbhost = data['pg_dbhost']
    dbport = data['pg_port']
    dbuser = data['pg_user']
    dbpassword = data['pg_password']
    del data['state']   
    
    try:
        connection=psycopg2.connect("user=%s dbname=%s host=%s port=%s password=%s" % (dbuser, dbname, dbhost, dbport, dbpassword))
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()
        return False, True, {"status": "SUCCESS"}
    except Exception, e:
        return False, False, e.json()
    
      # default: something went wrong
    meta = {"status": result.status_code, 'response': result.json()}
    return True, False, meta
          
    
def main():

    fields = {
        "pg_sql_file": {"required": True, "type": "str"},
        "pg_dbname": {"required": True, "type": "str"},
        "pg_dbhost": {"required": False, "type": "str"},
        "pg_port": {"required": True, "type": "str"},
        "pg_user": {"required": True, "type": "str"},
        "pg_password": {"required": True, "type": "str"},
        "state": {
            "default": "present",
            "choices": ['present'],
            "type": 'str'
        },
    }

    choice_map = {
        "present": pgsql_present,
    }

    module = AnsibleModule(argument_spec=fields)
    is_error, has_changed, result = choice_map.get(
        module.params['state'])(module.params)

    if not is_error:
        module.exit_json(changed=has_changed, meta=result)
    else:
        module.fail_json(msg="Error initializing sql", meta=result)


if __name__ == '__main__':
    main()
