import pymysql

# db_module 2023.05.13 

def db_select(dict_request):
    try:
        conn = pymysql.connect(host="3.35.6.10", user="webuser", password="mysql", db="webfab", charset="utf8")
        curs = conn.cursor()

        str_sql = dict_request["req_sql"]
        tup_param = dict_request["req_param"]

        tup_cnt = len(tup_param)
        if (tup_cnt > 0) :
            curs.execute(str_sql,tup_param)
        else :
            curs.execute(str_sql)            
        rows = curs.fetchall()

        curs.close()
        conn.close()
   
        dict_return = dict()
        dict_return["status"] = "OK"
        dict_return["rowcount"] = curs.rowcount
        dict_return["description"] = curs.description
        dict_return["rows"] = rows   
        print(dict_return)
        return dict_return

    except pymysql.Error as e:
        print("pymysql error ==> %d: %s" %(e.args[0], e.args[1]))

        dict_return = dict()
        dict_return["status"] = "ERROR"
        dict_return["err_code"] = str(e.args[0])
        dict_return["err_desc"] = e.args[1] 
        dict_return["err_path"] = "db_module : db_select"

        curs.close()
        conn.close()

        print(dict_return)
        return dict_return 

    except Exception as e:
        dict_return = dict()
        dict_return["status"] = "ERROR"
        dict_return["err_cd"] = "E001"
        dict_return["err_desc"] = str(e) 
        dict_return["err_path"] = "db_module : db_select"

        curs.close()
        conn.close()

        print(dict_return)
        return dict_return 


def db_execute(dict_request):
    try:
        conn = pymysql.connect(host="3.35.6.10", user="webuser", password="mysql", db="webfab", charset="utf8")
        curs = conn.cursor()

        str_sql = dict_request["req_sql"]
        tup_param = dict_request["req_param"]

        tup_cnt = len(tup_param)
        if (tup_cnt > 0) :
            curs.execute(str_sql,tup_param)
        else :
            curs.execute(str_sql) 

        conn.commit()
        curs.close()
        conn.close()
   
        dict_return = dict()
        dict_return["status"] = "OK"
    
        return dict_return

    except pymysql.Error as e:
        #print("pymysql error ==> %d: %s" %(e.args[0], e.args[1]))

        dict_return = dict()
        dict_return["status"] = "ERROR"
        dict_return["err_code"] = str(e.args[0])
        dict_return["err_desc"] = e.args[1] 
        dict_return["err_path"] = "db_module : db_execute"
        #print(dict_return)

        conn.rollback()
        curs.close()
        conn.close()

        return dict_return 



    except Exception as e:
        dict_return = dict()
        dict_return["status"] = "ERROR"
        dict_return["err_cd"] = "S001"
        dict_return["err_desc"] = str(e)  
        dict_return["err_path"] = "db_module : db_execute"
        #print(dict_return)

        conn.rollback()
        curs.close()
        conn.close()

        return dict_return 

def tr_select(db_conn,dict_request):
    try:

        curs = db_conn.cursor()

        str_sql = dict_request["req_sql"]
        tup_param = dict_request["req_param"]
        #print(dict_request)
        
        tup_cnt = len(tup_param)
        if (tup_cnt > 0) :
            curs.execute(str_sql,tup_param)
        else :
            curs.execute(str_sql)            
        rows = curs.fetchall()
   
        dict_return = dict()
        dict_return["status"] = "OK"
        dict_return["rowcount"] = curs.rowcount
        dict_return["description"] = curs.description
        dict_return["rows"] = rows   
        #print(dict_return)

        curs.close()
        return dict_return

    except pymysql.Error as e:
        #print("pymysql error ==> %d: %s" %(e.args[0], e.args[1]))

        dict_return = dict()
        dict_return["status"] = "NG"
        dict_return["err_code"] = str(e.args[0])
        dict_return["err_desc"] = e.args[1] 
        dict_return["err_path"] = "db_module : tr_select"
        #print(dict_return)

        curs.close()
        return dict_return 

    except Exception as e:

        dict_return = dict()
        dict_return["status"] = "NG"
        dict_return["err_code"] = "E001"
        dict_return["err_desc"] = str(e) 
        dict_return["err_path"] = "db_module : tr_select"
        #print(dict_return)

        curs.close()
        return dict_return 


def tr_execute(db_conn,dict_request):
    try:
        curs = db_conn.cursor()

        str_sql = dict_request["req_sql"]
        tup_param = dict_request["req_param"]
        #print(dict_request)


        tup_cnt = len(tup_param)
        if (tup_cnt > 0) :
            curs.execute(str_sql,tup_param)
        else :
            curs.execute(str_sql) 

        dict_return = dict()
        dict_return["status"] = "OK"
        dict_return["rowcount"] = curs.rowcount

        curs.close()
        return dict_return

    except pymysql.Error as e:
        #print("pymysql error ==> %d: %s" %(e.args[0], e.args[1]))

        dict_return = dict()
        dict_return["status"] = "NG"
        dict_return["err_code"] = str(e.args[0])
        dict_return["err_desc"] = e.args[1] 
        dict_return["err_path"] = "db_module : tr_execute"
        #print(dict_return)

        curs.close()
        return dict_return 

    except Exception as e:

        dict_return = dict()
        dict_return["status"] = "NG"
        dict_return["err_code"] = "E001"
        dict_return["err_desc"] = str(e) 
        dict_return["err_path"] = "db_module : tr_execute"
        #print(dict_return)

        curs.close()
        return dict_return 
