import pymysql
import json
import datetime,time
import db_module

def do_main():
    try:
        #db_conn = pymysql.connect(host="localhost", user="user", password="pwd", db="db", charset="utf8")
        #db_conn.autocommit = False

        print("Hello robot")
        str_sql = """ SELECT RUN_ID,LOT_ID,EQP_ID,MAT_ID,OPR_ID,PJT_ID,SEQ_IT,TYPE_ID,TASK_ID,QTY_IT,UOM_ID
                      FROM MSGDB.RBT_EQP_RUN
                      WHERE EQP_ID = %s AND MAT_ID = %s AND PJT_ID = %s
                  """
        tup_param = ("EQP01","MAT01","N")

        dic_db_request = dict()
        dic_db_request["req_sql"] = str_sql
        dic_db_request["req_param"] = tup_param

        dic_db_return = db_module.db_select(dic_db_request)
        print(dic_db_return)

        if dic_db_return["status"] != "OK":
            raise Exception("Get User - Key db_module.tr_select Error")
        rows = dic_db_return["rows"]

        run_eqp(rows)

        #db_conn.close()


    except Exception as e:
        print("ERROR : robot_main > do_main : ",str(e))


def run_eqp(rows):
    try:
        for row in rows:
            dic_eqp_run = dict()
            dic_eqp_run["RUN_ID"] = row[0]  
            dic_eqp_run["LOT_ID"] = row[1] 
            dic_eqp_run["EQP_ID"] = row[2] 
            dic_eqp_run["MAT_ID"] = row[3] 
            dic_eqp_run["OPR_ID"] = row[4]               
            dic_eqp_run["PJT_ID"] = row[5]
            dic_eqp_run["SEQ_IT"] = row[6]
            dic_eqp_run["TYPE_ID"] = row[7]
            dic_eqp_run["TASK_ID"] = row[8]
            dic_eqp_run["QTY_IT"] = row[9]
            dic_eqp_run["UOM_ID"] = row[10]

            if dic_eqp_run["TYPE_ID"] == "START" :
                run_eqp_start(dic_eqp_run)
            elif dic_eqp_run["TYPE_ID"] == "END" :
                run_eqp_end(dic_eqp_run)
            elif dic_eqp_run["TYPE_ID"] == "PROCESS" :
                run_eqp_process_start(dic_eqp_run)
                time.sleep(dic_eqp_run["QTY_IT"])
                run_eqp_process_end(dic_eqp_run)

    except Exception as e:
        print("ERROR : robot_main > run_eqp : ",str(e))

def run_eqp_start(dic_eqp_run):
    try:
        print("run_eqp_start")
        db_conn = pymysql.connect(host="localhost", user="user", password="pwd", db="db", charset="utf8")
        db_conn.autocommit = False

        dic_msg_start = dict()
        dic_msg_start["LOT_ID"] = dic_eqp_run["LOT_ID"]
        dic_msg_start["EQP_ID"] = dic_eqp_run["EQP_ID"]
        dic_msg_start["MAT_ID"] = dic_eqp_run["MAT_ID"]
        dic_msg_start["OPR_ID"] = dic_eqp_run["OPR_ID"]
        dic_msg_start["PJT_ID"] = dic_eqp_run["PJT_ID"]

        str_msg_eqp_id = dic_eqp_run["EQP_ID"]
        
        dt_now = datetime.datetime.now()
        str_msg_id = dt_now.strftime("%Y%m%d%H%M%S") + "-" + dt_now.strftime("%f") + "-" + str_msg_eqp_id

        str_msg_type_id = "EQP_START"
        str_msg_dat_js =  json.dumps(dic_msg_start)
        str_msg_dtm_st = dt_now.strftime("%Y-%m-%d %H:%M:%S")
        str_msg_sts_id = "0"    

        str_sql = """ INSERT INTO RBT_MSG_OUT(MSG_ID,MSG_EQP_ID,MSG_TYPE_ID,MSG_DAT_JS,MSG_STS_ID,MSG_DTM_ST)
                                  VALUES(%s,%s,%s,%s,%s,%s)
                    """
        tup_param = (str_msg_id,str_msg_eqp_id,str_msg_type_id,str_msg_dat_js,str_msg_sts_id,str_msg_dtm_st)

        dic_db_request = dict()
        dic_db_request["req_sql"] = str_sql
        dic_db_request["req_param"] = tup_param

        dic_db_return = db_module.tr_execute(db_conn,dic_db_request)
        if dic_db_return["status"] != "OK":
            raise Exception("Insert MSG_QUE_DAT  db_module.tr_select Error")   

        db_conn.commit()
        db_conn.close()      

    except Exception as e:
        db_conn.rollback()
        db_conn.close() 

        print("ERROR : robot_main > run_eqp_start : ",str(e))



def run_eqp_end(dic_eqp_run):
    try:
        print("run_eqp_end")
        db_conn = pymysql.connect(host="localhost", user="user", password="pwd", db="db", charset="utf8")
        db_conn.autocommit = False

        dic_msg_start = dict()
        dic_msg_start["LOT_ID"] = dic_eqp_run["LOT_ID"]
        dic_msg_start["EQP_ID"] = dic_eqp_run["EQP_ID"]
        dic_msg_start["MAT_ID"] = dic_eqp_run["MAT_ID"]
        dic_msg_start["OPR_ID"] = dic_eqp_run["OPR_ID"]
        dic_msg_start["PJT_ID"] = dic_eqp_run["PJT_ID"]


        str_msg_eqp_id = dic_eqp_run["EQP_ID"]

        dt_now = datetime.datetime.now()
        str_msg_id = dt_now.strftime("%Y%m%d%H%M%S") + "-" + dt_now.strftime("%f") + "-" + str_msg_eqp_id

        str_msg_type_id = "EQP_END"
        str_msg_dat_js =  json.dumps(dic_msg_start)
        str_msg_dtm_st = dt_now.strftime("%Y-%m-%d %H:%M:%S")
        str_msg_sts_id = "0"    

        str_sql = """ INSERT INTO RBT_MSG_OUT(MSG_ID,MSG_EQP_ID,MSG_TYPE_ID,MSG_DAT_JS,MSG_STS_ID,MSG_DTM_ST)
                                  VALUES(%s,%s,%s,%s,%s,%s)
                    """
        tup_param = (str_msg_id,str_msg_eqp_id,str_msg_type_id,str_msg_dat_js,str_msg_sts_id,str_msg_dtm_st)

        dic_db_request = dict()
        dic_db_request["req_sql"] = str_sql
        dic_db_request["req_param"] = tup_param

        dic_db_return = db_module.tr_execute(db_conn,dic_db_request)
        if dic_db_return["status"] != "OK":
            raise Exception("Insert MSG_QUE_DAT  db_module.tr_select Error")  
    
        db_conn.commit()
        db_conn.close()   

    except Exception as e:
        db_conn.rollback()
        db_conn.close()

        print("ERROR : robot_main > run_eqp_end : ",str(e))


def run_eqp_process_start(dic_eqp_run):
    try:
        print("run_eqp_process_start : ",str(dic_eqp_run["QTY_IT"]))
        db_conn = pymysql.connect(host="localhost", user="user", password="pwd", db="db", charset="utf8")
        db_conn.autocommit = False

        dic_msg_start = dict()
        dic_msg_start["LOT_ID"] = dic_eqp_run["LOT_ID"]
        dic_msg_start["EQP_ID"] = dic_eqp_run["EQP_ID"]
        dic_msg_start["MAT_ID"] = dic_eqp_run["MAT_ID"]
        dic_msg_start["OPR_ID"] = dic_eqp_run["OPR_ID"]
        dic_msg_start["PJT_ID"] = dic_eqp_run["PJT_ID"]


        str_msg_eqp_id = dic_eqp_run["EQP_ID"]

        dt_now = datetime.datetime.now()
        str_msg_id = dt_now.strftime("%Y%m%d%H%M%S") + "-" + dt_now.strftime("%f") + "-" + str_msg_eqp_id

        str_msg_type_id = "PROCESS_START"
        str_msg_dat_js =  json.dumps(dic_msg_start)
        str_msg_dtm_st = dt_now.strftime("%Y-%m-%d %H:%M:%S")
        str_msg_sts_id = "0"    

        str_sql = """ INSERT INTO RBT_MSG_OUT(MSG_ID,MSG_EQP_ID,MSG_TYPE_ID,MSG_DAT_JS,MSG_STS_ID,MSG_DTM_ST)
                                  VALUES(%s,%s,%s,%s,%s,%s)
                    """
        tup_param = (str_msg_id,str_msg_eqp_id,str_msg_type_id,str_msg_dat_js,str_msg_sts_id,str_msg_dtm_st)

        dic_db_request = dict()
        dic_db_request["req_sql"] = str_sql
        dic_db_request["req_param"] = tup_param

        dic_db_return = db_module.tr_execute(db_conn,dic_db_request)
        if dic_db_return["status"] != "OK":
            raise Exception("Insert MSG_QUE_DAT  db_module.tr_select Error")
        
        db_conn.commit()
        db_conn.close()    

    except Exception as e:
        db_conn.rollback()
        db_conn.close()    

        print("ERROR : robot_main > run_eqp_process : ",str(e))


def run_eqp_process_end(dic_eqp_run):
    try:
        print("run_eqp_process_end")
        db_conn = pymysql.connect(host="localhost", user="user", password="pwd", db="db", charset="utf8")
        db_conn.autocommit = False

        dic_msg_start = dict()
        dic_msg_start["LOT_ID"] = dic_eqp_run["LOT_ID"]
        dic_msg_start["EQP_ID"] = dic_eqp_run["EQP_ID"]
        dic_msg_start["MAT_ID"] = dic_eqp_run["MAT_ID"]
        dic_msg_start["OPR_ID"] = dic_eqp_run["OPR_ID"]
        dic_msg_start["PJT_ID"] = dic_eqp_run["PJT_ID"]


        str_msg_eqp_id = dic_eqp_run["EQP_ID"]

        dt_now = datetime.datetime.now()
        str_msg_id = dt_now.strftime("%Y%m%d%H%M%S") + "-" + dt_now.strftime("%f") + "-" + str_msg_eqp_id

        str_msg_type_id = "PROCESS_END"
        str_msg_dat_js =  json.dumps(dic_msg_start)
        str_msg_dtm_st = dt_now.strftime("%Y-%m-%d %H:%M:%S")
        str_msg_sts_id = "0"    

        str_sql = """ INSERT INTO RBT_MSG_OUT(MSG_ID,MSG_EQP_ID,MSG_TYPE_ID,MSG_DAT_JS,MSG_STS_ID,MSG_DTM_ST)
                                  VALUES(%s,%s,%s,%s,%s,%s)
                    """
        tup_param = (str_msg_id,str_msg_eqp_id,str_msg_type_id,str_msg_dat_js,str_msg_sts_id,str_msg_dtm_st)

        dic_db_request = dict()
        dic_db_request["req_sql"] = str_sql
        dic_db_request["req_param"] = tup_param

        dic_db_return = db_module.tr_execute(db_conn,dic_db_request)
        if dic_db_return["status"] != "OK":
            raise Exception("Insert MSG_QUE_DAT  db_module.tr_select Error")
        
        db_conn.commit()
        db_conn.close()    

    except Exception as e:
        db_conn.rollback()
        db_conn.close()    

        print("ERROR : robot_main > run_eqp_process : ",str(e))

if __name__ == "__main__":
    do_main()