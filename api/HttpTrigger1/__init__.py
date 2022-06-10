import logging
import pyodbc
import json
import os
import time
import datetime
import azure.functions as func

#宣告執行的主程序
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
#設定連接字串
    sqlConnectionString = os.environ["SQLConnectionString"]
    #turkeySize = ''
    messages = []
    #設定返回狀態
    statusCode = 200
    ingredients = []	
    iotdata = []
    plugdata = []
	
    try:#取得來自post請求的數據
        req_body_bytes =  req.get_body()
        logging.info(f"Request Bytes: {req_body_bytes}")
        req_body = req_body_bytes.decode("utf-8") #使用UTF8
        logging.info(f"Request: {req_body}")
        my_json = json.loads(req_body) #取出JSON
        turkeyS = my_json['name'] #取得輸入的名
        logging.info(turkeyS)
        turkeyS1 = my_json['plug'] #取得輸入的plug
        #turkeyS1 = '565-50-0015-30'
        logging.info(turkeyS1)		
    except:#送出問題的訊息
        messages.append('Use the query string "turkey" to send a turkey .')
        return generateHttpResponse(ingredients, messages, 400)
    try:#連接資料庫
        sqlConnection = getSqlConnection(sqlConnectionString)
    except:#送出問題的訊息
        messages.append('sqlConnection error.')
        return generateHttpResponse(ingredients, messages, 400)	
    try:
        a = 'refresh'
        if turkeyS == a :
            plugrecord = getIngredients2(sqlConnection)
            p=0
            pc=0
            for item in plugrecord:
                p = item['sn'] 
            p8 = 'reset'
            p +=1
            getIngredients3(sqlConnection, p, turkeyS, p8, p8, p8, p8,p8,pc)
            messages.append('refresh SN  .')
            return generateHttpResponse(ingredients, messages, 200)			
		
    except:
        messages.append('plugcount error.')
        return generateHttpResponse(ingredients, messages, 400)	
    try:	
        #讀取一筆avaiotdata資料的程序
        avaiotdata = getIngredients4(sqlConnection)
        logging.info('avaiotdata: %s', avaiotdata)
        #取出影像識別的結果跟創建時間 		
        for item in avaiotdata:
            p1 = item['name']
            p4 = item['Created_On']			
        logging.info('avaiotdata1: %s', p1) 
        logging.info('avaiotdata2: %s', p4)		
    except:#送出問題的訊息
        messages.append('avaiotdata error.')
        return generateHttpResponse(avaiotdata, messages, 400)			
    try:	#讀取plugdata資料的程序
        plugdata = getIngredients1(sqlConnection, turkeyS1)
        logging.info('plugdata: %s', plugdata)
        #Plug資料
        p2 = plugdata[2] 
        #Plug-R資料
        p3 = plugdata[3] 
        #Item Number資料
        p10 = plugdata[0] 	
	
        logging.info('plugdata1: %s', p2) 
        logging.info('plugdata2: %s', p3)		
    except:#送出問題的訊息
        messages.append('plugname error.')
        return generateHttpResponse(plugdata, messages, 401)		
    try:	#讀取10筆資料的程序
        plugrecord = getIngredients2(sqlConnection)
        #p = json.loads(plugrecord)
        p=0
        pc=0 #取出資料
        for item in plugrecord:
            p = item['sn'] 
            pc = item['plugcount'] 
        p +=1
        pc +=1
        logging.info('plugrecord: %s', str(p)) 
        p6 = 0
        p7 = 'fault'
		#輸入資料跟影像結果比對
        for item in avaiotdata:
            p5 = item['name']
            if (p5==p2) or (p5==p3):
                p6 +=1 #10筆內有大於5筆的資料跟輸入一樣就認定是ok
                p5save = p5
        if p6 > 5 :
            p7 = 'ok'
            p5 = p5save
	    
        #date2 = str(datetime.datetime.now())
        #增加一筆備註
        date1 = 'test123'
        p8 = p2 +'or'+p3
        p9 = p4
        logging.info('plugrecord1: %s', date1)   
        #宣告寫入一筆執行紀錄資料的程序
        getIngredients3(sqlConnection, p, turkeyS, p10, p8, p5, p7, p9,pc)		
        plugrecord = getIngredients2(sqlConnection)
        b = 'fault' 
        if p7 == b :
            getIngredients6(sqlConnection, turkeyS, p10, p8, p5, p7, p9, p)
            		
    except:#送出問題的訊息
        messages.append('plugrecord error.')
        #錯誤就直接返回
        return generateHttpResponse(plugrecord, messages, 400)	
    return generateHttpResponse2(plugrecord, messages, statusCode)
#宣告增加json的程序,關於Messages與Ingredients
def generateHttpResponse(ingredients, messages, statusCode):
    return func.HttpResponse(
        json.dumps({"Messages": messages, "Ingredients": ingredients}, sort_keys=True, indent=4),
        status_code=statusCode
    )
#宣告增加json的程序,關於plugdata與AVAiotdata與Messages
def generateHttpResponse1(plugdata, iotdata, messages, statusCode):
    return func.HttpResponse(
        json.dumps({"plugdata": plugdata, "AVAiotdata": iotdata,"Messages": messages}, sort_keys=True, indent=4),
        status_code=statusCode
    )
#宣告增加json的程序,關於plugdata與Messages
def generateHttpResponse2(plugdata, messages, statusCode):
    return func.HttpResponse(
        json.dumps({"plugdata": plugdata, "Messages": messages}, sort_keys=True, indent=4),
        status_code=statusCode
    )
#宣告連接資料庫的程序	
def getSqlConnection(sqlConnectionString):
    i = 0 #執行1-6次
    while i < 6:
        logging.info('contacting DB')
        try: #連接資料庫
            sqlConnection = pyodbc.connect(sqlConnectionString)
        except:
            time.sleep(10) # wait 10s before retry
            i+=1
        else:
            return sqlConnection
#宣告讀取plugdata資料的程序
def getIngredients1(sqlConnection, turkeyS1):
    turkeyS2 = str(turkeyS1)

    results = []
    sqlCursor = sqlConnection.cursor()
    #設定搜尋資料庫	
    sql =  "SELECT * FROM cablels WHERE Item_Number =  " +"'"+turkeyS1+"'"
    logging.info('getting plugname1: %s',sql)   
    #執行搜尋資料庫
    sqlCursor.execute(sql)
    #返回單個的元組，也就是一條記錄(row)，如果沒有結果, 則返回None
    results = sqlCursor.fetchone()
    logging.info('getting plugname2: %s',results)    
    sqlCursor.commit()
    sqlCursor.close()
    return results	
#宣告讀取10筆資料的程序
def getIngredients2(sqlConnection):
    logging.info('getting plugrecord3')
    results = []
    #設定搜尋資料庫	
    sqlCursor = sqlConnection.cursor()
    #執行plugrecord3程序讀取10筆資料
    sqlCursor.execute('EXEC plugrecord3 ')
    #返回單個的元組，也就是一條記錄(row)，如果沒有結果, 則返回None	
    results = json.loads(sqlCursor.fetchone()[0])
    #提交至 SQL	
    sqlCursor.commit()
    #關閉 SQL 連線
    sqlCursor.close()
    return results
#宣告讀取10筆資料的程序			
def getIngredients3(sqlConnection, turkey1, turkey2, turkey3, turkey4, turkey5, turkey6, turkey7, turkey8):
    logging.info('getting plugrecord4')
    #設定搜尋資料庫
    sqlCursor = sqlConnection.cursor()
    #執行plugrecord4程序寫入一筆資料
    go1 = "EXEC plugrecord4 "+str(turkey1)+" , "+"'"+turkey2+"'"+" , "+"'"+turkey3+"'"+" ,"+"'"+turkey4+"'"+" ,"+"'"+turkey5+"'"+" , "+"'"+turkey6+"'"+" , "+"'"+turkey7+"'"+", "+"'"+str(turkey8)+"'"
    logging.info('plugrecord: %s', go1) 
    sqlCursor.execute(go1)
    #提交至 SQL
    sqlCursor.commit()
    #關閉 SQL 連線
    sqlCursor.close()
    return
#宣告讀取一筆avaiotdata資料的程序		
def getIngredients4(sqlConnection):
    logging.info('getting avaiotdata')
    results = []
    #設定搜尋資料庫
    sqlCursor = sqlConnection.cursor()
    #執行avaiot1程序讀取一筆avaiotdata資料
    sqlCursor.execute('EXEC avaiot1 ')
    #返回單個的元組，也就是一條記錄(row)，如果沒有結果, 則返回None
    results = json.loads(sqlCursor.fetchone()[0])
    #提交至 SQL
    sqlCursor.commit()
    #關閉 SQL 連線
    sqlCursor.close()
    return results	

def getIngredients6(sqlConnection,turkey2, turkey3, turkey4, turkey5, turkey6, turkey7, turkey1):
    logging.info('getting plugng4')

    sqlCursor = sqlConnection.cursor()
    go1 = "EXEC plugng4 "+turkey2+" , "+"'"+turkey3+"'"+" , "+"'"+turkey4+"'"+" ,"+"'"+turkey5+"'"+" ,"+"'"+turkey6+"'"+", "+"'"+turkey7+"'"+"   , "+"'"+str(turkey1)+"'"
    logging.info('plugng: %s', go1) 
    sqlCursor.execute(go1)
    sqlCursor.commit()
    sqlCursor.close()
    return		
