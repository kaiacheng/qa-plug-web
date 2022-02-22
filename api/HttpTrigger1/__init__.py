import logging
import pyodbc
import json
import os
import time
import datetime
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    sqlConnectionString = os.environ["SQLConnectionString"]
    #turkeySize = ''
    messages = []
    statusCode = 200
    ingredients = []	
    iotdata = []
    plugdata = []
	
    try:
        req_body_bytes =  req.get_body()
        logging.info(f"Request Bytes: {req_body_bytes}")
        req_body = req_body_bytes.decode("utf-8")
        logging.info(f"Request: {req_body}")
        my_json = json.loads(req_body)
        turkeyS = my_json['name']
        logging.info(turkeyS)
        turkeyS1 = my_json['plug']
        #turkeyS1 = '565-50-0015-30'
        logging.info(turkeyS1)		
    except:
        messages.append('Use the query string "turkey" to send a turkey .')
        return generateHttpResponse(ingredients, messages, 400)
    try:
        sqlConnection = getSqlConnection(sqlConnectionString)
    except:
        messages.append('sqlConnection error.')
        return generateHttpResponse(ingredients, messages, 400)		
    try:	
        avaiotdata = getIngredients4(sqlConnection)
        logging.info('avaiotdata: %s', avaiotdata) 		
        for item in avaiotdata:
            p1 = item['name']
            p4 = item['Created_On']			
        logging.info('avaiotdata1: %s', p1) 
        logging.info('avaiotdata2: %s', p4)		
    except:
        messages.append('avaiotdata error.')
        return generateHttpResponse(avaiotdata, messages, 400)			
    try:	
        plugdata = getIngredients1(sqlConnection, turkeyS1)
        logging.info('plugdata: %s', plugdata)
        p2 = plugdata[2] 
        p3 = plugdata[3] 
        p10 = plugdata[0] 	
	
        logging.info('plugdata1: %s', p2) 
        logging.info('plugdata2: %s', p3)		
    except:
        messages.append('plugname error.')
        return generateHttpResponse(plugdata, messages, 400)		
    try:	
        plugrecord = getIngredients2(sqlConnection)
        #p = json.loads(plugrecord)
        p=0
        for item in plugrecord:
            p = item['sn'] 
        p +=1
        logging.info('plugrecord: %s', str(p)) 
        p6 = 0
        p7 = 'fault'
		
        for item in avaiotdata:
            p5 = item['name']
            if (p5==p2) or (p5==p3):
                p6 +=1
        if p6 > 5 :
            p7 = 'ok'
	    
        #date2 = str(datetime.datetime.now())
        date1 = 'test123'
        p8 = p2 +'or'+p3
        p9 = p1 +'_'+p4
        logging.info('plugrecord1: %s', date1)   
        getIngredients3(sqlConnection, p, turkeyS, p10, p8, p5, p7, p9)		
        plugrecord = getIngredients2(sqlConnection)		
    except:
        messages.append('plugrecord error.')
        return generateHttpResponse(plugrecord, messages, 400)	
    return generateHttpResponse2(plugrecord, messages, statusCode)

def generateHttpResponse(ingredients, messages, statusCode):
    return func.HttpResponse(
        json.dumps({"Messages": messages, "Ingredients": ingredients}, sort_keys=True, indent=4),
        status_code=statusCode
    )

def generateHttpResponse1(plugdata, iotdata, messages, statusCode):
    return func.HttpResponse(
        json.dumps({"plugdata": plugdata, "AVAiotdata": iotdata,"Messages": messages}, sort_keys=True, indent=4),
        status_code=statusCode
    )
def generateHttpResponse2(plugdata, messages, statusCode):
    return func.HttpResponse(
        json.dumps({"plugdata": plugdata, "Messages": messages}, sort_keys=True, indent=4),
        status_code=statusCode
    )	
def getSqlConnection(sqlConnectionString):
    i = 0
    while i < 6:
        logging.info('contacting DB')
        try:
            sqlConnection = pyodbc.connect(sqlConnectionString)
        except:
            time.sleep(10) # wait 10s before retry
            i+=1
        else:
            return sqlConnection

def getIngredients1(sqlConnection, turkeyS1):
    turkeyS2 = str(turkeyS1)

    results = []
    sqlCursor = sqlConnection.cursor()
    sql =  "SELECT * FROM cablels WHERE Item_Number =  " +"'"+turkeyS1+"'"
    logging.info('getting plugname1: %s',sql)   
    sqlCursor.execute(sql)
    results = sqlCursor.fetchone()
    logging.info('getting plugname2: %s',results)    
    sqlCursor.commit()
    sqlCursor.close()
    return results	
def getIngredients2(sqlConnection):
    logging.info('getting plugrecord3')
    results = []
    sqlCursor = sqlConnection.cursor()
    sqlCursor.execute('EXEC plugrecord3 ')
    results = json.loads(sqlCursor.fetchone()[0])
    sqlCursor.commit()
    sqlCursor.close()
    return results		
def getIngredients3(sqlConnection, turkey1, turkey2, turkey3, turkey4, turkey5, turkey6, turkey7):
    logging.info('getting plugrecord4')

    sqlCursor = sqlConnection.cursor()
    go1 = "EXEC plugrecord4 "+str(turkey1)+" , "+"'"+turkey2+"'"+" , "+"'"+turkey3+"'"+" ,"+"'"+turkey4+"'"+" ,"+"'"+turkey5+"'"+" , "+"'"+turkey6+"'"+" , "+"'"+turkey7+"'"
    logging.info('plugrecord: %s', go1) 
    sqlCursor.execute(go1)
    sqlCursor.commit()
    sqlCursor.close()
    return		
def getIngredients4(sqlConnection):
    logging.info('getting avaiotdata')
    results = []
    sqlCursor = sqlConnection.cursor()
    sqlCursor.execute('EXEC avaiot1 ')
    results = json.loads(sqlCursor.fetchone()[0])
    sqlCursor.commit()
    sqlCursor.close()
    return results	