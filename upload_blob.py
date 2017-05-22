#! /usr/bin/python
import cx_Oracle
import sys
import tinys3
import os
from random import randint

staticPath = '/home/ec2-user/python_code/'
dbUsername = os.environ['dbUsername']
password = os.environ['password']
host = os.environ['host']
port = '1521'
sid = os.environ['sid']

#s3 creditinals
accessKeyId     = os.environ['accessKeyId']
secretAccessKey = os.environ['secretAccessKey']
tlsValue        = True
bucketname      = os.environ['bucketname']

#accessKeyId = raw_input('Enter the AWS access key: ')
#secretAccessKey = raw_input ('Enter the AWS secret key: ')
#tlsValue = True
#bucketname = raw_input ('Enter the S3 bucket name: ')

#--- function for writing blob data into FILE #
def write_file(data, filename):
    with open(filename, 'wb') as f:
        f.write(data)
args = sys.argv

#-- check if required attribute is passed----
if ( len(args) >=2 ):
    table = args[1]
else:
    print('Required Attribute table missing')
    sys.exit()

if( len(args) >=3 ):
    fromdate = args[2]
else:
    print('Required Attribute From date missing')
    sys.exit()

if ( len(args) >=4 ):
    todate = args[3]
else:
    print('Required Attribute To Date missing')
    sys.exit()


connection = cx_Oracle.connect(dbUsername+'/'+password+'@'+host+':'+port+'/'+sid)
cursor = connection.cursor()

#----------------------1st table ------------------#
if ( table == 'VANKIOSKMEDIA'):
    print('---------------table1 start----------')
    # getting data between dates #
    querystring = "select * from VANKIOSKMEDIA WHERE DATESTAMP BETWEEN TO_DATE('"+fromdate+"','YYYY/MM/DD') AND TO_DATE('"+todate+"','YYYY/MM/DD') AND CUSTOMERIMGURL is null"
    cursor.execute(querystring)
    data = cursor.fetchall()
    for row in data :
        customerId = row[0]
        customerImgId = row[2]
        datestamp = row[1]
	blobFile = row[3]
        ID = row[5]
	query = "select VANSTOREID FROM VANCUSTOMERENGAGEMENT WHERE CUSTOMERID = "+str(customerId)+" AND ROWNUM =1"
	cursor.execute(query)
	stores = cursor.fetchone()
	storeId = stores[0]

	#print(str(customerId)+'-'+str(customerImgId)+'-'+str(datestamp)+'-'+str(ID)+' staff id '+'='+str(storeId))
        randomInteger = randint(100000,999999)
        filename = str(randomInteger)+'_'+str(customerId)+'_'+str(storeId)+'_'+str(customerImgId)+'.png'
        if not blobFile:	
		print('blob data not found for customerid = '+str(row[0]))
	else:
	   #write file into path
           blobData = blobFile.read()
           write_file(blobData,filename)
           path = staticPath+filename

           #uploading file to s3
           conn = tinys3.Connection(accessKeyId,secretAccessKey,tls=tlsValue)
           f = open(path,'rb')
           conn.upload(filename,f,bucketname)
	   os.remove(filename)
	   
	   #updating database
          # querystring = "UPDATE VANKIOSKMEDIA set CUSTOMERIMGURL ='"+filename+"' WHERE CUSTOMERID = '"+str(customerId)+"' and to_char(DATESTAMP,'YYYY-MM-DD hh24:mi:ss') ='"+str(datestamp)+"' and CUSTOMERIMGID = '"+str(customerImgId) +"'"
           querystring = "UPDATE ODSDADM.VANKIOSKMEDIA set CUSTOMERIMGURL ='"+filename+"' WHERE CUSTOMERID = '"+str(customerId)+"' and ID ='"+str(ID)+"' and CUSTOMERIMGID = '"+str(customerImgId) +"'"           
           cursor.execute(querystring)
    print('-------------end for table1-----------')


#--------------------2nd table -----------------------#
if(table == 'VANBENEFITSPLUSSTORAGE' ):
    print('--------------table2 start-------------')
    querystring = "select * from VANBENEFITSPLUSSTORAGE  WHERE DATESTAMP BETWEEN TO_DATE('"+fromdate+"','YYYY/MM/DD') AND TO_DATE('"+todate+"','YYYY/MM/DD') AND BENEFITSPLUSUNSIGNEDURL is null AND BENEFITSPLUSSIGNEDURL is null"
    cursor.execute(querystring)

    #-- getting data for 2nd table #
    data = cursor.fetchall()
    for row in data:
	customerId   = row[0]
        unsignedBlob = row[1]
        signedBlob   = row[2]
	query = "select VANSTOREID FROM VANCUSTOMERENGAGEMENT WHERE CUSTOMERID = "+str(customerId)+" AND ROWNUM =1"
	cursor.execute(query)
	stores = cursor.fetchone()
	storeId = stores[0]
        randomInteger = randint(100000,999999)
        unsignedFilename = str(randomInteger)+'_'+str(customerId)+'_'+str(storeId)+'_docTemplate-unsigned.pdf'
        if not unsignedBlob:
		print('unsignedblob data not found for customerid = '+str( customerId ) )
	else:
	    #create file for 2nd table unsigned blob
            unsignedBlobData = unsignedBlob.read()
            write_file(unsignedBlobData,unsignedFilename)
            path = staticPath+unsignedFilename
	    
	    #uploading file for 2nd table unsigned blob
            conn = tinys3.Connection(accessKeyId,secretAccessKey,tls=tlsValue)
            f = open(path,'rb')
            conn.upload(unsignedFilename,f,bucketname)
	    os.remove(unsignedFilename)
	    
	    #updating url in database 
            querystring = "UPDATE VANBENEFITSPLUSSTORAGE set BENEFITSPLUSUNSIGNEDURL ='"+unsignedFilename+"' where CUSTOMERID = '"+str(customerId)+"'"
            cursor.execute(querystring)

        if not signedBlob:
		print('signedblob data not found for customerid = '+str( customerId ) )
	else:
	    #create file for 2nd table signed blob
            signedFilename = str(randomInteger)+'_'+str(customerId)+'_'+str(storeId)+'_docTemplate.pdf'
            signedBlobData = signedBlob.read()
            write_file(signedBlobData,signedFilename)
            path = staticPath+signedFilename
	    
	    #uploading file for 2nd table signed blob
            conn = tinys3.Connection(accessKeyId,secretAccessKey,tls=tlsValue)
            f = open(path,'rb')
            conn.upload(signedFilename,f,bucketname)
	    os.remove(signedFilename)

	    #updating url in database
            querystring = "UPDATE VANBENEFITSPLUSSTORAGE set BENEFITSPLUSSIGNEDURL ='"+signedFilename+"' where CUSTOMERID = '"+str(customerId)+"'"
            cursor.execute(querystring)
    print('-------------end for table2--------------')

#---------------3rd table -------------#
if(table == 'VANCUSTOMERAGREEMENT' ):
	print('--------------table3 start-------------')
	querystring = "select * from VANCUSTOMERAGREEMENT where DATESTAMP BETWEEN TO_DATE('"+fromdate+"','YYYY/MM/DD') AND TO_DATE('"+todate+"','YYYY/MM/DD') and  UNSIGNEDDOCURL  is null AND SIGNEDDOCURL  is null"
	cursor.execute(querystring)
	data = cursor.fetchall()
	for row in data:
	        engagementId = row[0]
		doctype      = row[1]
		unsignedDoc  = row[3]
		signedDoc    = row[4]
		query = "select VANSTOREID from VANCUSTOMERENGAGEMENT where ENGAGEMENTID = "+str(engagementId)+" AND ROWNUM =1"
		cursor.execute(query)		
		stores = cursor.fetchone()
                storeId      = stores[0]		
    		randomInteger = randint(100000,999999)
    		unsignedFilename = str(randomInteger)+'_'+str(engagementId)+'_'+str(storeId)+'_docTemplate-unsigned.pdf'
    		if not unsignedDoc:
			print('unsignedDoc Blob not found for engagement id = '+str(engagementId) )
		else:
        		unsignedBlobData = unsignedDoc.read()
        		write_file(unsignedBlobData,unsignedFilename)
			path = staticPath+unsignedFilename
            		conn = tinys3.Connection(accessKeyId,secretAccessKey,tls=tlsValue)
            		f = open(path,'rb')
            		conn.upload(unsignedFilename,f,bucketname)
			os.remove(unsignedFilename)
            		querystring = "UPDATE VANCUSTOMERAGREEMENT set UNSIGNEDDOCURL ='"+unsignedFilename+"' where ENGAGEMENTID = '"+str(engagementId)+"' AND DOCTYPE ='"+str(doctype)+"'"
            		cursor.execute(querystring)

    		if not signedDoc:	
			print('signedDoc Blob not found for engagement id = '+str(engagementId) )
		else:
        		signedFilename = str(randomInteger)+'_'+str(engagementId)+'_'+str(storeId)+'_docTemplate.pdf'
        		signedBlobData = signedDoc.read()
        		write_file(signedBlobData,signedFilename)
			path = staticPath+signedFilename
            		conn = tinys3.Connection(accessKeyId,secretAccessKey,tls=tlsValue)
            		f = open(path,'rb')
            		conn.upload(signedFilename,f,'racrdstestankurbucket')
			os.remove(signedFilename)
            		querystring = "UPDATE VANCUSTOMERAGREEMENT set SIGNEDDOCURL ='"+signedFilename+"' where ENGAGEMENTID = '"+str(engagementId)+"' AND DOCTYPE ='"+str(doctype)+"'"
            		cursor.execute(querystring)
	print('-------------end for table3--------------')

connection.commit()
cursor.close ()
sys.exit()
