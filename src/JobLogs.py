import mysql.connector
import sys
import json
class JobAuditing:

    def __init__(self,job_name):
        self.job_name = job_name

        try:
            with open("config.json",'r') as configfile:
                self.configJsonObj = json.load(configfile)
                self.tblJobLoad = self.configJsonObj["TBL_JobLoadStatus"]
                self.tblBatchLoad = self.configJsonObj["TBL_BatchLoadStatus"]
                self.tblDataQualityStats = self.configJsonObj["TBL_DataQualityStats"]
                self.schema=self.configJsonObj["schema"]
                self.mydb = mysql.connector.connect(
                    host= self.configJsonObj["db"]["host"],
                    user=self.configJsonObj["db"]["user"],
                    password=self.configJsonObj["db"]["password"]
                )
        except mysql.connector.Error as err:
            print('Connection Unsuccessful reason - ',err.msg)
            sys.exit(1)
        except FileNotFoundError as err:
            print("Json Configuration file not found - reason -",err.msg)
            sys.exit(1)
        self.batch_id = self.getCurrentBatchId()
        if job_name == 'START':
            self.initiateBatchId()
        elif job_name == 'END':
            self.endBatchId()

a
    def endBatchId(self):
        runningBatchId = self.getCurrentBatchId()
        if runningBatchId == None:
            pass
        else:
            print(runningBatchId)
            endBatchIdCursor = self.mydb.cursor()
            query = "UPDATE "+self.schema + "." + self.tblBatchLoad + " SET EndTimestamp = NOW(), STATUS = 'C' WHERE BatchId =" +runningBatchId
            print(query)
            endBatchIdCursor.execute(query)
            self.mydb.commit()

    def validatePrevBatch(self,maxBatchId,statusOfPrevRuns):
        if statusOfPrevRuns == 'F' :
            print("Previous Batch did not complete correctly")
            sys.exit(1)
        elif statusOfPrevRuns == 'R':
            print("Previous Batch is currently Running ")
            sys.exit(1)

    def initiateBatchId(self):
        cursor = self.mydb.cursor()
        insCursor = self.mydb.cursor()
        try:
            query = "select b.bid, status from " + self.schema + "." +self.tblBatchLoad + \
                           " a INNER JOIN ( SELECT MAX(BATCHID) bid FROM "+self.schema + "."+self.tblBatchLoad + \
                           " )b on b.bid = a.BatchId"
            print(query)
            cursor.execute(query)
            res = cursor.fetchone()
            print(res)
            maxBatchId = res

            print("No of Rows fetch from BatchLoad Status ", maxBatchId)

            if maxBatchId == None:
                query = "INSERT INTO " + self.schema + "."+self.tblBatchLoad + " (BatchId,Status,StartTimestamp) values ('1','R',NOW() )"
                print("Query Executing - \n ", query)
                insCursor.execute(query)
                self.mydb.commit()
            else:
                maxBatchId = res[0]
                statusOfPrevRuns = res[1]
                self.validatePrevBatch(maxBatchId, statusOfPrevRuns)
                newBatchId = int(maxBatchId) + 1
                query = "INSERT INTO "+ self.schema + "." + self.tblBatchLoad + \
                        "(BatchId, Status, StartTimestamp) values ( '" + str(newBatchId) + "', 'R' , NOW() )"
                print("Query Executing - \n ", query)
                insCursor.execute(query)
                self.mydb.commit()
            insCursor.close()
            cursor.close()
        except mysql.connector.Error as err:
            print("Error in Query to BatchLoadTbl, reason -"+err.msg)
            sys.exit(1)


    def getCurrentBatchId(self):
        query = "SELECT MAX(BATCHID) FROM "+ self.schema + "." +self.tblBatchLoad + " WHERE STATUS = 'R'"
        fetchBatchIdCursor = self.mydb.cursor()
        fetchBatchIdCursor.execute(query)
        batchid = fetchBatchIdCursor.fetchone()[0]
        return batchid


    def insertJob(self,recordCount):
        if ((self.job_name == 'START') | (self.job_name == 'END')):
            raise Exception('START and END job cannot be Inserted in Job Logs')
#Check if Job Name executed for that Batch
        tableName = self.schema + "."+self.tblJobLoad
        try:
            cur_fetch_job_name = self.mydb.cursor()
            query = "select table_name, Batch_id from "+ tableName + " where Batch_Id = '"+self.batch_id +"' and table_name = '"+ self.job_name +"'"
            print("Job Name executed ",query)
            cur_fetch_job_name.execute(query)
            for val in cur_fetch_job_name:
                print(val)
        except mysql.connector.Error as err:
            print("Phat gaya",err.msg)
            sys.exit(1)
        try:
            cur_insert_jobs = self.mydb.cursor()
            query = "INSERT INTO audit.dataloadprocess values(" + self.batch_id + \
                  "," + "'" + self.job_name + "'" + ", '' , 100, 'DAG_START_JOB', \
            '',current_date(),'S', current_date(),current_date(),current_date())"
            cur_insert_jobs.execute(query)
            self.mydb.commit()
            cur_insert_jobs.close()

        except mysql.connector.Error as err:
            print("Something went wrong with Query reason - ",err.msg)
            sys.exit(1)
        # for x in cursor:
        #     print(x)

if __name__== '__main__':
    audit = JobAuditing('END')
    print('BatchId is - ',audit.batch_id)
 #   audit.insertJob(100)
