__author__ = 'Aji John'
# ==============================================================================
# Main monitor thread to sweep the outstanding requests
# Revised: 2016-09-10
__version__ = '0.0.1'
# ==============================================================================

from datetime import date
from pymongo import MongoClient
import SES
import pyncl
import sched, time
#from boto3 import s3

from boto.s3.connection import OrdinaryCallingFormat
from boto import s3

import os
from os.path import join, dirname
from dotenv import load_dotenv


dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
s3bucket = os.environ.get("BUCKET")
awsregion = os.environ.get("AWSREGION")
inputdir = os.environ.get("INPUTDIR")
outputdir = os.environ.get("OUTPUTDIR")

s = sched.scheduler(time.time, time.sleep)

'''
Extract Request

{"email": "x.y@gmail.com",
 "text": "Request for extract",
 "lats": ["30", "43"],
 "longs":["-125", "-113"],
 "variable":["Tsurface"],
 "startdate":"19810102",
 "enddate":"19810102",
 "interval":"",
 "aggregationmetric":"",
  "outputformat":"csv",
  "timelogged":"",
  "status":"OPEN",
  "misc":""
}
'''
def check_new(sc):
    # look for new jobs
    # if exists, pick it, change the status
    #
    today = date.today()
    print("Starting sweep on " + str(today.strftime('%m/%d/%Y %H:%M')))
    requests = db.requests

    request_lkup = requests.find_one({"status": "OPEN"})
    if(request_lkup is not None):
        # update the status to say picked-up
        # invoke the script to call the ncl
        # verify the file exists in S3
        # send email out

        #if the work directory doesn't exist create it
        if not os.path.exists(outputdir + '/' + str(request_lkup['_id'])):
            os.makedirs(outputdir + '/' + str(request_lkup['_id']))

        #Iterate through all the variables
        for variable in request_lkup['variable']:
            #lat is LatS, LatN
            #lon is LonW, LonE
            pyncl.RunNCLV2.withvar(inputdir,outputdir + '/' + str(request_lkup['_id']), request_lkup['startdate'],
                             request_lkup['enddate'],
                             request_lkup['lats'][0],
                             request_lkup['lats'][1],
                             request_lkup['longs'][0],
                             request_lkup['longs'][1],
                             variable,
                             #0, 0,0,0,1)
                             request_lkup['shadelevel'],
                             request_lkup['hod'],
                             request_lkup['interval'],
                             request_lkup['aggregation'],
                             1)

        #Last set of variables shade, height,interval,aggregation,output

        c = s3.connect_to_region(awsregion,calling_format=OrdinaryCallingFormat())
        bucket = c.get_bucket(s3bucket, validate=False)

        #key = bucket.new_key('/' + str(request_lkup['_id']) +'/extract.txt')
        #key.set_contents_from_filename(outputdir+ '/d02.txt')

        transitdirectory = outputdir + '/' + str(request_lkup['_id'])

        #copy all the created files
        filestosend = [f for f in os.listdir(transitdirectory) if os.path.isfile(os.path.join(transitdirectory, f))]

        for file in filestosend:
            key = bucket.new_key('/' + str(request_lkup['_id']) + '/' + file)
            key.set_contents_from_filename(transitdirectory + '/' + file)
            key.set_metadata('Content-Type', 'text/plain')
            key.set_acl('public-read')


        #2 days expiry
        url = key.generate_url(expires_in=172800, query_auth=False, force_http=True)

        SES.send_ses(awsregion,'requests@microclim.org', 'Your extract request-' +
                     str(request_lkup['_id'])  +  ' has completed',
                     "You can access your file here\n " + url, request_lkup['email'])

        requests.update_one({
            '_id': request_lkup['_id']
                },{
            '$set': {
                'status': "EMAILED"
                }
            }, upsert=False)
        #TODO
        #Check if the update actually occured

        print("Processed request id - " + str(request_lkup['_id']) + " from " + str(request_lkup['email']) )


    print("Completed sweep on " + str(today.strftime('%m/%d/%Y %H:%M')) )

    # Resechedule
    s.enter(60, 1, check_new, (sc,))

def test():
    requests = db.requests


    request= {"email": "aji.john@xyz.com",
        "text": "Request for extract",
        "lats": ["30", "43"],
        "longs":["-125", "-113"],
        "variable":["Tsurface"],
        "startdate":"19810102",
        "enddate":"19810131",
        "interval":"",
        "aggregationmetric":"",
        "outputformat":"csv",
        "timelogged":"",
        "status":"OPEN",
        "misc":""}
    request_id = requests.insert_one(request).inserted_id
    print(request_id)


if __name__ == '__main__':
    # Initialize with DB Context
    db = MongoClient()['ebm']
    #test()
    #Check every minute
    s.enter(60, 1, check_new, (s,))
    s.run()

