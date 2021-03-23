import os
import sched
import time
from datetime import date
from datetime import datetime
from enum import Enum
from os.path import join, dirname
from string import Template
import shutil
from dotenv import load_dotenv
from pymongo import MongoClient
import sys
import cdsapi
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import sendgrid
from sendgrid.helpers.mail import *

#dotenv_path = join(dirname(__file__), '.env')
#load_dotenv(dotenv_path)
s3bucket = os.environ.get("BUCKET")
awsregion = os.environ.get("AWSREGION")
inputdir = os.environ.get("INPUTDIR")
outputdir = os.environ.get("OUTPUTDIR")
s = sched.scheduler(time.time, time.sleep)
#print(outputdir)

def function_cds(start_year,end_year,start_month,end_month,start_day,end_day,North,South,East,West,variable,output,time):
    c = cdsapi.Client()
    d = c.retrieve(
        'reanalysis-era5-single-levels',
    {
        'product_type': 'reanalysis',
        'variable': variable,
                    
        'year': [
            start_year,
            end_year
        ],
        'month': [
            start_month,
            end_month
        ],
        'day': [
            start_day,
            end_day
        ],
        'time': time,
        'format': output,
        'area': [
            North, West, South,
            East,
        ],
    },
    )
    return d

def check_new(sc):
    #print("Function starts")
    # look for new jobs
    # if exists, pick it, change the status
    today = date.today()
    print("Starting sweep on " + str(today.strftime('%m/%d/%Y %H:%M')))
    requests = db.requests

    error = True
    filesDownloaded = False

    request_lkup = requests.find_one({"status": "OPEN"})
    #print(request_lkup)

    # copy the required files to local
    # update the status to say picked-up
    # invoke the script to call the ncl
    # verify the file exists in S3
    # send email out

    if(request_lkup is not None):

        # iterate thru variables to get the file names
        # for variable in request_lkup['variable']:
        # find the distinct years in the range
        # dates are in format - "19810131"
        enddate = datetime.strptime(request_lkup['enddate'], '%Y%m%d')
        fromdate = datetime.strptime(request_lkup['startdate'], '%Y%m%d')
        noofyears = enddate.year - fromdate.year

        # set the time period
        timeperiod = ''
        years = []

        # Requested for same year
        if(noofyears == 0):
            # use the from year
            # initiate copy of all the files for the year for each requested variable
            years = [fromdate.year]
            #print(years)

        else:
            # get the years
            years = [i+fromdate.year for i in range(noofyears)]
            #print(years)

        # check if past or future
        if fromdate.year < datetime.now().year:
            timeperiod = 'past'
            #print(timeperiod)
        else:
            timeperiod = 'future'
            #print(timeperiod)

        # if the input work directory doesn't exist, create it
        if not os.path.exists(str(inputdir) + '/' + str(request_lkup['_id'])):
                os.makedirs(str(inputdir) + '/' + str(request_lkup['_id']))
                #print("Input directory created Successful")

        # fail to proceed, limit for only 2 years
        if(noofyears > 2):
            #print("Error")
            error = True
            retCode = 3
        else:
            #print("Error False")
            error = False

        # Form the request string
        requeststring = Template(
                'Your request was submitted with parameters -  start date $startdate, enddate $enddate,' +
                ' bounding box ($lats,$latn) ($lonw,$lone), ' +
                ' shade level $shadelevel, height $hod, interval $interval and aggregation metric $aggregation.\n')
        request_text = requeststring.safe_substitute(startdate=request_lkup['startdate'],
                                                         enddate=request_lkup['enddate'],
                                                         lats=request_lkup['lats'][0], latn=request_lkup['lats'][1],
                                                         lonw=request_lkup['longs'][0], lone=request_lkup['longs'][1],
                                                         shadelevel=request_lkup['shadelevel'], hod=request_lkup['hod'],
                                                         interval=request_lkup['interval'],
                                                         aggregation=request_lkup['aggregation'])

        if not(os.path.exists(str(outputdir) + '/' + str(request_lkup['_id']))):
            os.makedirs(str(outputdir) + '/' + str(request_lkup['_id']))
        

        
        if request_lkup['sourcetype'] == 'ERA5':
            start_day = request_lkup['enddate'][6:8]
            end_day = request_lkup['startdate'][6:8]
            start_month = request_lkup['startdate'][4:6]
            end_month = request_lkup['enddate'][4:6]
            start_year = request_lkup['startdate'][:4]
            end_year = request_lkup['enddate'][:4]
            North = request_lkup['lats'][0]
            South = request_lkup['lats'][1]
            East = request_lkup['longs'][0]
            West = request_lkup['longs'][1]
            interval = request_lkup['interval']
            output = request_lkup['outputformat']
            email = request_lkup['email']
            
            if interval == 'Daily':
                time = ['00:00']
            elif interval == '6 Hourly':
                time = ['00:00','06:00','12:00','18:00']
            elif interval == '12 Hourly':
                time = ['00:00','12:00']
            elif interval == 'Hourly':
                time = ['00:00','01:00','02:00','03:00','04:00','05:00','06:00','07:00','08:00','09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00','17:00','18:00','19:00','20:00','21:00','22:00','23:00']
    
            variables = request_lkup['variable'] 
            variable = []
            dict_variable = {'Tair':'2m_temperature','Tsurface':'skin_temperature','Tsoil':'soil_temperature_level_1','SMOIS':'volumetric_soil_water_layer_1'}
            for i in variables:
                variable.append(dict_variable[i])
        
            result = function_cds(start_year,end_year,start_month,end_month,start_day,end_day,North,South,East,West,variable,output,time)
            print("Result :",result)
            os.chdir(str(outputdir) + '/' + str(request_lkup['_id']))
            #path = os.getcwd()
            #print('Getwd',path)
            with open("myfile.txt", "w") as file1: 
                file1.write(result.download())
            file1.close()

            #Send Grid Mail
            message = Mail(from_email='devteam-noreply@hashdev.in',to_emails=email,subject='Microclim.org',html_content= str(result))
            
            attachedFile = Attachment(
                FileContent(result.download()),
                FileName('Microclim.org'),
                FileType('application/pdf'),
                Disposition('attachment')
            )
            message.attachment = attachedFile

            try:
                sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
                response = sg.send(message)
                print(response.status_code)
                print(response.body)
                print(response.headers)   
            except Exception as e:
                print(e)

if __name__ == '__main__':
    # Initialize with DB Context
    db=MongoClient()['ebm']
    # test()
    # Check every minute
    s.enter(60, 1, check_new, (s,))
    s.run()
