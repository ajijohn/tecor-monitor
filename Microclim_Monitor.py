import base64
import os
import sched
import shutil
import time
import urllib.request
from datetime import date
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os.path import join, dirname

import boto3
import cdsapi
import xarray as xr
from dotenv import load_dotenv
from pymongo import MongoClient
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

inputdir = os.environ.get("INPUTDIR")
outputdir = os.environ.get("OUTPUTDIR")
emailfrom = os.environ.get("EMAILFROM")
s3bucket = os.environ.get("BUCKET")
awsregion = os.environ.get("AWSREGION")
smtp_provider = os.environ.get("SMTP_PROVIDER")
awsprofile = os.environ.get("AWS_PROFILE")
aerisclientid= os.environ.get("AERIS_CLIENT_ID")
aerisclientsecret=os.environ.get("AERIS_CLIENT_SECRET")

session = boto3.session.Session(profile_name=awsprofile,region_name=awsregion)
print(boto3.session.Session().available_profiles)

ses = session.client('ses')
s3 = session.client('s3')


s = sched.scheduler(time.time, time.sleep)


def function_cds(start_year, end_year, start_month, end_month, start_day, end_day, North, South, East, West, variables,
                 output_format, time):
    c = cdsapi.Client()
    d = c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'variable': variables,

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
            'format': output_format,
            'area': [
                North, West, South,
                East,
            ],
        },
    )
    return d


def check_new(sc):
    # print("Function starts")
    # look for new jobs
    # if exists, pick it, change the status
    today = date.today()
    print("Starting sweep on " + str(today.strftime('%m/%d/%Y %H:%M')))
    requests = db.requests

    error = True
    filesDownloaded = False

    request_lkup = requests.find_one({"status": "OPEN","version":"2"})
    # print(request_lkup)

    # copy the required files to local
    # update the status to say picked-up
    # invoke the script to call the ncl
    # verify the file exists in S3
    # send email out

    if (request_lkup is not None):

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
        if (noofyears == 0):
            # use the from year
            # initiate copy of all the files for the year for each requested variable
            years = [fromdate.year]
            # print(years)

        else:
            # get the years
            years = [i + fromdate.year for i in range(noofyears)]
            # print(years)

        # check if past or future
        if fromdate.year < datetime.now().year:
            timeperiod = 'past'
            # print(timeperiod)
        else:
            timeperiod = 'future'
            # print(timeperiod)

        # if the input work directory doesn't exist, create it
        if not os.path.exists(str(inputdir) + '/' + str(request_lkup['_id'])):
            os.makedirs(str(inputdir) + '/' + str(request_lkup['_id']))
            # print("Input directory created Successful")

        # fail to proceed, limit for only 2 years
        if (noofyears > 2):
            # print("Error")
            error = True
            retCode = 3
        else:
            # print("Error False")
            error = False

        # Ouput Directory is made with the request id
        if not (os.path.exists(str(outputdir) + '/' + str(request_lkup['_id']))):
            os.makedirs(str(outputdir) + '/' + str(request_lkup['_id']))

        # Parameters are extracted from the request
        start_day = request_lkup['startdate'][6:8]
        end_day = request_lkup['enddate'][6:8]
        start_month = request_lkup['startdate'][4:6]
        end_month = request_lkup['enddate'][4:6]
        start_year = request_lkup['startdate'][:4]
        end_year = request_lkup['enddate'][:4]
        South = request_lkup['lats'][0]
        North = request_lkup['lats'][1]
        West = request_lkup['longs'][0]
        East = request_lkup['longs'][1]
        interval = request_lkup['interval']
        output = request_lkup['outputformat']
        email = request_lkup['email']

        footer = '\n \n If any questions/issues, please report it on \
        our GitHub issues page - https://github.com/ajijohn/ebm/issues'

        try:

            if(smtp_provider=='SENDGRID'):
                # Sending the first mail with the parameters
                message = Mail(from_email=emailfrom,
                               to_emails=email,
                               subject='Microclim Request is submitted succesfully',
                               html_content='''Your request was submitted with paramaters <br><br>
                Start date : ''' + str(start_day) + " " + str(start_month) + " " + str(
                                   start_year) + '''<br> End date : ''' + str(end_day) + " " + str(
                                   end_month) + " " + str(end_year) +
                                            '''<br> Bounding Box :<br>   Latitude : ''' + str(
                                   North) + ", " + str(South) + '''<br>   Longitude : ''' + str(East) + ", " + str(
                                   West) + '''<br> Interval : ''' + str(interval) + '''<br> Output format : ''' + str(
                                   output))

                sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
                response = sg.send(message)
                print(response.status_code)
                print(response.body)
                print(response.headers)
            else:
                msg = MIMEMultipart()
                request_text = "Your request was submitted with paramaters \n \
                               Start date : " + str(start_day) + " " + str(start_month) + " " +\
                str(start_year) + '''\n End date : ''' + str(end_day) + " " + str(end_month) + \
                 " " + str(end_year) +\
                     "\n Bounding Box :\n   Latitude :" + str(\
                    North) + ", " + str(South) + '''\n  Longitude : ''' + str(East) +\
                               ", " + str(West) + '''\n Interval : ''' + str(interval) + \
                '''\nOutput format : ''' + str(output) +"\n"
                # the message body
                part = MIMEText(request_text  + footer)
                msg.attach(part)

                msg['Subject'] = 'Your Microclim extract request- ' + str(request_lkup['_id']) + ' was submitted successfully.'
                msg['From'] = emailfrom
                # Temporary
                msg['To'] = email
                #msg['To'] = emailfrom
                result = ses.send_raw_email(
                    Source=msg['From'],
                    Destinations=[request_lkup['email']],
                    #Destinations=[emailfrom],
                    RawMessage={'Data': msg.as_string()}
                )


        except Exception as e:
            print(e)

        # If the source type is aeris weather
        if request_lkup['sourcetype'] == 'aeris':
            # variables needed for the aeries request is extracted
            North = request_lkup['lats'][0]
            East = request_lkup['lats'][1]
            South = request_lkup['longs'][0]
            West = request_lkup['longs'][1]
            email = request_lkup['email']
            start_day = request_lkup['startdate'][6:8]
            start_month = request_lkup['startdate'][4:6]
            start_year = request_lkup['startdate'][:4]
            end_day = request_lkup['enddate'][6:8]
            end_month = request_lkup['enddate'][4:6]
            end_year = request_lkup['enddate'][:4]
            variable = request_lkup['variable']

            location = str(North) + ',' + str(South) + ',' + str(East) + ',' + str(West)
            start_date = str(start_month) + '/' + str(start_day) + '/' + str(start_year)
            end_date = str(end_month) + '/' + str(end_day) + '/' + str(end_year)

            v1 = ' '
            v2 = ' '
            v3 = ' '
            v5 = ' '
            result_aeris = ' '

            for i in variable:
                if i == 'Temperature':
                    v1 = 'periods.ob.tempC,'
                if i == 'Wind Speed':
                    v2 = 'periods.ob.windSpeedKPH,'
                if i == 'Wind Direction':
                    v3 = 'periods.ob.windDir,'
                if i == 'Solar Radiation':
                    v5 = 'periods.ob.solradWM2'
            v4 = v1 + v2 + v3 + v5

            # Aeris weather
            request = urllib.request.urlopen(
                'https://api.aerisapi.com/observations/archive/within?p=' +
                location + '&from=' + start_date + '&to=' +
                end_date + '&format=json&filter=allstations&limit=100&plimit=10&fields=' +
                v4.strip() + '&client_id=' +aerisclientid+ '&client_secret=' +aerisclientsecret)

            response = request.read()
            import json
            json = json.loads(response)
            if json['success']:
                print(json)
                result_aeris = json
            else:
                print("An error occurred: %s" % (json['error']['description']))
                request.close()

            # Output directory is given as the path
            os.chdir(str(outputdir) + '/' + str(request_lkup['_id']))
            path2 = os.getcwd()

            # Into the text file we are appending the result of aeris
            with open("myfile.txt", "w") as file_2:
                file_2.write("Aeries weather\n")
            file_2.close()
            with open("myfile.txt", "a") as file_2:
                file_2.write("\n" + str(result_aeris))
            file_2.close()

            x = os.listdir(path2)
            file_name = x[0]
            file_path = os.path.join(path2, file_name)

            if (smtp_provider == 'SENDGRID'):
               # SendGrid Mail is sent with the result and text file attachment
               message = Mail(from_email=emailfrom, to_emails=email, subject='Microclim.org',
                           html_content='Your Request with Microclim.org was successful.<br> The result is attached below.')
               with open(file_path, 'rb') as f:
                   data = f.read()
               f.close()
               encoded_file = base64.b64encode(data).decode()
               attachment = Attachment()
               attachment.file_content = FileContent(encoded_file)
               attachment.file_name = FileName('Microclim.')
               attachment.disposition = Disposition('attachment')
               message.attachment = attachment
               try:
                   sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
                   response = sg.send(message)
                   print(response.status_code)
                   print(response.body)
                   print(response.headers)
               except Exception as e:
                   print(e)
            else:
                transitdirectory = outputdir + '/' + str(request_lkup['_id'])

                # copy all the created files
                filestosend = [f for f in os.listdir(transitdirectory) if os.path.isfile(
                    os.path.join(transitdirectory, f))]

                fileurls = []
                emailbodyurl = '\n'
                footer = '\n \n If any questions/issues, please report it on our GitHub\
                                         issues page - https://github.com/ajijohn/ebm/issues.'

                for file in filestosend:
                    # Create file on S3 and setup the permissions - boto3
                    with open(transitdirectory + '/' + file, "rb") as f:
                        s3.upload_fileobj(f, s3bucket, '/' + str(request_lkup['_id']) + '/' + file,
                                          ExtraArgs={"Metadata": {"Content-Type": "text/plain"}, 'ACL': 'public-read'})

                        # 2 days expiry
                        # url = key.generate_url(expires_in=172800, query_auth=False, force_http=True)
                        # boto3
                        url = s3.generate_presigned_url(
                            ClientMethod='get_object',
                            Params={
                                'Bucket': s3bucket,
                                'Key': '/' + str(request_lkup['_id']) + '/' + file
                            },
                            ExpiresIn=172800)

                        fileurls.append(url)
                        emailbodyurl = emailbodyurl + "\n" + url

                msg = MIMEMultipart()
                # the message body
                part = MIMEText(
                    request_text + "You can access your files which are linked below \n " + emailbodyurl + '\n' + footer)
                msg.attach(part)

                msg['Subject'] = 'Your Microclim extract request-' + str(request_lkup['_id']) + ' has completed'
                msg['From'] = emailfrom
                msg['To'] = request_lkup['email']
                # msg['To'] = emailfrom

                # what a recipient sees if they don't use an email reader
                msg.preamble = 'Your extract request-' + \
                               str(request_lkup['_id']) + \
                               ' has completed. Links are below \n' + \
                               emailbodyurl + '\n'

                result = ses.send_raw_email(
                    Source=msg['From'],
                    Destinations=[request_lkup['email']],
                    #Destinations=[emailfrom],
                    RawMessage={'Data': msg.as_string()}
                )

                filesDownloaded = True

            # If the source is ERA5
        if request_lkup['sourcetype'] == 'ERA5':

            # Parameters needed for ERA5 are extracted
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
            if output == 'netcdf' or output == 'csv':
                output_format = 'netcdf'
            if output == 'GRIB':
                output_format = output
            if interval == 'Daily':
                time = ['00:00']
            elif interval == '6 Hourly':
                time = ['00:00', '06:00', '12:00', '18:00']
            elif interval == '12 Hourly':
                time = ['00:00', '12:00']
            elif interval == 'Hourly':
                time = ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00', '07:00', '08:00', '09:00',
                        '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00',
                        '20:00', '21:00', '22:00', '23:00']
            variables = request_lkup['variable']

            # Function call for function_cds
            result = function_cds(start_year, end_year, start_month, end_month, start_day, end_day, North, South, East,
                                  West, variables, output_format, time)
            # print("Result :",result)

            # Changing the path to the output directory
            os.chdir(str(outputdir) + '/' + str(request_lkup['_id']))
            path = os.getcwd()
            print('Getwd', path)

            # The result is written onto the textfile and the result is downloaded in the output directory
            with open("myfile.txt", "w") as file1:
                file1.write(result.download())
            file1.close()
            x = os.listdir(path)
            file_name=""
            file_path=""
            for file in os.listdir(path):
                if file.endswith(".nc"):
                    print(os.path.join(path, file))
                    file_name = file
                    file_path = os.path.join(path, file_name)

            if output == 'netcdf' or output == 'GRIB':
                if output == 'netcdf':
                    ext = '.nc'
                if output == 'GRIB':
                    ext = '.grib'

                send_file_name = 'Microclim' + ext

                try:
                    if (smtp_provider == 'SENDGRID'):
                        # Sendgrid mail for output formats such as netCDF and GRIB
                        message = Mail(from_email=emailfrom, to_emails=email,
                                       subject='Microclim.org',
                                       html_content='Your Request with Microclim.org \
                                       is successful.<br><br> The result is attached below.<br><br>' + str(
                                           result))
                        with open(file_path, 'rb') as f:
                            data = f.read()
                            f.close()
                        encoded_file = base64.b64encode(data).decode()
                        attachment = Attachment()
                        attachment.file_content = FileContent(encoded_file)
                        attachment.file_name = FileName(send_file_name)
                        attachment.disposition = Disposition('attachment')
                        message.attachment = attachment

                        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
                        response = sg.send(message)
                        print(response.status_code)
                        print(response.body)
                        print(response.headers)

                        filesDownloaded = True
                    else:

                        transitdirectory = outputdir + '/' + str(request_lkup['_id'])

                        # copy all the created files
                        filestosend = [f for f in os.listdir(transitdirectory) if os.path.isfile(
                            os.path.join(transitdirectory, f))]

                        fileurls = []
                        emailbodyurl = '\n'
                        footer = '\n \n If any questions/issues, please report it on our GitHub\
                         issues page - https://github.com/ajijohn/ebm/issues.'


                        for file in filestosend:
                            # Create file on S3 and setup the permissions - boto3
                            with open(transitdirectory + '/' + file, "rb") as f:
                                s3.upload_fileobj(f, s3bucket, '/' + str(request_lkup['_id']) + '/' + file,
                                  ExtraArgs = {"Metadata": {"Content-Type": "text/plain"}, 'ACL': 'public-read'})

                                # 2 days expiry
                                # url = key.generate_url(expires_in=172800, query_auth=False, force_http=True)
                                # boto3
                                url = s3.generate_presigned_url(
                                ClientMethod = 'get_object',
                                Params = {
                                             'Bucket': s3bucket,
                                             'Key': '/' + str(request_lkup['_id']) + '/' + file
                                         },
                                ExpiresIn = 172800)

                                fileurls.append(url)
                                emailbodyurl = emailbodyurl + "\n" + url

                        msg = MIMEMultipart()
                        # the message body
                        part = MIMEText(
                            request_text + "You can access your files which are linked below \n " + emailbodyurl + '\n' + footer)
                        msg.attach(part)

                        msg['Subject'] = 'Your extract request-' + str(request_lkup['_id']) + ' has completed'
                        msg['From'] = emailfrom
                        msg['To'] = request_lkup['email']
                        #msg['To'] = emailfrom

                        # what a recipient sees if they don't use an email reader
                        msg.preamble = 'Your extract request-' + \
                                       str(request_lkup['_id']) +\
                                       ' has completed. Links are below \n' + \
                                       emailbodyurl + '\n'

                        result = ses.send_raw_email(
                            Source=msg['From'],
                            # Destinations=[request_lkup['email']],
                            Destinations=[emailfrom],
                            RawMessage={'Data': msg.as_string()}
                        )

                        filesDownloaded = True

                except Exception as e:
                    print(e)
                    # Updating the status of the request to Emailed
                    requests.update_one({
                    '_id': request_lkup['_id']
                            }, {
                            '$set': {
                        'status': "ERRORED"
                        }
                        }, upsert=False)

                # TODO
                # Check if the update actually occured

            # If the output format is csv
            if output == 'csv':

                # input file [netCDF] directory and output file[csv] path is given here
                netcdf_file_name = file_name
                netcdf_file_in = path + '/' + file_name
                csv_file_out = path + '/' + netcdf_file_name[:-3] + '.csv'

                # function for file_conversion netCDF to csv
                ds = xr.open_dataset(netcdf_file_in)
                df = ds.to_dataframe()
                df.to_csv(csv_file_out)
                file_name = netcdf_file_name[1:-3] + '.csv'

                emailbodyurl = 'Link to the files are below :-'

                try:
                    if (smtp_provider == 'SENDGRID'):
                        # Sendgrid Mail for output format csv
                        message = Mail(from_email=emailfrom,
                                    to_emails=email, subject='Microclim.org',
                                   html_content='Your Request with Microclim.org is \
                                   successful.<br><br> The result is attached below.<br><br>' + str(\
                                       result))
                        with open(csv_file_out, 'rb') as f:
                           data = f.read()
                           f.close()
                        encoded_file = base64.b64encode(data).decode()
                        attachment = Attachment()
                        attachment.file_content = FileContent(encoded_file)
                        attachment.file_name = FileName('Microclim.csv')
                        attachment.disposition = Disposition('attachment')
                        message.attachment = attachment

                        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
                        response = sg.send(message)
                        print(response.status_code)
                        print(response.body)
                        print(response.headers)

                        filesDownloaded = True
                    else:
                        transitdirectory = outputdir + '/' + str(request_lkup['_id'])

                        # copy all the created files
                        filestosend = [f for f in os.listdir(transitdirectory) if os.path.isfile(
                        os.path.join(transitdirectory, f))]

                        footer = '\n \n If any questions/issues, please report it on our \
                            GitHub issues page - https://github.com/ajijohn/ebm/issues.'



                        for file in filestosend:
                              # Create file on S3 and setup the permissions - boto3
                              # Create file on S3 and setup the permissions - boto3
                              with open(transitdirectory + '/' + file, "rb") as f:
                                  s3.upload_fileobj(f, s3bucket, '/' + str(request_lkup['_id']) + '/' + file,
                                                    ExtraArgs={"Metadata": {"Content-Type": "text/plain"},
                                                               'ACL': 'public-read'})

                                  # 2 days expiry
                                  # url = key.generate_url(expires_in=172800, query_auth=False, force_http=True)
                                  # boto3
                                  url = s3.generate_presigned_url(
                                      ClientMethod='get_object',
                                      Params={
                                          'Bucket': s3bucket,
                                          'Key': '/' + str(request_lkup['_id']) + '/' + file
                                      },
                                      ExpiresIn=172800)

                                  emailbodyurl = emailbodyurl + "\n" + url



                        msg = MIMEMultipart()
                        # the message body
                        part = MIMEText(
                            request_text + "You can access your files which are attached below \n " + emailbodyurl + "\n" + footer)
                        msg.attach(part)

                        msg['Subject'] = 'Your extract request-' + str(request_lkup['_id']) + ' has completed'
                        msg['From'] = emailfrom
                        msg['To'] = request_lkup['email']
                        #msg['To'] = emailfrom

                        # what a recipient sees if they don't use an email reader
                        msg.preamble = 'Your extract request-' + str(request_lkup['_id']) + ' has completed.\n'


                        result = ses.send_raw_email(
                            Source=msg['From'],
                            Destinations=[request_lkup['email']],
                            #Destinations=[emailfrom],
                            RawMessage={'Data': msg.as_string()}
                            )

                        filesDownloaded = True
                except Exception as e:
                    print(e)
                    # Updating the status of the request to emailed
                    requests.update_one({
                    '_id': request_lkup['_id']
                     }, {
                    '$set': {
                        'status': "ERRORED"
                    }
                     }, upsert=False)



    # Remove the files from ebinput/{requestid}
    if filesDownloaded:
        # Updating the status of the request to emailed
        requests.update_one({
            '_id': request_lkup['_id']
        }, {
            '$set': {
                'status': "EMAILED"
            }
        }, upsert=False)

        if os.path.isdir(inputdir + '/' + str(request_lkup['_id'])):
            shutil.rmtree(inputdir + '/' + str(request_lkup['_id']))
        if os.path.isdir(outputdir + '/' + str(request_lkup['_id'])):
            shutil.rmtree(outputdir + '/' + str(request_lkup['_id']))

    print("Completed sweep on " + str(today.strftime('%m/%d/%Y %H:%M')))

    # Resechedule
    s.enter(60, 1, check_new, (sc,))


if __name__ == '__main__':
    # Initialize with DB Context
    db = MongoClient()['ebm']
    # test()
    # Check every minute
    s.enter(60, 1, check_new, (s,))
    s.run()
