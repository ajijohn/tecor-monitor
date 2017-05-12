# ==============================================================================
# Author: Feng Zhu
# Adapted by: Aji John
# Date: 2015-01-26 12:23:30
# Revised: 2016-09-10
__version__ = '0.0.1'
# ==============================================================================

import os
import subprocess
import sys

work_dir = os.getcwd()
tmp_dir = os.path.join(work_dir, '.pyncl')

if not os.path.isdir(tmp_dir):
    os.mkdir(tmp_dir)


class NCL:

    def create(file_path, ncl_code):
        ncl_file = open(file_path, 'w')
        ncl_file.write(ncl_code)
        ncl_file.close()

    def run(file_path,inputdir,outputdir, startdate, enddate,latS,latN,lonW,lonE ,varname):
        retcode = subprocess.call('ncl ' +
                                  'startdate=' + startdate+
                                  ' enddate=' + enddate+
                                  ' latS=' + latS+
                                  ' latN=' + latN+
                                  ' lonW=' + lonW+
                                  ' lonE=' + lonE+
                                  ' \'varname="' + varname+
                                  '"\' '+
                                  ' \'INPUTDIR="' + inputdir+
                                  '"\' '+
                                  ' \'OUTPUTDIR="' + outputdir+
                                  '"\' '+
                                  file_path, shell=True)
        if retcode < 0:
            print("Child was terminated by signal", -retcode, file=sys.stderr)
        else:
            print("Child returned", retcode, file=sys.stderr)

    #shade=0 height=0 interval=0 aggregation=0 output=0

    def runV2(file_path,inputdir,outputdir, startdate, enddate,latS,latN,lonW,lonE ,varname,shade, height,interval,aggregation,output):
        returnCode=0

        try:
            retcode = subprocess.call('ncl ' +
                                  'startdate=' + startdate+
                                  ' enddate=' + enddate+
                                  ' latS=' + latS+
                                  ' latN=' + latN+
                                  ' lonW=' + lonW+
                                  ' lonE=' + lonE+
                                  ' shade=' + str(shade)+
                                  ' height=' + str(height)+
                                  ' interval=' + str(interval)+
                                  ' aggregation=' + str(aggregation)+
                                  ' output=' + str(output)+
                                  ' \'varname="' + varname+
                                  '"\' '+
                                  ' \'INPUTDIR="' + inputdir+
                                  '"\' '+
                                  ' \'OUTPUTDIR="' + outputdir+
                                  '"\' '+
                                  file_path, shell=True,timeout=300)
            if retcode < 0:
                print("Child was terminated by signal", -retcode, file=sys.stderr)

            else:
                print("Child returned", retcode, file=sys.stderr)

            returnCode = retcode

        except subprocess.CalledProcessError as e:
            print(e.output)
            returnCode = -1

        finally:
            return returnCode


class RunNCLV2:

    def withvar(inputdir,outputdir,startdate, enddate,latS,latN,lonW,lonE,varname,shade, height,interval,aggregation,output):
        ncl_file_path = os.path.join(tmp_dir, 'tmp.ncl')

        ncl_code = '''

load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load "constants_and_functions.ncl"
begin

 if (.not. isvar("startdate")) then      ; is startdate on command line?
      startdate = 19810101;
  end if

  if (.not. isvar("enddate")) then      ; is enddate on command line?
      enddate = 19810131;
  end if

  if (.not. isvar("latS")) then      ; is latS on command line?
      latS = 30;
  end if

  if (.not. isvar("latN")) then      ; is latN on command line?
      latN = 43;
  end if

  if (.not. isvar("lonW")) then      ; is lonW on command line?
      lonW = -125;
  end if

  if (.not. isvar("lonE")) then      ; is lonE on command line?
      lonE = -113;
  end if

  if (.not. isvar("varname")) then      ; is varname on command line?
      varname = "Tsurface";
  end if

  if (.not. isvar("INPUTDIR")) then      ; is INPUTDIR on command line?
      INPUTDIR = "/ebm/input";
  end if

  if (.not. isvar("OUTPUTDIR")) then      ; is OUTDIR on command line?
      OUTDIR = "/ebm/output";
  end if

; sample boundary always works
;latS=20
;latN=30
;lonW=-111.
;lonE=-105


;debug
print(latS)
print(latN)
print(lonW)
print(lonE)


;;;;;;;;;;;get the time frame (past or future)
if (startdate.gt.20010000) then
  model_time = "future"
else
  model_time = "past"
end if
;;;;;;;;;;;;;;

;debug
print(model_time)

setfileoption("nc","Format","LargeFile")

;;;;;;;;;;;;;; get the time values to extract from the files
startyear = startdate/10000
endyear = enddate/10000
TIME = yyyymmdd_time(startyear, endyear, "integer")
;get rid of leap year Feb 29
mmdd = TIME%10000 ;integer array showing mmdd
ii = ind(mmdd.ne.0229) ;every date that ISN'T 29th Feb
time_no_leap = TIME(ii) ;yyyymmdd array excluding all cases of 29th Feb.
time = time_no_leap({startdate:enddate})
num_of_times = dimsizes(time) * 24
;;;;;;;;;;;;;;;;;


;;;;;;;;;;;;;;;; get the indexes that bound the selected
climate_file = INPUTDIR + "/" + model_time + "_" + tostring(startyear) + "_" + varname + ".nc"

;debug
print(climate_file)

exists = fileexists(climate_file)

;check it it exists
if (.not.exists) then
    print("Microclimate for year - "+ tostring(startyear) + " unavailable now" )
    status_exit(42)
end if

year_file = addfile(climate_file , "r")

;debug
print(year_file)

lat2d = year_file->lat
;debug
;print(lat2d)

lon2d = year_file->lon

;debug
;print(lon2d)

times = year_file->time

;debug
;print(times)

if ((abs(latS-latN).lt.0.01) .and. (abs(lonW-lonE).lt.0.01)) then
  print("ONE POINT QUERY")
  region = getind_latlon2d (lat2d,lon2d, latS, lonW)
  jStrt = region(0,0)      ; lat start
  jLast = region(0,0)      ; lat last
  iStrt = region(0,1)      ; lon start
  iLast = region(0,1)      ; lon last
else
  print("REGION QUERY")
  region = region_ind (lat2d,lon2d, latS, latN, lonW, lonE)
  ;debug
  print(region)
  jStrt = region(0)      ; lat start
  jLast = region(1)      ; lat last
  iStrt = region(2)      ; lon start
  iLast = region(3)      ; lon last
end if

  num_of_lats = jLast - jStrt + 1
  num_of_lons = iLast - iStrt + 1


;;testing
;LAT2D = lat2d(jStrt:jLast,iStrt:iLast)
;LON2D = lon2d(jStrt:jLast,iStrt:iLast)

;debug
;printMinMax(LAT2D, 0)
;printMinMax(LON2D, 0)
;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;calculate the number of time points in the output file (need for creating the output file)
if (interval.eq.0) then ;hourly
  output_num_of_times = num_of_times
end if
if (interval.eq.1) then ;6-hourly
  output_num_of_times = toint(ceil(num_of_times / 6.))
end if
if (interval.eq.2) then ;12-hourly
  output_num_of_times = toint(ceil(num_of_times / 12.) - 1) ; remove one becuase the first night is not in the dataset (dataset starts at the middle of the night)
end if
if (interval.eq.3) then ;daily
  output_num_of_times = toint(ceil(num_of_times / 24.))
end if
if (interval.eq.4) then ;monthly
  yyyymm = time / 100
  ; count number of months
  nmonths = 1
  prev_month = yyyymm(0)
  do itime=1, num_of_times/24 - 1
    if (yyyymm(itime).ne.prev_month) then
      nmonths = nmonths+1
    end if
    prev_month = yyyymm(itime)
  end do
  output_num_of_times = nmonths
end if
;;testing
;print(output_num_of_times)
;;;;;;;;;;;;;;

;;;;;;;;;;;;;; set the output file
dimNames = (/"time", "south_north", "west_east"/)
dimSizes = (/ output_num_of_times   ,  num_of_lats,  num_of_lons /)
dimUnlim = (/ False, False, False/)
;;;;;; determine the file name based on the selected variable, height, and shade
;get current time in YYYYMMDDhhmm
current_time = systemfunc("date -u '+%Y-%m-%d-%H%M'")
varindex = ind(varnames.eq.varname)
if (vardims(varindex).eq."hourly") then
  fout_name = varname+"_output"
end if
if (vardims(varindex).eq."hourly/wrf_soil") then
  fout_name = varname+"_output_depth-" + tostring(SMOIS_depths(height))
end if
if (vardims(varindex).eq."hourly/mic_layer") then
  fout_name = varname+"_output_height-" + tostring(heights(height))
end if
if (vardims(varindex).eq."hourly/shade/mic_layer") then
  if (varname.eq."Tair") then
    layers_desc = "height"
  else
    layers_desc = "depth"
  end if
  fout_name = varname+"_output_shade-" + tostring(shades(shade)) + "_" + layers_desc + "-" + tostring(heights(height))
end if
if (vardims(varindex).eq."hourly/shade") then
  fout_name = varname+"_output_shade-" + tostring(shades(shade))
end if
fout_name = OUTPUTDIR + "/" + fout_name +  "_interval-" + intervals(interval) + "_aggregation-" + aggregations(aggregation) + "_times-" + tostring(startdate) + "-" + tostring(enddate) + "_created-"+ current_time

if (output.eq.1) then
  csv_name = fout_name + ".csv"
end if

fout_name = fout_name +".nc"

print(fout_name)
;;;;;;;; end of determining file name
print(output)


;;;;;;;; create the file
fout     = addfile(fout_name ,"c")  ; open output netCDF file
filedimdef(fout,dimNames,dimSizes,dimUnlim)
filevardef(fout, "time" ,typeof(times),(/"time"/))
filevardef(fout, "lat"  ,typeof(lat2d), (/ "south_north", "west_east" /) )
filevardef(fout, "lon"  ,typeof(lon2d),(/ "south_north", "west_east" /))
filevardef(fout, varname    ,"float" ,(/ "time", "south_north", "west_east" /))

;;;;;;;; copy units and long_name of the variable
if (vardims(varindex).eq."hourly") then
  data = year_file->$varname$(0, 0:10, 0:10)
end if
if (vardims(varindex).eq."hourly/shade/mic_layer") then
  data = year_file->$varname$(0, 0, 0, 0:10, 0:10)
end if
if (vardims(varindex).eq."hourly/shade" .or. vardims(varindex).eq."hourly/mic_layer" .or. vardims(varindex).eq."hourly/wrf_soil") then
  data = year_file->$varname$(0, 0, 0:10, 0:10)
end if
;filevarattdef(fout,varname,data)                    ; copy variable attributes
;;;; end of copying the attributes of the variable
if (isatt(data,"long_name")) then
    fout->$varname$@long_name = data@long_name
end if
if (isatt(data,"units")) then
    fout->$varname$@units = data@units
end if
fout->$varname$@_FillValue = data@_FillValue
delete(data)
;;;;;;;;

;;;;;copy attributes of time and lat/lon
filevarattdef(fout,"time" ,times)                   ; copy time attributes
filevarattdef(fout,"lon"  ,lon2d)                     ; copy lev attributes
filevarattdef(fout,"lat"  ,lat2d)                     ; copy lat attributes
;;;; end of copying attributes of time and lat/lon
;;;;;;;;;;;;;; end of setting up the output file

;;;;copy the selected lat/lon values to the output file
fout->lat = (/lat2d(jStrt:jLast, iStrt:iLast)/)
fout->lon = (/lon2d(jStrt:jLast, iStrt:iLast)/)
;;;;

;;;;;;;;;;;;;; add selection parameters as global attributes to the output file
globalAtt = True
globalAtt@varname = varname
globalAtt@startdate = startdate
globalAtt@enddate = enddate
globalAtt@latN = latN
globalAtt@latS = latS
globalAtt@lonW = lonW
globalAtt@lonE = lonE
globalAtt@interval = intervals(interval)
globalAtt@aggregation = aggregations(aggregation)
globalAtt@createdOn = current_time
if (vardims(varindex).eq."hourly/wrf_soil") then
  globalAtt@depth = tostring(SMOIS_depths(height)) + "(cm)"
end if
if (vardims(varindex).eq."hourly/mic_layer") then
  globalAtt@height = tostring(heights(height)) + "(cm)"
end if
if (vardims(varindex).eq."hourly/shade/mic_layer") then
  if (varname.eq."Tair") then
    globalAtt@height = tostring(heights(height)) + "(cm)"
  else
    globalAtt@depth = tostring(heights(height)) + "(cm)"
  end if
  globalAtt@shade = tostring(shades(shade)) + "(%)"
end if
if (vardims(varindex).eq."hourly/shade") then
  globalAtt@shade = tostring(shades(shade)) + "(%)"
end if
fileattdef( fout, globalAtt )

;; add information to csv file
if (output.eq.1) then
  write_csv_header(fout, csv_name, varname)
end if
;;;;;;;;;;;;;;

delete(year_file)

; set the time values at the beginning and end
starttime = startdate*100+1 ; the first hour of that day (hour 0 is considered as belong to the previous day)
endtime = enddate*100+23

;;; no segment from a previous year yet, just putting some values
num_of_times_in_prev_segment =0
prev_seg_times = new(12, "integer")
prev_seg_data = new((/12,num_of_lats, num_of_lons/) ,"float")

;;;;;;;;;;;;;; add data to the the output file ;;;;;;;;;
itime = 0
iyear=startyear
do iyear=startyear, endyear
    climate_file = INPUTDIR + "/" + model_time + "_" + tostring(iyear) + "_" + varname + ".nc"
    year_file = addfile(climate_file , "r")
    year_times = year_file->time
    tstart = ind(year_times.eq.starttime)
    tend = ind(year_times.eq.endtime)
    if (.not.ismissing(tend)) then ; add one to include the zero hour of the next day
      tend = tend + 1
    end if
    if (num_of_times_in_prev_segment.gt.0) then
      if (.not.ismissing(tstart)) then
        print("error: previous segment has data but tstart is not missing")
      end if
      ;add prev time points to year_times
      new_year_times = array_append_record(prev_seg_times(0:(num_of_times_in_prev_segment-1)), year_times, 0)
      delete(year_times)
      year_times = new_year_times
      delete(new_year_times)
    end if
    ntimes = get_ntimes(tstart, tend, year_times, interval, aggregation, num_of_times_in_prev_segment)
    ;print(ntimes)
    totime = itime + ntimes - 1
    ;print(tostring(iyear)+" "+tostring(itime) + " " + tostring(totime))

    ;get data
    if (vardims(varindex).eq."hourly") then
      data = (/year_file->$varname$(tstart:tend , jStrt:jLast, iStrt:iLast)/)
    end if
    if (vardims(varindex).eq."hourly/wrf_soil" .or. vardims(varindex).eq."hourly/mic_layer") then
      data = (/year_file->$varname$(height, tstart:tend , jStrt:jLast, iStrt:iLast)/)
    end if
    if (vardims(varindex).eq."hourly/shade/mic_layer") then
      data = (/year_file->$varname$(height, shade, tstart:tend , jStrt:jLast, iStrt:iLast)/)
    end if
    if (vardims(varindex).eq."hourly/shade") then
      data = (/year_file->$varname$(shade, tstart:tend , jStrt:jLast, iStrt:iLast)/)
    end if

    ;analyze data
    if (interval.eq.0) then ; hourly, nothing to analyze, just write to file
      fout->$varname$(itime:totime, :, :) =  (/ data /)
      fout->time(itime:totime) = (/ year_file->time(tstart:tend) /)
      if (output.eq.1) then ; write to csv file
        print ("writing data to csv")
        write_csv_data(csv_name, data, year_file->time(tstart:tend), ntimes, num_of_lats, num_of_lons, fout->lat, fout->lon)
      end if
    else ; analyze data by intervals
      output_time = new (ntimes, integer)
      if (num_of_times_in_prev_segment.gt.0) then
        ;add prev data points to data
        new_data = array_append_record(prev_seg_data(0:(num_of_times_in_prev_segment-1), :, :), data, 0)
        delete(data)
        data = new_data
        delete(new_data)
        tend = tend + num_of_times_in_prev_segment
      end if
      output_data =  get_segment_stats (data, year_times, tstart, tend, ntimes, num_of_lats, num_of_lons, output_time , prev_seg_times, prev_seg_data, num_of_times_in_prev_segment )
      ;print(ntimes)
      ;print(itime)
      ;print(totime)
      fout->$varname$(itime:totime, :, :) =  (/ output_data /)
      fout->time(itime:totime) = (/ output_time /)
      if (output.eq.1) then ; write to csv file
        print ("writing data to csv")
        write_csv_data(csv_name, output_data, output_time, ntimes, num_of_lats, num_of_lons, fout->lat, fout->lon)
      end if
      delete (output_data)
      delete(output_time)
    end if
    itime = itime + ntimes

    delete(data)
    delete(year_file)
    delete(year_times)
print("finished "+tostring(iyear))
end do
delete(prev_seg_data)
delete(prev_seg_times)
delete(fout)



;exit
status_exit(0)
end
    '''
        NCL.create(ncl_file_path, ncl_code)
        NCL.run(ncl_file_path,inputdir,outputdir, startdate, enddate,latS,latN,lonW,lonE ,varname)
