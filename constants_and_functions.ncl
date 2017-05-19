load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"

; example of a call to the code: ncl read_by_location_file_by_maps_Oct_27.ncl  'INPUTDIR="."' 'OUTPUTDIR="."' 'varname="Tsurface"' startdate=19821212 enddate=19891001 latS=20. latN=30. lonW=-111. lonE=-105. shade=0 height=0 interval=0 aggregation=0 output=0


;input parameters
;inputdir - a string with the full path of the input files
;outputdir - a string with the full path of the folder that should hold the output files
;startdate - an integer with the start date as YYYYMMDD
;enddate - an integer with the end date as YYYYMMDD

;latS,latN,lonW,lonE is the bounds of the selected region
;latS - a float number with the southernmost latitude
;latN  - a float number with the northernmost latitude
;lonW - a float number with the westernmost longitude
;lonE - a float number with the easternmost longitude

;varname - a string with the name of the variable 
;shade - an integer with the index of the shade level (0 to 4)
;height - an integer with the index of the height/depth layer (0 to 18 for WIND10, Tair, and Tsoil), (0 to 3 for SMOIS)
;interval - an integer with the index of the interval parameter (0 for hourly, 1 for 6-hourly, 2 for 12-hourly, 3 for daily, and 4 for monthly)
;aggregation - an integer with the aggregation method (0 for inst, 1 for max, 2 for min, 3 for mean). Note: instantaneous data for daily and monthly intervals does not make sense...
;output - an integer with the index of the output format (0 for netcdf, 1 for csv)
;Code should return netCDF in initial format or csv in following (or similar) format:
;Headers:
;lat lon date varname aggregation shade height VALUE 


varnames =   (/"EAH",     "EAIR",  "RHOAIR", "PSFC",    "FVEG",    "WGAP",  "BGAP",   "ISNOW",  "TAH", "ALBEDO",   "SWDOWN",        "GLW",              "TV",           "QAIR",         "SMOIS",                  "Tsurface",             "Tair",                               "Tsoil",                        "WIND10"/)
vardims = (/"hourly", "hourly", "hourly", "hourly", "hourly", "hourly", "hourly", "hourly", "hourly","hourly",  "hourly",       "hourly",         "hourly",         "hourly",      "hourly/wrf_soil",       "hourly/shade",       "hourly/shade/mic_layer",          "hourly/shade/mic_layer", "hourly/mic_layer"/)     
ndims =       (/  1,         1,        1,          1,      1,         1,        1,      1,          1,      1,        1,                1,              1,                1,             2                        , 2                       , 3                               , 3                             , 2/) 
;month_days = (/ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 /)
shades =  (/ 0,25,50,75,100 /)
heights = (/ 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 48, 66, 84, 102, 120, 138, 156, 174, 198 /)
SMOIS_depths = (/ 5, 10, 40, 100 /)
intervals = (/ "hourly", "6-hr", "12-hr", "daily", "monthly" /)
aggregations = (/ "inst", "max", "min", "avg" /)
sunrise_hr = 7


; this function returns the number of time points that will be written to the output file for a given year
function get_ntimes(tstart, tend, year_times, interval, aggregation, num_of_times_in_prev_segment)
local ntimes, yyymm, nmonths, prev_month, itime
begin

    if (ismissing(tstart)).and.(ismissing(tend)) then ; file in between the range of dates
        ;print ("middle file")
        tstart=0
        tend = 365*24-1
    else if (.not.ismissing(tstart)).and.(ismissing(tend)) then ; startdate is in the file but not enddate (the first file if more than on particular year is requested)
        tend = 365*24-1
    else if (ismissing(tstart)).and.(.not.ismissing(tend)) then ; enddate is in the file but not startdate (the last file if more than on particular year is requested)
        tstart=0
    end if
    end if
    end if
    
    ;print(tstart)
    ;print(tend)
    ntimes = tend - tstart + 1
    if (interval.eq.1) then ;6-hourly
      ntimes = toint(ceil(ntimes / 6.))
    end if
    if (interval.eq.2) then ;12-hourly (day/night)
      ;print(tostring(ntimes))
      ntimes = toint(ceil((ntimes + num_of_times_in_prev_segment) / 12.)-1) ; remove one since the first interval starts at 7am, the 12am to 6am of the first day and the 7pm to 12am of the last day are not included in the output
      ;print(tostring(ntimes))
    end if
    if (interval.eq.3) then ;daily
      ntimes = toint(ceil(ntimes / 24.))
    end if
    if (interval.eq.4) then ;monthly
      ; count number of months
      nmonths = 1
      do itime=tstart, tend
        hr = mod(year_times(itime), 100)
        day = mod(year_times(itime)/100, 100)  ; the day number of the month (minus one)
        if (day.eq.1 .and. hr.eq.1 .and. itime.gt.tstart) then ; the start of a month
            nmonths = nmonths+1
        end if
      end do
      ntimes = nmonths
    end if
    return(ntimes) 
end


function get_segment_stats (data[*][*][*], timevals, tstart, tend, output_num_of_times, numLats, numLons, output_times[*], prev_seg_times[*], prev_seg_data[*][*][*], num_of_times_in_prev_segment )
local ii, segend, isegment, t, segsize, sStart, sEnd, month, output_data, timeseg, tsegend
begin
    ;divide to segments
    ;sStart = new (output_num_of_times, integer)   ; the start index of each segment
    ;sEnd = new (output_num_of_times, integer)  ; the end index of each segment
    output_data =  new ( (/ output_num_of_times, numLats, numLons /), float) 
     
    if (interval.eq.1) then ;6-hourly
      segsize = 6
    end if
    if (interval.eq.2) then ;12-hourly
      segsize = 12
    end if
    if (interval.eq.3) then ;daily
      segsize = 24
    end if 
    
    iseg = 0
    sStart=-1
    sEnd=0
    segend=False
    timeseg = timevals(tstart:tend)
    tsegend = dimsizes(timeseg) - 1
    ;print(timeseg)
    do t = 0, tsegend
        hr = mod(timeseg(t), 100)
        ;print(hr)
        if (interval.eq.1 .or. interval.eq.3) then ; 6-hr or 24-hr intervals 
          m = mod(hr, segsize)
          if (m.eq.1) then ; a beginning of a segment
            sStart=t
            segend=False
            ;print("found start " + tostring(hr) + "at index "+ tostring(sStart)) 
          end if 
          if (m.eq.0 .and. (sStart.ge.0 .or. num_of_times_in_prev_segment.gt.0)) then ; the end of a segment (make sure that a start of a segment was found first - sStart>=0, or that we have data from a previous year num_of_times_in_prev_segment >0)
            segend=True 
            ;print("found end " + tostring(hr)) 
          end if 
        end if
        if (interval.eq.2) then ; 12-hr interval, calculate for day(from 7 to 18), and night (from 19 to 6)
          m = mod(hr, segsize)
          if (m.eq.(sunrise_hr)) then ; a beginning of a segment
            sStart=t
            segend=False
            ;print("found start " + tostring(hr) + "at index "+ tostring(sStart)) 
          end if 
          if (m.eq.(sunrise_hr-1) .and. (sStart.ge.0 .or. num_of_times_in_prev_segment.gt.0)) then ; the end of a segment
            segend=True 
            ;print("found end " + tostring(hr)) 
          end if 
        end if
        if (interval.eq.4) then ;monthly
          month = mod(timeseg(t)/10000, 100)
          day = mod(timeseg(t)/100, 100)  ; the day number of the month (minus one) 
          ;print("date:"+tostring(timeseg(t)) + ":"+ tostring(month)+tostring(day))
          if ((day.eq.1 .and. hr.eq.1) .or. sStart.lt.0 ) then ; the start of a month (include the first month even if it's only part of the month - sStart<0)
            sStart=t
            segend=False
          end if
          if ((day.eq.1 .and. hr.eq.0) .or. t.eq.tsegend) then ; the end of a month (include the last month even if it's only part of the month - t==tsegend)
            segend=True
          end if
        end if 
        sEnd=t
        if (segend) ;  calculate segment statistics
          ;print("segment:"+tostring(iseg) + " out of " + tostring(output_num_of_times))
          ;print(sStart)
          ;print(sEnd)
          output_times(iseg) =  timeseg(sStart) 
          if (aggregation.eq.3) then
              output_data(iseg, :, :) = dim_avg_n(data(sStart:sEnd,:,:) , 0)
          end if
          if (aggregation.eq.0) then
             output_data(iseg, :, :) = (/ data(sStart,:,:) /)
          end if
          if (aggregation.eq.2) then
             output_data(iseg, :, :) = dim_min_n(data(sStart:sEnd,:,:) , 0)
          end if
          if (aggregation.eq.1) then
             output_data(iseg, :, :) = dim_max_n(data(sStart:sEnd,:,:) , 0)
             ;print(dim_max_n(data(sStart:sEnd,:,:) , 0))
          end if
          iseg = iseg + 1          
        end if
    end do
    if (.not.segend) then ; the end of the segment is at the next year        
        num_of_times_in_prev_segment = sEnd-sStart +1
        ;delete(prev_seg_times) 
        prev_seg_times(0:(num_of_times_in_prev_segment-1)) = timeseg(sStart:sEnd)
        ;delete(prev_seg_data)
        prev_seg_data(0:(num_of_times_in_prev_segment-1), :, :) = (/ data(sStart:sEnd,:,:) /)
        ;print("saved data from "+tostring(timeseg(sStart))+ " to " + tostring(timeseg(sEnd)))
    else
        num_of_times_in_prev_segment = 0
    end if
    ;print(output_data(:, 15, 15))
    return(output_data)       
end

procedure write_csv_header(fout, csv_name, varname)
local header, hlist
begin
  
  ;;;;;;;add metedata to header
  header = (/ "#--------------------------------", \
            "# create on " + fout@createdOn, \
            "# varname = " + varname , \
            "# startdate = " + fout@startdate, \
            "# enddate = " + fout@enddate, \
            "# latS = " + sprintf("%.4f", fout@latS), \
            "# latN = " + sprintf("%.4f", fout@latN), \
            "# lonW = " + sprintf("%.4f", fout@lonW), \
            "# lonE = " + sprintf("%.4f", fout@lonE), \
            "# interval = " + fout@interval, \
            "# aggregation = " + fout@aggregation /)
  
  hlist = [/header/]
  write_table(csv_name, "w", hlist, "%s")
  if (isatt(fout,"shade")) then
    write_table(csv_name, "a", [/(/ "# shade = " + fout@shade /)/], "%s")
  end if
  if (isatt(fout,"depth")) then
    write_table(csv_name, "a", [/"# depth = " + fout@depth/], "%s")
  end if
  if (isatt(fout,"height")) then
    write_table(csv_name, "a", [/"# height = " + fout@height/], "%s")
  end if
  write_table(csv_name, "a",  [/"#--------------------------------"/], "%s")
  ;;;;;;;;;
   
  ;;;;;;;;; add names of columns to header
  hlist = [/"datetime","lat","lon",varname /]
  write_table(csv_name, "a", hlist, "%s, %s, %s, %s")
end

procedure write_csv_data(csv_name, data, times, num_of_times, num_of_lats, num_of_lons, lat2d, lon2d)
local times, lats, lons, values
begin

  ; create columns
  dims   = dimsizes(data)
  print("1d data")
  data1d = ndtooned( (/ data /) )
  inotmissing = ind(.not.ismissing(data1d))
  towritedata = data1d(inotmissing)
  delete(data1d)

  print("1d time")
  time1d = ndtooned(conform_dims(dims,times,0))
  towritetime = time1d(inotmissing)
  delete(time1d)

  print("1d lats")
  lat1d  = ndtooned(conform_dims(dims,lat2d, (/ 1,2 /)))
  towritelat = lat1d(inotmissing)
  delete(lat1d)

  print("1d lons")
  lon1d  = ndtooned(conform_dims(dims,lon2d, (/ 1,2 /)))
  towritelon = lon1d(inotmissing)
  delete(lon1d)
  delete(inotmissing)

  ;add columns to list
  ;alist = [/time1d, lat1d, lon1d, data1d/]
  alist = [/towritetime, towritelat, towritelon, towritedata/]
  write_table(csv_name, "a", alist, "%i, %g, %g, %g")
end

