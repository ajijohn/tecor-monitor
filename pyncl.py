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


class RunNCL:

    def withvar(inputdir,outputdir,startdate, enddate,latS,latN,lonW,lonE,varname):
        ncl_file_path = os.path.join(tmp_dir, 'tmp.ncl')

        ncl_code = '''

load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
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


;INPUTDIR=""

geo_em = addfile("geo_em.d01.nc", "r")
lat2d = geo_em->XLAT_M(0,:,:)
lon2d = geo_em->XLONG_M(0,:,:)

latS   = 30                      ; California [*rough*]
latN   = 43
lonW   = -125
lonE   = -113

ji = region_ind (lat2d,lon2d, latS, latN, lonW, lonE)
jStrt = ji(0)      ; lat start
jLast = ji(1)      ; lat last
iStrt = ji(2)      ; lon start
iLast = ji(3)      ; lon last

;varname = "Tsurface"
model_time = "past"

startyear = startdate/10000
endyear = enddate/10000

;startdate=19810101
;enddate=19810131

;get file name
climate_file = model_time + "_" + tostring(startyear) + "_" + varname + ".nc"
year_file = addfile(INPUTDIR + climate_file , "r")

tstart = ind(year_file->time.eq.(startdate*100))
tend = ind(year_file->time.eq.(enddate*100))
;print(tstart)
;print(tend)
;print(jStrt)
;print(jLast)
;print(iStrt)
;print(iLast)

var = year_file->$varname$(0, tstart:tend,jStrt:jLast, iStrt:iLast)

;OUTPUTDIR=""

asciiwrite(OUTPUTDIR + "d02.txt", var)
delete(year_file)
exit
end
    '''
        NCL.create(ncl_file_path, ncl_code)
        NCL.run(ncl_file_path,inputdir,outputdir, startdate, enddate,latS,latN,lonW,lonE ,varname)
