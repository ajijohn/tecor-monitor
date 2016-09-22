# ==============================================================================
# Author: Feng Zhu
# Date: 2015-01-26 12:23:30
__version__ = '0.0.1'
# ==============================================================================


# Notes
# f = addfile("no_such_file.nc","r")
# if (ismissing(f)) then
#    status_exit(42)
# end if
#
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

    def run(file_path):
        retcode = subprocess.call('ncl ' + file_path, shell=True)
        if retcode < 0:
            print("Child was terminated by signal", -retcode, file=sys.stderr)
        else:
            print("Child returned", retcode, file=sys.stderr)


class Func:

    def wrf_user_getvar(wrfout_path, var):
        ncl_file_path = os.path.join(tmp_dir, 'tmp.ncl')
        output_path = os.path.join(tmp_dir, var + '.dat')
        ncl_code = '''
load "$NCARG_ROOT/lib/ncarg/nclscripts/wrf/WRFUserARW.ncl"
begin
f = addfile("''' + wrfout_path + '''","r")
lat2d = f->XLAT(0, :, :)
lon2d = f->XLONG(0, :, :)
dims = dimsizes(lat2d)
nlat  = dims(0)
nlon  = dims(1)
time = 0
var = wrf_user_getvar(f, "''' + var + '''", time)
opt = True
opt@fout = "''' + output_path + '''"
fmt = nlon + "f15.9"
write_matrix(var, fmt, opt)
end
    '''
        NCL.create(ncl_file_path, ncl_code)
        NCL.run(ncl_file_path)


class Plot:

    def plot_track_error(track_errors, output_path):
        ncl_file_path = os.path.join(tmp_dir, 'tmp.ncl')
        ncl_code = '''
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
begin
f = addfile("''' + wrfout_path + '''","r")
lat2d = f->XLAT(0, :, :)
lon2d = f->XLONG(0, :, :)
dims = dimsizes(lat2d)
nlat  = dims(0)
nlon  = dims(1)
time = 0
var = wrf_user_getvar(f, "''' + var + '''", time)
opt = True
opt@fout = "''' + output_path + '''"
fmt = nlon + "f15.9"
write_matrix(var, fmt, opt)
end
    '''
        NCL.create(ncl_file_path, ncl_code)
        NCL.run(ncl_file_path)

class RunNCL:

    def netcdf_getvar(wrfout_path, var):
        ncl_file_path = os.path.join(tmp_dir, 'tmp.ncl')
        output_path = os.path.join(tmp_dir, var + '.dat')
        ncl_code = '''
load "$NCARG_ROOT/lib/ncarg/nclscripts/wrf/WRFUserARW.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
begin
f = addfile("/Users/ajijohn/Downloads/air.mon.mean.v401.nc","r")
x     = f->air                 ; (time,lat,lon) , ntim=1461
ts_extract = x(0,{30:60},{0:50})
xwks = gsn_open_wks("png","/Users/ajijohn/Downloads/flap")
res  = True
plot = gsn_csm_contour_map(xwks,ts_extract ,res)
exit
end
    '''
        NCL.create(ncl_file_path, ncl_code)
        NCL.run(ncl_file_path)
