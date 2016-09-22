#!/usr/bin/env python3

# from pyncl import NCL
import pyncl


def main():
    #pyncl.Func.wrf_user_getvar('wrfout.nc', 'slp')
    pyncl.RunNCL.netcdf_getvar('wrfout.nc', 'slp')


if __name__ == '__main__':
    main()