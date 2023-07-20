
import math
from .geographic import *

def convert_kelvin_to_fahrenheit(k):
    return 1.8*(k-273.15) + 32

def convert_celcius_to_fahrenheit(c):
    return 9*(c/5)+32

def convert_fahrenheit_to_celcius(f):
    return (5/9)*(f-32)
    


def calculate_heat_index(T, RH):
    ## Taken from the NWS 2011 definition of a heatwave
    if T<=40:
        return T
    else:
        A = -10.3 + 1.1*T + 0.047*RH
        if A<79:
            return A
        else:
            B = -42.379+2.04901523*T + 10.14333127*RH -0.22475541*T*RH- .00683783*T*T - .05481717*RH*RH + .00122874*T*T*RH + .00085282*T*RH*RH - .00000199*T*T*RH*RH
            if RH <= 13 and T>=80 and T<=112:
                return B - (math.abs(13-RH)/4)*(math.sqrt((17-math.abs(T-95))/17))
            elif RH>85 and T>=80 and T<=87:
                return B + 0.02*(RH-85)*(87-T)
            else:
                return B
    #HI = -42.379 + 2.04901523*T + 10.14333127*RH - .22475541*T*RH - .00683783*T*T - .05481717*RH*RH + .00122874*T*T*RH + .00085282*T*RH*RH - .00000199*T*T*RH*RH
    #return HI

def combine_tmax_rh(tmax, rh):
    tmax=tmax.to_dask_dataframe()
    tmax['Tasmax_F'] = tmax['Tasmax'].apply(lambda x: convert_kelvin_to_fahrenheit(x), meta = ('Tasmax', 'float64'))
    rh = rh.to_dask_dataframe()
    hi = tmax.merge(rh, on = ['time', 'lat', 'lon'])

    return hi


def classify_heat_index(x, t=0, rh=0):
    if x==103 and rh<95 and t>86 and t<90:
        return 3
    elif x>125:
        return 4
    elif x>=104:
        return 3
    elif x>90:
        return 2
    elif x>80:
        return 1
    else:
        return 0
    
def get_heat_index(ds_tasmax, ds_rh, shapefile):
    tmax = clip_area(ds_tasmax, shapefile)
    rh = clip_area(ds_rh, shapefile)

    hi = combine_tmax_rh(tmax, rh)
    hi['heat_index'] = hi.apply(lambda x: calculate_heat_index(x['Tasmax_F'], x['RH_f_inst']), meta = (None, 'float64'), axis=1)

    hi = hi.compute()

    return hi