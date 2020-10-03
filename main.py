import rasterio
import numpy as np
import math

def CO2_calc (lat, lon, area):
    ciudad = np.array([lat, lon, area]) #latitud, longtitud, area(km^2)

    dx = 0.014833485247 #degree
    dy = 0.009892490399 #degree
    dx_km = 1.649 
    dy_km= 1.100 

    dataset = rasterio.open('Maps/onroad_2017_lonlat.tif')

    band1 = dataset.read(1)

    lat_add = abs(53.3863-(ciudad[0]))
    lon_add = abs((ciudad[1])+137.257)
    
    pos_lon = int(round(lon_add/dx))
    pos_lat = int(round(lat_add/dy))

    lon_mitad = int(round(((math.sqrt(ciudad[2])/2)/dx_km)))
    lat_mitad = int(round(((math.sqrt(ciudad[2])/2)/dy_km)))

    submatrix = band1[(pos_lat-lat_mitad):(pos_lat+lat_mitad),(pos_lon-lon_mitad):(pos_lon+lon_mitad)

    return(np.sum(submatrix))
