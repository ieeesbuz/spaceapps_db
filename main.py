import rasterio
from rasterio.plot import show
import csv
import psycopg2
import numpy as np
import math
import sys

def CO2_calc (lat, lon, area, band1):
    ciudad = np.array([lat, lon, area]) #longitud, latitud, area(km^2)

    dx = 0.014833485247         #ncols  5070 nrows   3560
    dy = 0.009892490399
    dx_km = 1.649 
    dy_km= 1.100 

    
    # band1

    lat_add = abs(53.3863-(ciudad[0]))
    lon_add = abs((ciudad[1])+137.257)

    #print(lat_add,lon_add)
    pos_lon = int(round(lon_add/dx))
    pos_lat = int(round(lat_add/dy))

    # if area < math.pow(dx_km,2):
    #     try:
    #         return(band1[pos_lat,pos_lon]) 
    #     except IndexError: 
    #         print(ciudad)
    #         sys.exit(1)
    
    #print(pos_lat,pos_lon)
    #band1[pos_lat,pos_lon]

    lon_mitad = int(round(((math.sqrt(ciudad[2])/2)/dx_km)))
    lat_mitad = int(round(((math.sqrt(ciudad[2])/2)/dy_km)))

    #print(lon_mitad,lat_mitad)

    if lon_mitad == 0:        
       c = pos_lon
       d = pos_lon + 1
    else:
        c = pos_lon-lon_mitad
        d = pos_lon+lon_mitad

    if lat_mitad == 0:
        a = pos_lat
        b = pos_lat + 1       
    else:
        a = pos_lat-lat_mitad
        b = pos_lat+lat_mitad

    submatrix = band1[a:b,c:d]

    #print(submatrix)

    CO2_sum = np.sum(submatrix)

    #print(CO2_sum)
    return(CO2_sum)
 
def coordinatesValid(row):
    if -137.257 < float(row["lng"]) and -62.0377 > float(row["lng"]) \
    and 53.3863 > float(row["lat"]) and 22.0928 < float(row["lat"]):
        return True
    else:
        return False

conn = psycopg2.connect(database="mydb", user="postgres", password="WoodenRumba00", host="192.168.0.120")
cur = conn.cursor()
dataset = rasterio.open('Maps/onroad_2017_84.tif')
band1 = dataset.read(1)
cur.execute('DELETE FROM public.nasa')

with open('uscities.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    line_count = 0
    for row in csv_reader:   
        if float(row["density"]) > 0 and coordinatesValid(row) :
            area = float(row["population"])/float(row["density"])     
            co2=CO2_calc(float(row["lat"]),float(row["lng"]),area,band1)
            #line_count += 1
            print(co2)    
            ratio = co2/float(row["population"])
            city= row["city"]
            city = city.replace("'", "''")
            query=f'INSERT INTO public.nasa VALUES (DEFAULT, \'{city}\', \'{row["lat"]}\', \'{row["lng"]}\',\'{int(float(row["population"]))}\',\'{co2}\',\'{ratio}\' )'
            cur.execute(query)
conn.commit()

# Close communication with the database
cur.close()
conn.close()
