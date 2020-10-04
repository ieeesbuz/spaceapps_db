import rasterio
from rasterio.plot import show
import csv
import psycopg2
import numpy as np
import math
import os
import re

#This function calculates CO2 from raster data
def CO2_calc (lat, lon, area, band1):
    ciudad = np.array([lat, lon, area]) #longitud, latitud, area(km^2)

    #raster data has ncols 5070 nrows 3560
    dx = 0.014833485247         #degree increment
    dy = 0.009892490399
    dx_km = 1.649               #haversine calc for meter increment
    dy_km= 1.100 

    #offset from table origin in degrees
    lat_add = abs(53.3863-(ciudad[0]))
    lon_add = abs((ciudad[1])+137.257)

    #translate from degrees to position in table
    pos_lon = int(round(lon_add/dx))
    pos_lat = int(round(lat_add/dy))

    #calculate how many cells to search, asumes city is square
    lon_mitad = int(round(((math.sqrt(ciudad[2])/2)/dx_km)))
    lat_mitad = int(round(((math.sqrt(ciudad[2])/2)/dy_km)))

    #in case cities are small
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

    #cropped raster CO2 measures
    submatrix = band1[a:b,c:d]

    #finally we calc the sum
    CO2_sum = np.sum(submatrix)

    return(CO2_sum)

#checks if coordinates are within raster
def coordinatesValid(row):
    if -137.257 < float(row["lng"]) and -62.0377 > float(row["lng"]) \
    and 53.3863 > float(row["lat"]) and 22.0928 < float(row["lat"]):
        return True
    else:
        return False

#database stuff
conn = psycopg2.connect(database="mydb", user="postgres", password="WoodenRumba00", host="192.168.0.120")
cur = conn.cursor()

#erase previous data
cur.execute('DELETE FROM public.nasa')
cur.execute('DELETE FROM public.ranking')
cur.execute('ALTER SEQUENCE ranking_id_seq RESTART')

#open city list
with open('uscities.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    line_count = 0

    #check every tiff file
    for file in os.listdir("Maps"):
        dataset = rasterio.open(os.path.join("Maps",file))
        m = re.search('onroad_(.+?)_84', file)  #check year
        year = int(m.group(1))
        print(year)
        band1 = dataset.read(1)
        #print(band1[(2790-11):(2790+11),(3846-16):(3846+16)])

        #calc for every city
        for row in csv_reader:   
            if float(row["density"]) > 0 and coordinatesValid(row) :
                population = float(row["population"])/(1.037*(2018-year))
                area = population/float(row["density"])     
                co2=CO2_calc(float(row["lat"]),float(row["lng"]),area,band1)
                ratio = co2/population
                if ratio == 0:
                     print(row)
                city= row["city"]
                city = city.replace("'", "''")
                query=f'INSERT INTO public.nasa VALUES (DEFAULT, \'{city}\', \'{row["lat"]}\', \
                \'{row["lng"]}\',\'{int(population)}\',\'{co2}\',\'{ratio}\',\'{year}\' )'
                cur.execute(query)
                time.sleep(1)

cur.execute('INSERT INTO public.ranking (name,lat,lng,population,co2,ratio,year) SELECT name,lat,lng,population,co2,ratio,year FROM public.nasa ORDER BY ratio DESC')
#Commit all changes to database
conn.commit()

# Close communication with the database
cur.close()
conn.close()
