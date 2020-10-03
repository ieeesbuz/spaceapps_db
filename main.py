import requests


url = 'https://daac.ornl.gov/daacdata/cms/CMS_DARTE_V2/data/onroad_2017.tif'
r = requests.get(url, allow_redirects=True)
open('onroad_2017.tif', 'wb').write(r.content)