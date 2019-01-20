#!/usr/bin/env python3
import mysql.connector as mariadb
import time, re
import urllib.request
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as bs



def read_url(url):
    """
    Accepts:
        url     url of webpage

    Returns:
        text string of webpage or False on error
    """
    try:
        response = urllib.request.urlopen(url)
        data = response.read()
        text = data.decode('utf-8')
    except urllib.error.URLError:
        print('URL Error')
        text = False
    except urllib.error.HTTPError:
        text = False
    
    return text



def find_data(regex,subject,start=0):
    """
    Accepts:
        regex       re module regex that must contain 1 group labeled 'data'
                    example: r'MAXIMUM +(?P<max>[\-0-9TM]{1,4})'
                    
        subject     string that will be searched
        
        start       string position to begin the search.
        
    Returns:
        value of the data group or 'null'
        string posistion of the end of the match or original start
        
    """
    match = re.search(regex, subject[start:])

    value = 'null'
    end = start
    if match:
        value = match.group('data')
        end   = match.end('data')
    else:
        print('Match not found.')
        
    return value, end




start = time.time()
print("=" * 102)
print("Script starting.")




# Fecth the daily climo data
html = read_url('https://forecast.weather.gov/product.php?site=NWS&product=CLI&issuedby=MSP')
soup = bs(html,'lxml')

product_raw = soup.pre.string
product = soup.pre.string
print(product)


 

# check the date
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime('%B %-d %Y').upper()
yesterday_db = yesterday.strftime("%Y-%d-%m")

print('YESTERDAY: %s' % yesterday)

data = re.search(r'CLIMATE SUMMARY FOR ' + yesterday_str,product)
if data:
    # found climo data for yesterday.
    print("Found Yesterday: %s" % yesterday_db)
    pass




end = time.time()
print("Script finished in %s" % (end-start))
print("=" * 102)
