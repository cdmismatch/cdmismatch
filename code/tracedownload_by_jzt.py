import os, requests
from bs4 import BeautifulSoup

for month in range(1, 13):
    for day in range(1, 2):
        '''
        url = 'http://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2019/cycle-2019' + str(month).zfill(2) + str(day).zfill(2) + '/'
        try:
            if not os.path.exists('probe-data/2019' + str(month).zfill(2) + str(day).zfill(2) + '/'):
                os.makedirs('probe-data/2019' + str(month).zfill(2) + str(day).zfill(2) + '/')
            r = requests.get(url, stream=True)
            soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
            for link in soup.find_all('a', href=True):
                filename = link['href']
                if '2019' + str(month).zfill(2) + str(day).zfill(2) not in filename:
                    continue
                urlf = url + filename
                r = requests.get(urlf, stream = True)
                with open('probe-data/2019' + str(month).zfill(2) + str(day).zfill(2) + '/' + filename, 'wb') as code:
                    code.write(r.content)
        except Exception as e:
            print(e)
        continue
        '''

        url = 'http://data.caida.org/datasets/topology/ark/ipv4/prefix-probing/2019/' + str(month).zfill(2) + '/'
        try:
            if not os.path.exists('/mountdisk1/ana_c_d_incongruity/jzt/prefix-probing/2019' + str(month).zfill(2) + str(day).zfill(2) + '/'):
                os.makedirs('/mountdisk1/ana_c_d_incongruity/jzt/prefix-probing/2019' + str(month).zfill(2) + str(day).zfill(2) + '/')
            print(1)
            r = requests.get(url, stream=True)
            print(2)
            soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
            print(3)
            for link in soup.find_all('a', href=True):
                filename = link['href']
                if '2019' + str(month).zfill(2) + str(day).zfill(2) not in filename:
                    continue
                urlf = url + filename
                print(urlf)
                r = requests.get(urlf, stream = True)
                with open('/mountdisk1/ana_c_d_incongruity/jzt/prefix-probing/2019' + str(month).zfill(2) + str(day).zfill(2) + '/' + filename, 'wb') as code:
                    code.write(r.content)
        except Exception as e:
            print(e)
