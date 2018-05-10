"""Fetch plaintext data files from NASA Climate site and serialize as CSV

Fetches data files behind the presentations of these pages:

https://climate.nasa.gov/vital-signs/carbon-dioxide/
https://climate.nasa.gov/vital-signs/global-temperature/

And deserializes them into dicts using regex patterns.

Some cleaning is done (changing -99.99 and -1 to None, where appropriate).

And CSVs are saved to: datastash/wrangled/

For more context, refer to this outdated notebook:
https://github.com/dannguyen/python-notebooks-data-wrangling/blob/master/Data-Extraction--NASA-Text.ipynb

Author: Dan Nguyen twitter:@dancow github:dannguyen
"""


import csv
from pathlib import Path
import re
from urllib.request import urlopen

DATASTASH_PATH = Path('datastash')

DATASETS = {

    # C02 monthly measurements from NOAA
    # more info: https://climate.nasa.gov/vital-signs/carbon-dioxide/
    # sample:
    # 1964   4    1964.292      -99.99      321.77      319.48     -1
    # 1964   5    1964.375      322.25      322.25      319.42     -1

    'co2': {
        'url': 'ftp://aftp.cmdl.noaa.gov/products/trends/co2/co2_mm_mlo.txt',
        'headers': ('year', 'month', 'decimal_date', 'average',
                   'interpolated', 'trend', 'days'),
        'pattern':  re.compile(
                       r'^(?P<year>\d{4})\s+'
                       r'(?P<month>\d{1,2})\s+'
                       r'(?P<decimal_date>\d{4}\.\d{3})\s+'
                       r'(?P<average>-?\d+\.\d+)\s+'
                       r'(?P<interpolated>\d+\.\d+)\s+'
                       r'(?P<trend>\d+\.\d+)\s+'
                       r'(?P<days>-?\d+)\s*$'),

    },

    # Global Surface Air Temperature Anomaly (C) (Base: 1951-1980)
    # more info: https://climate.nasa.gov/vital-signs/global-temperature/
    # sample:
    # 1976    -0.11   0.03
    # 1977    0.17    0.07
    'global_temps': {
        'url': 'https://climate.nasa.gov/system/internal_resources/details/original/647_Global_Temperature_Data_File.txt',
        'headers': ('year', 'annual_mean', 'lowess'),
        'pattern': re.compile(
                    r'^(?P<year>\d{4})\s+'
                    r'(?P<annual_mean>-?\d+\.?\d*)\s+'
                    r'(?P<lowess>-?\d+\.?\d*)\s*$'),

    },


}

def fetch_and_save(url, dest):
    with urlopen(url) as u:
        txt = u.read().decode('utf-8')
        # save to raw/ directory
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(txt)
        return txt




def main():
    for dname, meta in DATASETS.items():
        print("Working on: ", dname)
        rawpath = DATASTASH_PATH.joinpath('raw', Path(meta['url']).name)

        print("\t", "Downloading from:", meta['url'])
        print("\t", "Saving to:", rawpath)
        txt = fetch_and_save(meta['url'], rawpath)

        # process and deserialize
        records = []
        for line in txt.splitlines():
            rx = meta['pattern'].match(line)
            if rx:
                d = rx.groupdict()
                records.append(d)
                # co2 data needs special processing
                if dname == 'co2':
                    if d['average'] == '-99.99':
                        d['average'] = None
                    if d['days'] == '-1':
                        d['days'] = None

        # done processing
        print("\t", "Wrangled:", len(records), "records")


        # write to disk
        csvpath = DATASTASH_PATH.joinpath('wrangled', dname + '.csv')
        csvpath.parent.mkdir(parents=True, exist_ok=True)
        print("\t", "Wrote records to:", csvpath)
        with open(csvpath, 'w') as w:
            wc = csv.DictWriter(w, fieldnames=meta['headers'])
            wc.writeheader()
            wc.writerows(records)



if __name__ == '__main__':
    main()






