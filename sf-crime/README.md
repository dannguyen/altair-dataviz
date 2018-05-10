# sf-crime

The [datastash/sfpd-incidents-annual.csv](datastash/sfpd-incidents-annual.csv) file contains an aggregate count of the incident log as posted by the San Francisco Police Department to Socrata:

https://data.sfgov.org/Public-Safety/-Change-Notice-Police-Department-Incidents/tmnf-yvry/data

The aggregation is by: year, category, descript, and pddistrict


## fetch and wrangle


Download the data from [Socrata](https://data.sfgov.org/Public-Safety/-Change-Notice-Police-Department-Incidents/tmnf-yvry/data)

```sh
# https://data.sfgov.org/Public-Safety/SF-Police-Department-Incidents-2012-and-2017/pmza-tuhm/data


# https://data.sfgov.org/Public-Safety/-Change-Notice-Police-Department-Incidents/tmnf-yvry/data
curl -o incidents.csv \
  https://data.sfgov.org/api/views/tmnf-yvry/rows.csv?accessType=DOWNLOAD
```



```sh

sqlite3 sfcrimes.sqlite <<EOF
.mode csv

DROP TABLE  IF EXISTS incidents;
CREATE TABLE incidents (
  "IncidntNum" INTEGER, 
  "Category" VARCHAR, 
  "Descript" VARCHAR, 
  "DayOfWeek" VARCHAR, 
  "Date" VARCHAR, 
  "Time" VARCHAR, 
  "PdDistrict" VARCHAR, 
  "Resolution" VARCHAR, 
  "Address" VARCHAR, 
  "X" DECIMAL, 
  "Y" DECIMAL, 
  "Location" VARCHAR, 
  "PdId" INTEGER
);

.import incidents.csv incidents


CREATE INDEX idx_date_on_incidents ON incidents(date);
CREATE INDEX idx_yrcatdist_on_incidents ON incidents(
    SUBSTR(date, -4), category, descript, PdDistrict);

DELETE FROM incidents WHERE IncidntNum = 'IncidntNum';
EOF
```


## Aggregate and export


```sh

echo  '''
.headers on
.mode csv

SELECT 
  SUBSTR(Date, -4)
    AS year 
  , PdDistrict
  , Category
  , Descript
  , COUNT(1) AS incident_count
FROM incidents 
GROUP BY year, pddistrict, category, descript
ORDER BY year, pddistrict, category, descript;
''' \
  | sqlite3 sfcrimes.sqlite > sfpd-incidents-annual.csv


```

