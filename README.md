# Call Reporter Project

Goal: Download credit union call report data, load to a database, expose through a Superset dashboard.

# Data Source
The data comes from [ncua.gov](https://ncua.gov/analysis/credit-union-corporate-call-report-data/quarterly-data).

Data for all credit unions are published each quarter. The data is bundled into a zipped directories. The directories are available for download and use the following convertion: `https://ncua.gov/files/publications/analysis/call-report-data-{yyyy}-{mm}.zip` where `yyyy` is the year and `mm` is the month for the given quarter it was published (one of `03` for March, `06` for June, `09` for September, `12` for December). Revised data is re-published to the same URL.

