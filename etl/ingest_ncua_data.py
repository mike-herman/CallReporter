# Script to ingest NCUA data files and load to S3 bucket.
from dataclasses import dataclass, field

import boto3
from botocore.exceptions import ClientError
import tempfile
import requests
import zipfile
import os
from itertools import product

import argparse
import logging
logger = logging.getLogger(__name__)
os.makedirs('.logs', exist_ok = True)
logging.basicConfig(filename='.logs/injest_ncua_data.log', encoding='utf-8', level=logging.DEBUG)

_DEFAULT_FILE_NAMES = [
        'AcctDesc.txt','Acct-DescCUSO.txt','Acct-DescGrants.txt','Acct-DescTradeNames.txt','ATM.txt',
        'Credit.txt','FOICU.txt','FOICUDES.txt','FS220.txt','FS220A.txt',
        'FS220B.txt','FS220C.txt','FS220CUSO.txt','FS220D.txt','FS220E.txt',
        'FS220G.txt','FS220H.txt','FS220I.txt','FS220J.txt','FS220K.txt',
        'FS220L.txt','FS220M.txt','FS220N.txt','FS220P.txt','FS220Q.txt',
        'FS220R.txt','FS220S.txt','Grants.txt','TradeNames.txt'
    ]

# URL Links by date range:
# https://ncua.gov/files/publications/analysis/call-report-data-{yyyy}-{mm}.zip for June 2015 onward.
# https://ncua.gov/files/publications/data-apps/QCR{yyyymm}.zip March 2015 and earlier.



# CLI Parser
parser = argparse.ArgumentParser(
    prog="ingest_ncua_data",
    description="Ingests NCUA call report data."
)
parser.add_argument('-s','--start_yyyy_mm', type=str, nargs=1, required=True, help='The year-month you want data for. Must be format "yyyy-mm". Months accepted: [03,06,09,12].')
parser.add_argument('-e','--end_yyyy_mm', type=str, nargs=1, required=False, help='(Optional) The last year-month you want data for. Will also pull data for all munths in between. Must be format "yyyy-mm". Months accepted: [03,06,09,12].')
args = parser.parse_args()


@dataclass
class NCUA_Ingester:
    '''Class to ingest NCUA data files into S3 bucket.'''

    DOWNLOAD_URL_TEMPLATE_PRE_JUNE_2015 = 'https://ncua.gov/files/publications/data-apps/QCR{}{}.zip'
    DOWNLOAD_URL_TEMPLATE: str = 'https://ncua.gov/files/publications/analysis/call-report-data-{}-{}.zip'
    S3_BUCKET_NAME: str = 'call-reporter'
    data_file_names: list[str] = field(default_factory=lambda: _DEFAULT_FILE_NAMES)

    def __post_init__(self):
        logger.info("Created NCUA_Ingester instance. S3_BUCKET_NAME={S3_BUCKET_NAME}.")

    def ingest_quarter_data(self, data_year: int, data_month: int):
        url_template = self.DOWNLOAD_URL_TEMPLATE if data_year+data_month/12 >= 2015+06/12 else self.DOWNLOAD_URL_TEMPLATE_PRE_JUNE_2015
        data_year = f'{data_year:0>4}'
        data_month = f'{data_month:0>2}'
        download_url = url_template.format(data_year,data_month)

        logger.info(f'Attempting download from {download_url}')
        response = requests.get(download_url)
        if response.status_code != 200:
            logger.info(f'Failed download from {download_url}. Status code: {response.status_code}.')
            return
        else:
            logger.info('Download successful.')

        temp_dir = tempfile.mkdtemp()
        logger.debug(f'temp_dir created: {temp_dir}')
        zip_path = os.path.join(temp_dir, f'call-report-data-{data_year}-{data_month}.zip')
        
        
        with open(zip_path, 'wb') as file:
            file.write(response.content)
        logger.debug(f'Zip file saved to {zip_path}')
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        logger.debug('Zip files extracted.')
        # logger.debug(f'Here are all the files in the directory: {os.listdir(temp_dir)}')
        
        s3 = boto3.client('s3')
        logger.info(f'Starting file upload to S3.')
        for file in os.listdir(temp_dir):
            if file in self.data_file_names:
                object_name = '/'.join(['ncua',data_year,data_month,file])
                logger.debug(f'Uploading {os.path.join(temp_dir,file)} to S3 as {object_name}.')
                try:
                    s3_response = s3.upload_file(os.path.join(temp_dir,file), self.S3_BUCKET_NAME, object_name)
                    logger.debug(f'Successfully uploaded {object_name} to S3 bucket {self.S3_BUCKET_NAME}.')
                except ClientError as e:
                    logger.error(e)
                    return False
        logger.info(f'All files uploaded to S3.')
        return True


ncua_ingester = NCUA_Ingester()

start_year = 1994
start_month = 3
end_year = 1995
end_month = 3

year_list = list(range(start_year,end_year+1))
month_list = [3,6,9,12]

year_month_set = [[year,month] for year, month in product(year_list, month_list) \
    if (year,month) >= (start_year, start_month) \
    and (year,month) <= (end_year, end_month)]

logger.debug(f'year_month_set: {year_month_set}')
breakpoint()
for year, month in year_month_set:
    ncua_ingester.ingest_quarter_data(year,month)


