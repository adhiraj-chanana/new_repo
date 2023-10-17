import os
import subprocess
from datetime import datetime, timedelta
import xarray
import wget
import requests
urls = [
    "https://download.scidb.cn/download?fileId=61b95de78bba886bd1c5230b&dataSetType=personal&fileName=atm_hist_1981_01.nc4",
    "https://download.scidb.cn/download?fileId=61b95de78bba886bd1c521ec&dataSetType=personal&fileName=atm_hist_1981_02.nc4",
    "https://download.scidb.cn/download?fileId=61b95de78bba886bd1c522ef&dataSetType=personal&fileName=atm_hist_1981_03.nc4",
    "https://download.scidb.cn/download?fileId=61b95de78bba886bd1c522eb&dataSetType=personal&fileName=atm_hist_1981_04.nc4",
    "https://download.scidb.cn/download?fileId=61b95de78bba886bd1c52223&dataSetType=personal&fileName=atm_hist_1981_05.nc4",
    "https://download.scidb.cn/download?fileId=61b95de78bba886bd1c521aa&dataSetType=personal&fileName=atm_hist_1981_06.nc4",
    "https://download.scidb.cn/download?fileId=61b95de78bba886bd1c52303&dataSetType=personal&fileName=atm_hist_1981_07.nc4",
    "https://download.scidb.cn/download?fileId=61b95de78bba886bd1c521ca&dataSetType=personal&fileName=atm_hist_1981_08.nc4",
    "https://download.scidb.cn/download?fileId=61b95de78bba886bd1c522d3&dataSetType=personal&fileName=atm_hist_1981_09.nc4",
    "https://download.scidb.cn/download?fileId=61b95de78bba886bd1c521bc&dataSetType=personal&fileName=atm_hist_1981_10.nc4",
    "https://download.scidb.cn/download?fileId=61b95de78bba886bd1c522de&dataSetType=personal&fileName=atm_hist_1981_11.nc4",
    "https://download.scidb.cn/download?fileId=61b95de78bba886bd1c52307&dataSetType=personal&fileName=atm_hist_1981_12.nc4"
]

# Function to download a file
def download_file(url, file_name):
    response = requests.get(url)
    with open(file_name, 'wb') as file:
        file.write(response.content)

# Download files
for url in urls:
    file_name = url.split("/")[-1]
    download_file(url, file_name)

# Open datasets
datasets = []
for i in range(1, 13):
    file_path = f'atm_hist_1981_{i:02d}.nc4'
    dataset = xr.open_dataset(file_path)
    datasets.append(dataset)

# Define a function to download and save data
def down_data(daily_mean, start_date, end_date):
    current = start_date
    delta = timedelta(days=1)
    while current <= end_date:
        day_data = daily_mean.sel(time=current)
        day_df = day_data.to_dataframe()
        file_name = f'{current}.csv'
        day_df.to_csv(file_name)
        current += delta

year = 1981

# Specify date ranges and download data
date_ranges = [
    (datetime(year, 1, 1), datetime(year, 1, 16)),
    (datetime(year, 1, 17), datetime(year, 1, 31)),
    (datetime(year, 2, 1), datetime(year, 2, 16)),
    (datetime(year, 2, 17), datetime(year, 2, 29)),
    (datetime(year, 3, 1), datetime(year, 3, 16)),
    (datetime(year, 3, 17), datetime(year, 3, 31)),
    (datetime(year, 4, 1), datetime(year, 4, 16)),
    (datetime(year, 4, 17), datetime(year, 4, 30)),
    (datetime(year, 5, 1), datetime(year, 5, 16)),
    (datetime(year, 5, 17), datetime(year, 5, 31)),
    (datetime(year, 6, 1), datetime(year, 6, 16)),
    (datetime(year, 6, 17), datetime(year, 6, 30)),
    (datetime(year, 7, 1), datetime(year, 7, 16)),
    (datetime(year, 7, 17), datetime(year, 7, 31)),
    (datetime(year, 8, 1), datetime(year, 8, 16)),
    (datetime(year, 8, 17), datetime(year, 8, 31)),
    (datetime(year, 9, 1), datetime(year, 9, 16)),
    (datetime(year, 9, 17), datetime(year, 9, 30)),
    (datetime(year, 10, 1), datetime(year, 10, 16)),
    (datetime(year, 10, 17), datetime(year, 10, 31)),
    (datetime(year, 11, 1), datetime(year, 11, 16)),
    (datetime(year, 11, 17), datetime(year, 11, 30)),
    (datetime(year, 12, 1), datetime(year, 12, 16)),
    (datetime(year, 12, 17), datetime(year, 12, 31))
]

for idx, date_range in enumerate(date_ranges):
    down_data(datasets[idx].resample(time='D').mean(), date_range[0], date_range[1])

# Remove downloaded NetCDF files
for file_name in os.listdir():
    if file_name.endswith(".nc4"):
        os.remove(file_name)
