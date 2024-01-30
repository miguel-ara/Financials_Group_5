# Big Data Trading for S&P 500, Financial Sector
Authors:
- _[Miguel Ara Adánez](https://github.com/miguel-ara)_
- _[Alejandro Martínez de Guinea García](https://github.com/xAlexMGGx)_
- _[Javier Prieto Domínguez](https://github.com/javiprietod)_
- _[Javier Rojo Llorens](https://github.com/JavierRojo8)_

## First Sprint
Objective: Cover requirenents I, IX and X from the project proposal document.
> **I.** A historical record of data is required from 01/01/2018 to 01/01/2024. Additionally, to store this data, the use of: Avro, Parquet, CSV, JSON, ORC, and Excel is required.

> **IX.** The source of origin is required to be Yahoo Finance.

> **X.** Detailed and justified documentation is required explaining why a Big Data architecture has been employed.


## Installation
To install the required libraries, run the following command:
```bash
pip install -r requirements.txt
```

## Description
This project arises from the need to optimize the company's Trading strategy, as the current one does not meet the minimum KPIs. The company conducts all its work in the U.S. stock market, focusing on the S&P 500. We are working in the Financial Sector, so we will focus on the companies that make up this sector. The data we will be using is from 2018 to 2024, and we will be using the data from Yahoo Finance.

## Files

#### config.json
In this file, the configuration of the project is stored.
You will need to change the following parameters according to your needs:
- **path_out**: Path where the historical data will be stored.

#### main.py
In this file, the functions for loading the data are stored. To use them you must use the following syntax:
```bash
python main.py <year> <format>
```
or:
```bash
python main.py <year>-<year> <format>
```
The first command will load the data for the specified year in the specified format. The second command will load the data for the specified range of years in the specified format.

  
