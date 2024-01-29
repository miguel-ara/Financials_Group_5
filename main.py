import pandas as pd
import yfinance as yf
import os
import sys
from typing import Optional
import fastavro
from io import BytesIO
import pyarrow as pa
import pyarrow.orc as orc


def load_data(year: int, year2: int) -> pd.DataFrame:
    """
    Load data from Yahoo Finance API based on the companies in the list_companies.csv file
    """
    companies = pd.read_csv("list_companies.csv")
    df = yf.Ticker(companies["ticker"][0]).history(
        start=f"{year}-01-01", end=f"{year2}-12-31"
    )
    df["Ticker"] = companies["ticker"][0]
    df["CIK"] = companies["CIK"][0]
    for i in range(1, len(companies)):
        df2 = yf.Ticker(companies["ticker"][i]).history(
            start=f"{year}-01-01", end=f"{year2}-12-31"
        )
        df2["Ticker"] = companies["ticker"][i]
        df2["CIK"] = companies["CIK"][i]
        df = pd.concat([df, df2])
    return df


def parse_args(argv: list) -> (list, str):
    """
    Parse arguments from command line.
    """
    if len(argv) < 3:
        print(
            "Format can be: \n \
                    python3 main.py <year> <format>\n \
                    python3 main.py <year>-<year> <format>\n \
                "
        )
        sys.exit(1)

    if len(argv[1]) not in [4, 9]:
        raise ValueError("Year must be in format YYYY or YYYY-YYYY")
    elif len(argv[1]) == 9:
        year = int(argv[1].split("-")[0])
        if year < 2018 or year > 2023:
            raise ValueError("Not valid year")

        year2 = int(argv[1].split("-")[1])
        if year2 < 2018 or year2 > 2023 or year2 < year:
            raise ValueError("Not valid year")

        format = argv[2]
        if format not in ["csv", "parquet", "orc", "avro", "json", "excel"]:
            raise ValueError("Format not supported")
        return [year, year2], format
    else:
        year = int(argv[1])
        if year < 2018 or year > 2024:
            raise ValueError("Not valid year")
        format = argv[2]
        if format not in ["csv", "parquet", "orc", "avro", "json", "excel"]:
            raise ValueError("Format not supported")
        return [year, year], format


def df_to_csv(df: pd.DataFrame, year: int):
    """
    Saves the DataFrame filtered by year to a CSV file.
    """
    os.makedirs(f"data/{year}", exist_ok=True)
    return df[df.index.year == year].to_csv(f"data/{year}/data_{year}.csv", index=False)


def df_to_parquet(df: pd.DataFrame, year: int):
    """
    Saves the DataFrame filtered by year to a Parquet file.
    """
    os.makedirs(f"data/{year}", exist_ok=True)
    return df[df.index.year == year].to_parquet(
        f"data/{year}/data_{year}.parquet", index=False
    )


def df_to_orc(df: pd.DataFrame, year: int):
    """
    Saves the DataFrame filtered by year to a ORC file.
    """
    os.makedirs(f"data/{year}", exist_ok=True)
    table = pa.Table.from_pandas(df[df.index.year == year])
    with open(f"data/{year}/data_{year}.orc", "wb") as f:
        orc.write_table(table, f)


def df_to_avro(df: pd.DataFrame, year: int):
    """
    Saves the DataFrame filtered by year to a Avro file.
    """

    def dataframe_to_avro(df: pd.DataFrame):
        """
        Converts a Pandas DataFrame to a Avro file.
        """

        def pandas_type_to_avro(dtype):
            if dtype == "int64":
                return "long"
            elif dtype == "float64":
                return "double"
            elif dtype == "bool":
                return "boolean"
            else:
                return "string"

        records = df.to_dict(orient="records")

        schema = {
            "doc": "Avro Schema",
            "name": "Root",
            "type": "record",
            "fields": [
                {"name": name, "type": [pandas_type_to_avro(dtype), "null"]}
                for name, dtype in df.dtypes.items()
            ],
        }

        buffer = BytesIO()
        fastavro.writer(buffer, schema, records)

        return buffer.getvalue()

    os.makedirs(f"data/{year}", exist_ok=True)
    with open(f"data/{year}/data_{year}.avro", "wb") as out:
        out.write(dataframe_to_avro(df[df.index.year == year]))


def df_to_json(df: pd.DataFrame, year: int):
    """
    Saves the DataFrame filtered by year to a JSON file.
    """
    os.makedirs(f"data/{year}", exist_ok=True)
    return df[df.index.year == year].to_json(
        f"data/{year}/data_{year}.json", orient="records"
    )


def df_to_excel(df: pd.DataFrame, year: int):
    """
    Saves the DataFrame filtered by year to a Excel file.
    """
    os.makedirs(f"data/{year}", exist_ok=True)
    return df[df.index.year == year].to_excel(
        f"data/{year}/data_{year}.xlsx", index=False
    )


def main(years: list[int, int], format: str) -> None:
    """
    Main function to save the data to the desired format.
    """

    df = load_data(*years)
    for year in range(years[0], years[1] + 1):
        if format == "csv":
            df_to_csv(df, year)
        elif format == "parquet":
            df_to_parquet(df, year)
        elif format == "orc":
            df_to_orc(df, year)
        elif format == "avro":
            df_to_avro(df, year)
        elif format == "json":
            df_to_json(df, year)
        elif format == "excel":
            df_to_excel(df, year)

    print(
        f"Data saved in {format} format for year {year} in data/{year}/data_{year}.{format}"
    )


if __name__ == "__main__":
    year, format = parse_args(sys.argv)
    main(year, format)
