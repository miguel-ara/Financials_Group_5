import sys
from functions import *



def main(years: list[int, int], format: str) -> None:
    """
    Main function to save the data to the desired format.
    """
    df = load_data(*years)
    for year in range(years[0], years[1] + 1):
        f = Functions(df, year)
        if format == "csv":
            f.df_to_csv()
        elif format == "parquet":
            f.df_to_parquet()
        elif format == "orc":
            f.df_to_orc()
        elif format == "avro":
            f.df_to_avro()
        elif format == "json":
            f.df_to_json()
        elif format == "excel":
            f.df_to_excel()

    print(
        f"Data saved in {format} format for year {year} in data/{year}/data_{year}.{format}"
    )


if __name__ == "__main__":
    year, format = parse_args(sys.argv)
    main(year, format)
