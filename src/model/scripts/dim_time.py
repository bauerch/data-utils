import argparse
import locale
import pandas as pd


class DimTime:
    HOURS = [i for i in range(0, 24)]
    MINUTES = [i for i in range(0, 60)]
    SECONDS = [i for i in range(0, 60)]

    def __init__(self) -> None:
        data = self.__create_rows()
        self.__dataframe = pd.DataFrame(data, columns=self.columns)

    @property
    def dataframe(self) -> pd.DataFrame:
        return self.__dataframe

    @property
    def columns(self) -> list[str]:
        return [
            "Zeit_Key",
            "Stunde_Format_24",
            "Stunde_Format_24_Text",
            "Stunde_Format_24_Kurz_Text",
            "Stunde_Format_24_Lang_Text",
            "Minute_Key",
            "Minute",
            "Minute_Text",
            "Minute_Kurz_Text",
            "Minute_Lang_Text",
            "Sekunde",
            "Sekunde_Text",
            "Zeit_Kurz_Text",
            "Zeit_Lang_Text"
        ]

    def __create_rows(self) -> list[list]:
        rows = []

        for hour in self.HOURS:
            for minute in self.MINUTES:
                for second in self.SECONDS:
                    rows.append([
                        10000 * hour + 100 * minute + second,
                        hour,
                        f"{hour:02d}",
                        f"{hour:02d}:00",
                        f"{hour:02d}:00:00",
                        hour * 100 + minute,
                        minute,
                        f"{minute:02d}",
                        f"{hour:02d}:{minute:02d}",
                        f"{hour:02d}:{minute:02d}:00",
                        second,
                        f"{second:02d}",
                        f"{hour:02d}:{minute:02d}:{second:02d}",
                        f"{hour:02d}:{minute:02d}:{second:02d}.000"
                    ])

        return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a Time Dimension for a Data Warehouse. "
        + "The output will be written to a CSV file unless a file "
        + "is specified with the -f/--file option."
    )
    parser.add_argument(
        "-f",
        "--file",
        dest="filename",
        help="Write data to a file",
        metavar="FILE"
    )
    args = parser.parse_args()

    locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
    dim_date = DimTime()

    if args.filename.endswith(".csv"):
        dim_date.dataframe.to_csv(args.filename, index=False)
    elif args.filename.endswith(".parquet"):
        dim_date.dataframe.to_parquet(args.filename)
    else:
        dim_date.dataframe.to_csv("Dim_Zeit.csv", index=False)


if __name__ == "__main__":
    main()
