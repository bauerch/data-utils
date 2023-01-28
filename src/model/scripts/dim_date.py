import argparse
import datetime
import holidays
import locale
import pandas as pd


class DimDate:
    def __init__(self, start: datetime.date, end: datetime.date) -> None:
        self.__ger_holiday = holidays.Germany(subdiv="BW", years=range(start.year, end.year))
        self.__date_series = pd.Series(pd.date_range(start, end))
        self.__dataframe = pd.DataFrame(self.__create_columns())

        keys = self.column_date_key()
        self.__dataframe.insert(0, keys[0], keys[1])

    @property
    def dataframe(self) -> pd.DataFrame:
        return self.__dataframe

    def __create_columns(self) -> dict[str, pd.Series]:
        column_data = {"Datum": self.__date_series}
        handlers = [
            self.column_year(),
            self.column_quarter(),
            self.column_quarter_name(),
            self.column_quarter_name_year(),
            self.column_month(),
            self.column_month_text(),
            self.column_month_name_short(),
            self.column_month_name_long(),
            self.column_month_name_long_year(),
            self.column_week_year(),
            self.column_week_year_text(),
            self.column_day_month(),
            self.column_day_month_text(),
            self.column_day_name_short(),
            self.column_day_name_long(),
            self.column_is_holiday_bw(),
            self.column_holiday_name_bw()
        ]

        column_data.update({column: series for column, series in handlers})

        return column_data

    def column_template(self) -> tuple[str, pd.Series]:
        column = ""
        series = self.__date_series
        return column, series

    def column_date_key(self) -> tuple[str, pd.Series]:
        column = "Datum_Key"
        series = self.__date_series.apply(lambda x: 10000 * x.year + 100 * x.month + x.day)
        return column, series

    def column_year(self) -> tuple[str, pd.Series]:
        column = "Jahr"
        series = self.__date_series.dt.year
        return column, series

    def column_quarter(self) -> tuple[str, pd.Series]:
        column = "Quartal"
        series = self.__date_series.dt.quarter
        return column, series

    def column_quarter_name(self) -> tuple[str, pd.Series]:
        column = "Quartal_Name"
        series = self.__date_series.apply(lambda x: f"Q{x.quarter}")
        return column, series

    def column_quarter_name_year(self) -> tuple[str, pd.Series]:
        column = "Quartal_Name_Jahr"
        series = self.__date_series.apply(lambda x: f"Q{x.quarter} {x.strftime('%Y')}")
        return column, series

    def column_month(self) -> tuple[str, pd.Series]:
        column = "Monat"
        series = self.__date_series.dt.month
        return column, series

    def column_month_text(self) -> tuple[str, pd.Series]:
        column = "Monat_Text"
        series = self.__date_series.apply(lambda x: f"{x.month:02d}")
        return column, series

    def column_month_name_short(self) -> tuple[str, pd.Series]:
        column = "Monat_Name_Kurz"
        series = self.__date_series.dt.strftime("%b")
        return column, series

    def column_month_name_long(self) -> tuple[str, pd.Series]:
        column = "Monat_Name_Lang"
        series = self.__date_series.dt.strftime("%B")
        return column, series

    def column_month_name_long_year(self) -> tuple[str, pd.Series]:
        column = "Monat_Name_Lang_Jahr"
        series = self.__date_series.dt.strftime("%B %Y")
        return column, series

    def column_week_year(self) -> tuple[str, pd.Series]:
        column = "Woche_Jahr"
        series = self.__date_series.dt.isocalendar().week
        return column, series

    def column_week_year_text(self) -> tuple[str, pd.Series]:
        column = "Woche_Jahr_Text"
        series = self.__date_series.apply(
            lambda x: f"Woche {x.isocalendar()[1]}, {x.isocalendar()[0]}"
        )
        return column, series

    def column_day_month(self) -> tuple[str, pd.Series]:
        column = "Tag_im_Monat"
        series = self.__date_series.dt.day
        return column, series

    def column_day_month_text(self) -> tuple[str, pd.Series]:
        column = "Tag_im_Monat_Text"
        series = self.__date_series.apply(lambda x: f"{x.day:02d}")
        return column, series

    def column_day_name_short(self) -> tuple[str, pd.Series]:
        column = "Tag_Name_Kurz"
        series = self.__date_series.dt.strftime("%a")
        return column, series

    def column_day_name_long(self) -> tuple[str, pd.Series]:
        column = "Tag_Name_Lang"
        series = self.__date_series.dt.strftime("%A")
        return column, series

    def column_is_holiday_bw(self) -> tuple[str, pd.Series]:
        column = "Feiertag_BW"
        series = self.__date_series.isin(self.__ger_holiday)
        return column, series

    def column_holiday_name_bw(self) -> tuple[str, pd.Series]:
        column = "Feiertag_Name_BW"
        series = self.__date_series.apply(lambda x: f"{self.__ger_holiday.get(x)}")
        return column, series


def main() -> None:
    """
    """
    parser = argparse.ArgumentParser(
        description="Build a Date Dimension for a Data Warehouse. "
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
    parser.add_argument(
        "--from",
        dest="date_from",
        default="01.01.2000",
        help="Starting date of the date dimension. Default is 01.01.2000",
        metavar="DATE"
    )
    parser.add_argument(
        "--till",
        dest="date_till",
        default="31.12.2049",
        help="Ending date for the date dimension. Default is 31.12.2049",
        metavar="DATE"
    )
    args = parser.parse_args()

    try:
        date_from = datetime.datetime.strptime(args.date_from, "%d.%m.%Y")
        date_till = datetime.datetime.strptime(args.date_till, "%d.%m.%Y")
    except ValueError as error:
        print(
            f"Unable to create the date dimension for the given dates "
            f"{args.date_from} and {args.date_till}. Error details: {error!r}"
        )
        parser.print_help()
    else:
        locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
        dim_date = DimDate(start=date_from, end=date_till)

        if args.filename.endswith(".csv"):
            dim_date.dataframe.to_csv(args.filename, index=False)
        elif args.filename.endswith(".parquet"):
            dim_date.dataframe.to_parquet(args.filename)
        else:
            dim_date.dataframe.to_csv("Dim_Kalendertag.csv", index=False)


if __name__ == "__main__":
    main()
