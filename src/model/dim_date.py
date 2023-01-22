import datetime
import holidays
import locale
import pandas as pd


class DimDate:
    def __init__(self, start: datetime.date, end: datetime.date) -> None:
        self.__ger_holiday = holidays.Germany(subdiv="BW", years=range(start.year, end.year))
        self.__date_series = pd.Series(pd.date_range(start, end))
        self.__dataframe = pd.DataFrame(self.__create_columns())

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


def main(
        filename: str = "Dim_Kalendertag.csv",
        date_from: datetime.date = datetime.date(2000, 1, 1),
        date_till: datetime.date = datetime.date(2050, 1, 1)
) -> None:
    """
    """
    assert filename.endswith(".csv")

    locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
    dim_date = DimDate(start=date_from, end=date_till)
    dim_date.dataframe.to_csv(filename, index=False)


if __name__ == "__main__":
    main()
