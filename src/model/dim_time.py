import locale
import pandas as pd


class DimTime:
    HOURS = [i for i in range(0, 24)]
    MINUTES = [i for i in range(0, 60)]
    SECONDS = [i for i in range(0, 60)]

    def __init__(self) -> None:
        columns, data = self.__create_columns()
        self.__dataframe = pd.DataFrame(data, columns=columns)

    @property
    def dataframe(self) -> pd.DataFrame:
        return self.__dataframe

    def __create_columns(self) -> tuple[list[str], list[list]]:
        columns = [
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
        data = []

        for hour in self.HOURS:
            for minute in self.MINUTES:
                for second in self.SECONDS:
                    data.append([
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

        return columns, data


def main(filename: str = "Dim_Zeit.csv") -> None:
    assert filename.endswith(".csv")

    locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
    dim_date = DimTime()
    dim_date.dataframe.to_csv(filename, index=False)


if __name__ == "__main__":
    main()
