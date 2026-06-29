import pandas as pd


def make_nbp_silver_transformation(
    rates_list: list[dict], column_mapping: dict
) -> pd.DataFrame:
    """
    Transform raw NBP exchange rate records into a normalized DataFrame.

    The function:
    - Creates a DataFrame from a list of dictionaries.
    - Renames columns according to the provided mapping.
    - Converts the ``average_rate`` value from a comma-separated string to
    a floating-point number.
    - Converts the ``rate`` column to an integer.
    - Calculates the normalized exchange rate by dividing
    ``average_rate`` by ``rate``.
    - Converts ``effective_date`` to a Python ``date`` object.

    Parameters
    ----------
    rates_list : list[dict]
        Raw exchange rate records retrieved from the NBP source.
    column_mapping : dict
        Mapping of source column names to target column names.

    Returns
    -------
    pd.DataFrame
        A transformed DataFrame containing normalized exchange rate data,
        including the calculated ``normalize_rate`` column.
    """
    df = pd.DataFrame.from_records(rates_list).rename(
        columns=column_mapping,
    )
    df["average_rate"] = df["average_rate"].str.replace(",", ".")
    df["rate"] = df["rate"].astype(int)
    df["average_rate"] = df["average_rate"].astype(float)
    df["normalize_rate"] = df["average_rate"] / df["rate"]
    df["effective_date"] = pd.to_datetime(df["effective_date"]).dt.date
    return df


def get_searched_date_strings(logical_date: str):
    """
    Extract year, month, and day strings from a logical date.

    The date is formatted as ``YYMMDD`` and split into separate
    two-character strings representing the year, month, and day.

    Parameters
    ----------
    logical_date : datetime.datetime
        The logical execution date.

    Returns
    -------
    tuple[str, str, str]
        A tuple containing:
        - year (``YY``)
        - month (``MM``)
        - day (``DD``)
    """
    run_date = logical_date.strftime("%y%m%d")
    searched_year = run_date[:2]
    searched_month = run_date[2:4]
    searched_day = run_date[4:6]
    return searched_year, searched_month, searched_day
