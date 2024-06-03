import logging
import azure.functions as func
from datetime import datetime, timedelta
from DailyGroupAgg.fetch_agg_group_data import extract_data
from DailyGroupAgg.import_daily_group_agg import load_data


def main(myTimer: func.TimerRequest) -> None:
    today = datetime.today()
    # first day of current month
    start_date = today.replace(day=1).date()
    # first day of next month
    first_day_next_month = (start_date + timedelta(days=32)).replace(day=1)

    # Then, subtract one day from the first day of next month to get the last day of the current month
    end_date = first_day_next_month - timedelta(days=1)
    # Extract
    agg_data = extract_data(start_date, end_date)
    # Load
    load_data(agg_data)

    logging.info("Function executed")
