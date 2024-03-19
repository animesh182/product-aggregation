import logging
import azure.functions as func
from datetime import datetime, timedelta
from DailyGroupAgg.fetch_agg_group_data import extract_data
from DailyGroupAgg.import_daily_group_agg import load_data

def main(myTimer: func.TimerRequest) -> None:
    today = datetime.today()
    first_day_this_month = today.replace(day=1)
    start_date = (first_day_this_month - timedelta(days=1)).replace(day=1)
    end_date = first_day_this_month - timedelta(days=1)  

    # Extract
    agg_data = extract_data(start_date, end_date)

    # Load
    load_data(agg_data)

    logging.info("Function executed")
