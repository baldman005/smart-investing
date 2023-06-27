import requests
from datetime import datetime, timedelta
from time import sleep
import pandas as pd
import logging
import os

subreddits = [
    'ethereum',
    'cardano',
    'Bitcoin',
    'dashpay',
    'nanocurrency',
    'tezos',
    'Polkadot',
    'Monero'
]
start_year = 2015

data_folder = 'C:/Data/Reddit'

logs_filename = 'C:/Data/Logs/reddit_data_source.log'
logs_level = logging.INFO
get_timeout = 60
max_retries = 10

def get_limit_dates(year, month, hour=12):
    """
    Get the start and end dates in Unix timestamp format for a given year and month.

    Args:
        year (int): The year.
        month (int): The month.
        Returns:
        str: The start date in Unix timestamp format.
        str: The end date in Unix timestamp format.
    """
    start_date = datetime(year, month, 1).timestamp()

    if month != 12:
        last_day = datetime(year, month + 1, 1, hour, 59, 59) + timedelta(days=-1)
    else:
        last_day = datetime(year + 1, 1, 1, hour, 59, 59) + timedelta(days=-1)
    end_date = last_day.timestamp()

    return int(start_date), int(end_date)

def save_reddit_submissions(subreddit_name, year, month):
    """
    Save submissions data for a given subreddit, year, and month.

    Args:
        subreddit_name (str): The name of the subreddit.
        year (int): The year.
        month (int): The month.
    """
    asset_type = "submission"
    if already_computed(subreddit_name, year, month, asset_type):
        return
    submissions, success = get_reddit_data(subreddit_name, year, month, asset_type)
    if not success:
        return
    data = pd.DataFrame(submissions, columns=['selftext', 'title', 'upvote_ratio', 'score', 'id', 'num_comments', 'created_utc'])

    save_reddit_data(subreddit_name, year, month, data, asset_type)

def save_reddit_comments(subreddit_name, year, month):
    """
    Save comments data for a given subreddit, year, and month.

    Args:
        subreddit_name (str): The name of the subreddit.
        year (int): The year.
        month (int): The month.
    """
    asset_type = "comment"
    if already_computed(subreddit_name, year, month, asset_type):
        return
    comments, success = get_reddit_data(subreddit_name, year, month, asset_type)
    if not success:
        return
    data = pd.DataFrame(comments, columns=['body', 'created_utc', 'controversiality', 'score', 'id'])

    save_reddit_data(subreddit_name, year, month, data, asset_type)

def already_computed(subreddit_name, year, month, asset_type):
    file_path = get_file_path(subreddit_name, year, month, asset_type)
    already_exists = os.path.exists(file_path)

    if already_exists:
        logging.debug(f"{file_path} already exists")

    return already_exists

def get_file_path(subreddit_name, year, month, asset_type):
    return os.path.join(data_folder, subreddit_name, asset_type, f"{year}_{month}.csv")

def save_reddit_data(subreddit_name, year, month, data, asset_type):
    """
    Saves Reddit data to a CSV file and logs the action.

    Args:
        subreddit_name (str): Name of the subreddit.
        year (int): Year of the data.
        month (int): Month of the data.
        data (pandas.DataFrame): Data to be saved.
        asset_type (str): Type of asset (e.g. submission, comment).
    """
    try:
        file_path = get_file_path(subreddit_name, year, month, asset_type)
        directory = os.path.dirname(file_path)
        os.makedirs(directory, exist_ok=True)

        asset_type += "s"
        data = data.set_index("id")
        data.to_csv(file_path, index=True)

        asset_type = asset_type[0].upper() + asset_type[1:].lower()
        logging.info(f"{asset_type} data saved for {subreddit_name} - Year: {year}, Month: {month}")
    except Exception as e:
        logging.error(f"Failed to save {asset_type}s for {subreddit_name} - Year: {year}, Month: {month}. Error: {str(e)}")

def get_reddit_data(subreddit_name, year, month, asset_type):
    """
    Gets Reddit data from Pushshift API and logs the action.

    Args:
        subreddit_name (str): Name of the subreddit.
        year (int): Year of the data.
        month (int): Month of the data.
        asset_type (str): Type of asset (e.g. submission, comment).

    Returns:
        List: List of data obtained from the API.
    """
    start_date, end_date = get_limit_dates(year, month)
    params = {
        'subreddit': subreddit_name,
        'after': start_date,
        'before': end_date,
        'q': '',
        'size': 500
    }
    base_url = f"https://api.pushshift.io/reddit/search/{asset_type}/"
    assets = []
    attempts = 0
    logging.info(f"Start getting {asset_type}s for {subreddit_name} - Year: {year}, Month: {month}.")
    while True:
        if attempts >= max_retries:
            break

        try:
            response = requests.get(base_url, params=params, timeout=get_timeout)
            data = response.json()['data']
            new_entries = len(data)
            if len(data) == 0:
                break
            assets += data

            min_creation_date = min([comment["created_utc"] for comment in data])
            params["before"] = min_creation_date - 1
            logging.debug(f"Found more {new_entries} {asset_type}s for {subreddit_name} - Year: {year}, Month: {month}.")
            sleep(2)
            attempts = 0
        except Exception as e:
            logging.warning(f"Failed to get {asset_type}s for {subreddit_name} - Year: {year}, Month: {month}. Error: {str(e)}")
            attempts += 1
            sleep(10)

    if attempts < max_retries:
        logging.info(f"{len(assets)} {asset_type}s extracted for {subreddit_name} - Year: {year}, Month: {month}")
        return assets, True
    else:
        logging.error(f"Failed to get {asset_type}s for {subreddit_name} - Year: {year}, Month: {month} after {max_retries} attempts")
        return [], False

def extract_subreddit_data(subreddit_name):
    """
    Extracts data from a subreddit for multiple years and months.

    Args:
        subreddit_name (str): Name of the subreddit.
    """
    current_year = datetime.now().year
    current_month = datetime.now().month

    for year in range(start_year, current_year + 1):
        max_month = 12 if year != current_year else current_month
        for month in range(1, max_month + 1):
            save_reddit_submissions(subreddit_name, year, month)
            save_reddit_comments(subreddit_name, year, month)

def main():
    for subreddit in subreddits:
        extract_subreddit_data(subreddit)

if __name__ == "__main__":
    logging.basicConfig(filename=logs_filename, level=logs_level, format='%(asctime)s - %(levelname)s - %(message)s')
    main()