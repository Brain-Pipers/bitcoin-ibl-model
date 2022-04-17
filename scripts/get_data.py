from datetime import datetime
import pandas as pd
import datetime

KAGGLE_DATASET = "scripts/data/bitstampUSD_1-min_data_2012-01-01_to_2021-03-31.csv"
KAGGLE_DATASET_NEW = "scripts/data/NewbitstampUSD_1-min_data_2012-01-01_to_2021-03-31.csv"
SAMPLE_DATASET = "scripts/data/bitstampUSD_1-min-10_SAMPLE.csv"
ONCE_PER_DAY_DATESET = "scripts/data/once_per_day_prices.csv"
EVERY_HOUR_OF_DAY_DATASET = "scripts/data/every_hour_of_day_prices.csv"


pd.options.display.max_rows = 10

def create_date_sample_data_set_from_org(org_file=KAGGLE_DATASET, sample_file_name=SAMPLE_DATASET, size=10000):
    df = pd.read_csv(org_file)
    df = df[:size]
    df.to_csv(sample_file_name)


def get_dataset(file_name=KAGGLE_DATASET):
    return pd.read_csv(file_name)

def check_date(value):
    print(value)
    return True

def get_data_2():
    df = pd.read_csv(KAGGLE_DATASET)
    df = df[df["Open"].notna()]
    df.to_csv(KAGGLE_DATASET_NEW)

def get_date(row):
    dt = datetime.datetime.fromtimestamp(row.Timestamp)
    formatted_time = dt.strftime('%Y-%m-%d')
    return formatted_time

# Get price at 8:00AM
def check_row(timestamp):
    dt = datetime.datetime.fromtimestamp(timestamp)
    formatted_time = dt.strftime('%Y-%m-%d#%H:%M')
    split_time = formatted_time.split("#")
    hour = split_time.split(":")
    return True if split_time=="08:00" else False

def get_data(size=10000):
    """Testing data"""
    df = pd.read_csv(SAMPLE_DATASET)
    df = df[df["Open"].notna()]

    print("df Before new rows")
    print(df)
    df['Date'] = df.apply(lambda row: get_date(row),axis=1)

    df.reset_index()
    dates = set()
    prices =[]
    for _,row in df.iterrows():
        if row['Date'] not in dates:
            dates.add(row['Date'])
            prices.append(row['Open'])
        


    print(dates)
    print("df After new rows")
    print(df)

    prices_df = pd.DataFrame({"Prices":prices})
    prices_df.to_csv(ONCE_PER_DAY_DATESET)

    print("-------Program Terminated! Don't Panick!-------")


def get_date_hour(row):
    dt = datetime.datetime.fromtimestamp(row.Timestamp)
    formatted_time = dt.strftime('%Y-%m-%d#%H:')
    return formatted_time

def get_date(row):
    dt = datetime.datetime.fromtimestamp(row.Timestamp)
    formatted_time = dt.strftime('%Y-%m-%d')
    return formatted_time

def get_lifetime_prices_every_hour(dataset=KAGGLE_DATASET, file_name=EVERY_HOUR_OF_DAY_DATASET):
    """Testing data"""
    df = get_dataset(file_name=dataset)
    # Remove nan 
    df = df[df["Open"].notna()]
    df['Date'] = df.apply(lambda row: get_date_hour(row),axis=1)
    df.reset_index()

    prices = []
    dates = []
    set_of_dates = set()
    for _, row in df.iterrows():
        if row['Date'] not in set_of_dates:
            prices.append(row['Open'])
            dates.append(row['Date'])
            set_of_dates.add(row['Date'])
    data = {'Prices':prices, 'Date':dates}
    prices_df = pd.DataFrame(data)
    prices_df.to_csv(file_name)

    print("-------Program Terminated! Don't Panick!-------")

def get_lifetime_prices_once_per_day(dataset=KAGGLE_DATASET, file_name=ONCE_PER_DAY_DATESET):
    """Will create a CSV that contains all the prices."""

    df = get_dataset(file_name=dataset)
    # Remove nan 
    df = df[df["Open"].notna()]
    df['Date'] = df.apply(lambda row: get_date(row),axis=1)
    df.reset_index()

    prices = []
    dates = []
    set_of_dates = set()
    for _, row in df.iterrows():
        if row['Date'] not in set_of_dates:
            prices.append(row['Open'])
            dates.append(row['Date'])
            set_of_dates.add(row['Date'])
    data = {'Prices':prices, 'Date':dates}
    prices_df = pd.DataFrame(data)
    prices_df.to_csv(file_name)
    print("Success!")