# main.py
#   driver script
#


# imports from Python standard library
from pathlib import Path
import sys

# local imports
import utils
import preprocess
from utils import TweetDB

# pymongo
import pymongo

# data science packages


# main method
if __name__ == "__main__":
    db = TweetDB()

    utils.load_raw_data(db, drop_old_data=True)

    # test_csv = Path('../data/raw/tweets-troll/IRAhandle_tweets_1.csv')

    # my_path = Path()
    # print(my_path)
    # print(my_path.absolute())

    # if test_csv.exists():
    #     test_data = utils.load_csv_file(test_csv)

    #     print(test_data[0])

    #     db.insert_tweets(test_data, 'raw')
    