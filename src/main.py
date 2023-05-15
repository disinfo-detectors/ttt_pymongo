# main.py
#
#   The purpose of this script is to provide a command-line interface for certain
#   functions of this project. Because this is a proof-of-concept, CLI functions
#   are limited in scope.
#   
#   As of current version, these functions are:
#       --load-data     Loads raw data from default local directory to default local database
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


# Attribution note:
#   Using sys.argv parsing approach outlined by Real Python article "Python Command-Line Arguments"
#   Source: https://realpython.com/python-command-line-arguments/#custom-parsers
USAGE = f"Usage: python {sys.argv[0]} [--load-data]\n" + \
         "  [On Windows, substitute 'py' for 'python' to use default Python launcher]"


def parse_args(args: list[str]) -> None:
    """Parse command-line arguments provided to this script.

    Available CLI arguments:
        --load-data     Loads the initial raw data into default local MongoDB database.

    Args:
        args (list[str]): the value of `sys.argv` list.
    """
    if (len(args) == 1):
        # No CLI arguments provided
        print(USAGE)
    elif (len(args) == 2):
        # Option: --load-data
        if (args[1] == "--load-data"):
            print(f"{args[0]}: loading data into local database ...", end="")
            
            db = TweetDB()   
            utils.load_raw_data(db, drop_old_data=True)
            
            print(" complete!")
        else:
            print(USAGE)
    else:
        # Too many CLI arguments provided
        print(USAGE)


# main method
if __name__ == "__main__":
    # parse command-line arguments when this script is called
    parse_args(sys.argv)
