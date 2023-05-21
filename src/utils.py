# utils.py
#   Utility functions
#


# imports from Python standard library
import json
import os
from itertools import islice
from pathlib import Path
from typing import Any

# pymongo
import pymongo
import pymongo.collection
import pymongo.cursor
import pymongo.database
import pymongo.results
from pymongo import MongoClient

# other packages
import pandas as pd
from tqdm import tqdm


###################
#### CONSTANTS ####
###################

DB_CONFIG = {
    'HOST': 'localhost',
    'PORT': 27017,
    'USER': None,           # for development phase, not used
    'PASS': None,           # for development phase, not used
    'DB_NAME': 'tweets'
}


# data paths in relation to utils.py
RAW_DATA_PATHS = {
    "govt": {
        'PATH': "../data/raw/tweets-govt_entities/",
        'TYPE': 'json',
        },
    "indiv": {
        'PATH': "../data/raw/tweets-individuals/",
        'TYPE': 'json',
        },
    "news": {
        'PATH': "../data/raw/tweets-news_orgs/",
        'TYPE': 'json',
        },
    "random": {
        'PATH': "../data/raw/tweets-random/",
        'TYPE': 'json',
        },
    "troll": {
        'PATH': "../data/raw/tweets-troll/",
        'TYPE': 'csv',
        },
}


COLLECTION_NAMES = {
    'raw': 'raw',
}


TEXT_FILE_ENCODING = 'utf-8'


# used by load_csv_file() below
csv_column_dtype_mapping: dict = {
    "external_author_id": "string",
    "author": "string",
    "content": "string",
    "region": "string",
    "language": "string",
    "publish_date": "string",   # converted to datetime later
    "harvested_date": "string", # converted to datetime later
    "following": "int64",
    "followers": "int64",
    "updates": "int64",
    "post_type": "string",
    "account_type": "string",
    "retweet": "uint8",
    "account_category": "string",
    "new_june_2018": "uint8",
    "alt_external_id": "string",
    "tweet_id": "string",
    "article_url": "string",
    "tco1_step1": "string",
    "tco2_step1": "string",
    "tco3_step1": "string"
}


# Note on this constant:
#   Within 'raw' collection, average document size is 1.33 KB
#       50,000 * 1.33 KB = Approx 67 MB per chunk
#
#   MongoDB data can be divided into "shards", and shards are divided into "chunks",
#   with the default MongoDB chunk size being 128 MB. This chunk size is not
#   directly tied to MongoDB's chunk size, but the number was chosen to try to keep
#   chunks smaller than a MongoDB chunk.
CHUNK_SIZE_DEFAULT = 50000  # fifty thousand tweets (documents) per operation


##########################
#### DATABASE WRAPPER ####
##########################

class TweetDB(object):
    def __init__(self,
                 db_name:str|None = None, 
                 host:str|None = None, 
                 port:int|None = None, 
                 **kwargs
                 ):
        # process args
        self.db_name: str = db_name if (db_name is not None) else DB_CONFIG['DB_NAME']
        self._host: str = host if (host is not None) else DB_CONFIG['HOST']
        self._port: int = port if (port is not None) else DB_CONFIG['PORT']

        # instantiate a client
        #   docs: https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient
        self._client = MongoClient(  
            host=self._host,
            port=self._port,
            connect=True,       # default, reiterating for clarity
            **kwargs
            )
        
        # validate if db_name exists in server
        self._db: pymongo.database.Database = None

        if (self.db_name is not None):
            if (self.db_exists(self.db_name)):
                self._db = self._client[self.db_name]


    def db_exists(self, db_name: str) -> bool:
        db_list: list[str] = self._client.list_database_names()
        return db_name in db_list
    
    
    def collection_exists(self, collection_name: str) -> bool:        
        collection_list: list[str] = self._db.list_collection_names()
        return collection_name in collection_list
    
    
    def get_collection(self, collection_name: str) -> pymongo.collection.Collection:
        return self._db.get_collection(collection_name)
    
    
    def create_collection(self, 
                          new_collection_name: str,
                          overwrite_old_collection: bool = False
                          ) -> None:
        if (self.collection_exists(new_collection_name)):
            if (overwrite_old_collection == True):
                self.drop_collection(new_collection_name)
            else:
                print(f"create_collection: collection already exists and `overwrite_old_collection` set to False")
                return  # do nothing, don't overwrite
        else:
            self._db.create_collection(new_collection_name)

    
    def drop_collection(self, collection_name: str) -> None:
        if (self.collection_exists(collection_name) == False):
            print(f"drop_collection: collection does not exist '{collection_name=}'")
            return
        else:
            self._db.drop_collection(collection_name)
        
    
    def insert_tweets(self, 
                      tweet_list:list[dict],
                      collection_name:str,
                      validate:bool = False
                      ) -> bool|None:
        """Inserts one or more tweets into the specified Collection.

        If chunking is being performed, expectation is that chunking operation
        is upstream of this function.

        Returns boolean value or None:
          - `False` -> an error occurred (regardless of `validate` value)
          - `True` -> `validate` is True and no errors occurred (successful insert)
          - `None` -> `validate` is False and no errors occurred (successful insert)

        :Parameters:
          - `tweet_list` (`list[dict]`) - the list of tweets to insert 
                (for one tweet, still submit as a list of length 1)
          - `collection_name` (`str`) - the collection to insert tweets 
                into (displays error if collection does not exist)
          - `validate` (`bool`, optional) - adds a confirmation step to
                check that all tweets we wanted to insert actually were
                inserted. (Default: False)
        """
        # error trapping
        n_tweets: int = len(tweet_list)
        if (n_tweets == 0):
            print(f"insert_tweets: no tweets to insert; {len(tweet_list)=}")
            return False
        
        if (self.collection_exists(collection_name) == False):
            print(f"insert_tweets: collection does not exist; {collection_name=}")
            return False

        # attempt the insert operation
        #   docs for `insert_many`: https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.insert_many
        #   docs for `InsertManyResult`: https://pymongo.readthedocs.io/en/stable/api/pymongo/results.html#pymongo.results.InsertManyResult
        result: pymongo.results.InsertManyResult = self._db[collection_name].insert_many(
            documents=tweet_list,
            ordered=False,
        )

        # validate, if asked to
        if (validate):
            if (result.acknowledged):
                # simple validation on quantity of tweets reported as inserted
                n_tweets_inserted: int = len(result.inserted_ids)
                if (n_tweets == n_tweets_inserted):
                    return True
                else:
                    print(f"insert_tweets: validate error; {n_tweets=} {n_tweets_inserted=}")
                    return False
            else:
                print(f"insert_tweets: validate error; {result.acknowledged}")
                return False
        else:
            return None
        
        
    def query(self, 
              collection: str,
              query_dict: dict,
              return_fields: list[str]|dict[str, bool] = None,
              limit_results: int = 0,
              lazy: bool = True,
              **kwargs
              ) -> list[dict] | pymongo.cursor.Cursor | None:
        """Submits a query to the MongoDB (function is a wrapper for PyMongo's `find`).
        Docs for `find`: 
            https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.find
            https://www.mongodb.com/docs/compass/current/query/filter/
            https://www.mongodb.com/docs/manual/reference/operator/query/

        Comparing a MySQL query to a PyMongo query:

            SELECT
                author, text
            FROM
                raw
            WHERE
                text LIKE '%car%'
            LIMIT 100;

        In this function's form:

            result = db.query(
                collection='raw', 
                query_dict={'text': {'$regex': 'car'} },
                return_fields=['author', 'text'],
                limit_results=100
                )

        Args:
            collection (str): the name of the collection to query (collection must exist)
            query_dict (dict): the PyMongo-friendly dictionary of query criteria
            return_fields (list[str] | dict[str, bool], optional): a subset of fields to return. 
                Defaults to None.
            limit_results (int, optional): The maximum number of documents to return. When set to 0,
                no limit is imposed. Defaults to 0.
            lazy (bool, optional): When True, returns a PyMongo Cursor object (which acts like
                a Python generator). When False, fully retrieves the Cursor object results as a list.
                Defaults to True.
            
            Additional kwargs (optional) are passed on to the PyMongo `find()` method.

        Returns:
            list[dict] | pymongo.cursor.Cursor | None: The documents retreived by this query.
        """
        # check for whether collection exists
        if (not self.collection_exists(collection)):
            print(f"query: collection {collection} was not found in database, aborting query")
            return None
        
        # submit the query
        result: pymongo.cursor.Cursor = self._db.get_collection(collection).find(
            filter=query_dict,
            projection=return_fields,
            limit=limit_results,
            **kwargs
        )

        # lazy evaluate (returns a generator-like object)
        if (lazy):
            return result
        else:
            # returns a list (could be huge)
            return [doc for doc in result]


    def update_tweets(self, 
                      collection: str,
                      query_dict: dict,
                      update_dict: dict,
                      validate: bool = False,
                      verbose: bool = False,
                      **kwargs
                      ) -> None:
        """Update existing tweets within a given collection (function is a wrapper for PyMongo's `update_many`). 
        Docs for `update_many`:
            https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.update_many
            https://www.mongodb.com/docs/manual/reference/method/db.collection.updateMany/
            https://www.mongodb.com/docs/manual/reference/operator/update/#update-operators-1

        Comparing a MySQL alter to a PyMongo update:

            UPDATE some_collection
            SET
                existing_column = 'new value'
                /* , new_column = 'cant do this in an UPDATE in MySQL, need an ALTER to change schema' */
            WHERE
                tweet_id = '12345';

        In this function's form:

            update_result = db.update_tweets(
                collection='some_collection', 
                query_dict={'tweet_id': '12345'},
                update_dict={
                    '$set': {
                        'existing_column': 'new value',
                        'new_column': 'you *can* do this in MongoDB',
                    },
                }
            )

        Args:
            collection (str): The collection containing tweets to be updated
            query_dict (dict): A PyMongo-friendly dictionary used to find which tweets to modify
            update_dict (dict): A PyMongo-friendly dictionary of what updates to make
            validate (bool, optional): Performs a light validation of the update operation. 
                If MongoDB does not 'acknowledge' the update, this function raises a Runtime error.
                Defaults to False.
            verbose (bool, optional): If True, prints to console a quick summary of the result of 
                this update operation (number of tweets modified, number of tweets matched). 
                Defaults to False.

            Additional kwargs (optional) are passed on to the PyMongo `update_many()` method.

        Raises:
            RuntimeError: If `validate` is True and MongoDB does not acknowledge the update operation.

        Returns:
            None 
        """
        # check for whether collection exists
        if (not self.collection_exists(collection)):
            print(f"update_tweets: collection {collection} was not found in database, aborting query")
            return None
        
        # submit the update
        update_result: pymongo.results.UpdateResult = self.get_collection(collection).update_many(
            filter=query_dict,
            update=update_dict,
            **kwargs
        )

        # validate result
        if (validate and not update_result.acknowledged):
            raise RuntimeError(f"update_tweets: update was not acknowledged (likely unsuccessful).")

        # print some info on how things went
        if (update_result.acknowledged and verbose):
            print("update_tweets: update was acknowledged", 
                  f"\n\tnumber of tweets modified: {update_result.modified_count}",
                  f"\n\tnumber of tweets matched:  {update_result.matched_count}")


    def delete_tweets(self, 
                      collection: str,
                      query_dict: dict,
                      validate: bool = False,
                      verbose: bool = False,
                      **kwargs
                      ) -> None:
        """Deletes tweets within a collection (function is a wrapper for PyMongo's `delete_many`).
        Docs for `delete_many`:
            https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.delete_many

        Refer to docstrings for TweetDB.query() or TweetDB.update_tweets() for information on how
        to construct `query_dict`.

        Args:
            collection (str): The collection containing tweets to be deleted
            query_dict (dict): A PyMongo-friendly dictionary uised to find which tweets to delete
            validate (bool, optional): Performs a light validation of the delete operation. 
                If MongoDB does not 'acknowledge' the update, this function raises a Runtime error.
                Defaults to False.
            verbose (bool, optional): If True, prints to console a quick summer of the result of
                this delete operation (number of tweets deleted). 
                Defaults to False.
            
            Additional kwargs (optional) are passed on to the PyMongo `delete_many()` method.

        Raises:
            RuntimeError: If `validate` is True and MongoDB does not acknowledge the update operation.

        Returns:
            None
        """
        # check for whether collection exists
        if (not self.collection_exists(collection)):
            print(f"delete_tweets: collection {collection} was not found in database, aborting query")
            return None
        
        # attempt to delete
        delete_result = pymongo.results.DeleteResult = self.get_collection(collection).delete_many(
            filter=query_dict,
            **kwargs
        )

        # validate result
        if (validate and not delete_result.acknowledged):
            raise RuntimeError(f"delete_tweets: delete was not acknowledged (likely unsuccessful).")

        # print some info on how things went
        if (delete_result.acknowledged and verbose):
            print("delete_tweets: delete was acknowledged", 
                  f"\tnumber of tweets deleted: {delete_result.deleted_count}")


    def count_tweets(self,
                     collections: str|list[str],
                     approximate: bool = False
                     ) -> int | None:
        """Returns a count of the number of tweets in a collection(s). 
        Assumes collection has structure of 1 document = 1 tweet.

        Args:
            collections (str | list[str]): A collection name or list of collection names to count.
            approximate (bool, optional): Provide an approximate count (faster but potentially 
                less accurate). Defaults to False.

        Returns:
            int | None: The number of tweets in a collection (or sum of tweets in multiple collections).
        """
        if (collections is None):
            # count whole database?
            #  - would count documents that may not be tweets (e.g. "log" collection)
            print("count_tweets: `collections` provided was None, not implemented for this task")
            return None
        elif (isinstance(collections, str)):
            # count one collection
            if (self.collection_exists(collections)):
                if (approximate):
                    return self._db[collections].estimated_document_count()
                else:
                    return self._db[collections].count_documents({})
        elif (isinstance(collections, list)):
            # count multiple collections, return their sum
            sum = 0
            for collection in collections:
                if (self.collection_exists(collection)):
                    if (approximate):
                        sum += self._db[collection].estimated_document_count()
                    else:
                        sum += self._db[collection].count_documents({})
                else:
                    print(f"count_tweets: collection '{collection}' was not found in database, skipping it")
                    continue
            
            return sum

        else:
            # other types not implemented
            print("count_tweets: `collections` provided was not type `str` or `list`, not implemented for this task")
            return None


    def count_tweets_by_filter(self,
                               collection: str,
                               query_dict: dict,
                               approximate: bool = False
                               ) -> int | None:
        # check for whether collection exists
        if (not self.collection_exists(collection)):
            print(f"count_tweets_by_filter: collection {collection} was not found in database, aborting query")
            return None

        # count in one of two ways (depending on `approximate`)
        if (approximate):
            return self.get_collection(collection).estimated_document_count(query_dict)
        else:
            return self.get_collection(collection).count_documents(query_dict)

    # </class TweetDB>


########################
#### FILE FUNCTIONS ####
########################

def get_data_file_list(data_file_path: str|Path, data_file_extension:str) -> list:
    """Used to generate a list of data files (CSV/JSON) contained within a given path.
    
    Args:
      - `data_file_path` (`str` or `Path`) - the directory/path to search for files in.
            Does not search recursively, only lists files at the level specified.
            Can be relative path (if relative to working directory of calling script)
            or absolute path. Uses pathlib.Path internally so OS-agnostic.
            Example: "../data/raw/tweets-random"
      - `data_file_extension` (`str`) - the file extension (after the period) to search for.
            Example: "csv", "json"

    Returns:
      - a list of `pathlib.*` objects (`pathlib.WindowsPath` or `pathlib.PosixPath`)
    """
    # process args
    if (isinstance(data_file_path, str)):
        data_file_path = Path(data_file_path)

    if (data_file_path.exists() == False or data_file_path.is_dir() == False):
        return []
    
    # docs for `glob`: https://docs.python.org/3/library/pathlib.html#pathlib.Path.glob
    return list(data_file_path.glob(f"*.{data_file_extension}"))


def load_json_file(json_file_path: str|Path) -> list[dict] | None:
    """Loads a local JSON file into memory.

    Args:
        json_file_path (str | Path): the file to load into memory

    Returns:
        list[dict]: JSON data as Python types, or None if source file doesn't exist.
    """
    # process args
    if (isinstance(json_file_path, str)):
        json_file_path = Path(json_file_path)

    if (json_file_path.exists() == False):
        return None

    json_data: list[dict] = []
    with json_file_path.open(mode='r', encoding=TEXT_FILE_ENCODING) as json_file_handle:
        json_data = json.load(json_file_handle)

    return json_data


def load_csv_file(csv_file_path: str|Path) -> list[dict] | None:
    """Loads a local CSV file into memory and converts to list[dict] format.
    Each row in CSV file becomes a list element, with row data stored in a 
    dictionary as {column_name: value}.

    Args:
        csv_file_path (str | Path): the file to load into memory

    Returns:
        list[dict]: the CSV data represented as a list of dicts, or None if source 
        file doesn't exist.
    """
    # process args
    if (isinstance(csv_file_path, str)):
        csv_file_path = Path(csv_file_path)

    if (csv_file_path.exists() == False):
        print(f"load_csv_file: error loading file '{csv_file_path}'")
        return None
    
    csv_df: pd.DataFrame = pd.read_csv(
        csv_file_path,
        encoding=TEXT_FILE_ENCODING,
        low_memory=False,
        dtype=csv_column_dtype_mapping
    )

    # docs for `to_dict`: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_dict.html
    return csv_df.to_dict(orient="records")


def load_raw_data(db: TweetDB,
                  raw_collection_name: str = None, 
                  drop_old_data: bool = False,
                  verbose:bool = True
                  ) -> None:
    # process args
    if (raw_collection_name is None):
        raw_collection_name = COLLECTION_NAMES['raw']

    # check for existing collection
    if (db.collection_exists(raw_collection_name)):
        if (drop_old_data == False):
            print('load_raw_data: cancelled because collection exists and `drop_old_data` set to False')
            return

    # create the collection
    #   action of overwriting old collection is handled by create_collection()
    db.create_collection(
        new_collection_name=raw_collection_name,
        overwrite_old_collection=drop_old_data
        )

    # iterate over groups of raw data files
    for k, raw_data_path in RAW_DATA_PATHS.items():
        # process files based on their file extension
        if (raw_data_path['TYPE'] == 'json'):
            file_type = 'json'
            file_loader = load_json_file    # function as object
        elif (raw_data_path['TYPE'] == 'csv'):
            file_type = 'csv'
            file_loader = load_csv_file     # function as object
        else:
            print(f"load_raw_data: data file group '{raw_data_path['PATH']}' skipped;", 
                  f"not implemented for file type '{raw_data_path['TYPE']}'")
            continue

        data_file_list = get_data_file_list(raw_data_path['PATH'], file_type)

        # iterate over list of data files found in this folder
        for data_file in data_file_list:

            loaded_tweets: list[dict] = file_loader(data_file)

            result = db.insert_tweets(
                tweet_list=loaded_tweets, 
                collection_name=COLLECTION_NAMES['raw'],
                validate=True
                )

            if (result == True and verbose == True):
                print(f"load_raw_data: loaded {len(loaded_tweets):,} tweets from '{data_file.name}'.")
            elif (result == False):
                print(f"load_raw_data: error loading tweets from '{data_file.name}'.")
            else:
                pass


#########################
#### OTHER FUNCTIONS ####
#########################

def batched(cursor: pymongo.cursor.Cursor, 
            chunk_size: int = CHUNK_SIZE_DEFAULT,
            show_progress_bar: bool = False,
            progress_bar_n_chunks: int = -1
            ):  # -> generator
    """A means of retrieving chunks of results from a PyMongo cursor object.
    Based on this StackOverflow answer: https://stackoverflow.com/a/75813785/17403447
    Note this function will be included in the standard library of next Python version (3.12)
    using the same name: https://docs.python.org/3.12/library/itertools.html#itertools.batched

    Usage:
        >>> cursor = db.collection.query({'some': 'query'})
        >>> for chunk in batched(cursor, chunk_size):
        >>>     some_action(chunk)
        >>>     # some_action is applied chunk-wise for all results of find() query

    Args:
        cursor (pymongo.cursor.Cursor): The cursor returned by a find() query.
        chunk_size (int, optional): The number of tweets per chunk. Defaults to CHUNK_SIZE_DEFAULT.
        show_progress_bar (bool, optional): If True, prints tqdm progress bar. Requires a value for
            `progress_bar_n_chunks` if this arg is True. 
            Defaults to False.
        progress_bar_n_chunks (int, optional*): Required if `show_progress_bar` is True, provides the
            upper bound (total number of chunks) for a tqdm progress bar.
    """
    # quick check of chunk_size value
    if (chunk_size < 1):
        raise ValueError("batched: minimum chunk_size is 1")
    
    if (show_progress_bar and (progress_bar_n_chunks <= 0)):
        raise ValueError("batched: if `show_progress_bar` is true, must provide value for `progress_bar_n_chunks` that is greater than 0")
    
    progress_bar: tqdm = None
    if (show_progress_bar):
        progress_bar = tqdm(
            total=progress_bar_n_chunks,
            leave=True,
            desc="Number of batches"
        )

    cursor_as_iterator = iter(cursor)
    while (chunk := tuple(islice(cursor_as_iterator, chunk_size))):
        if (show_progress_bar): 
            progress_bar.update()
        yield chunk

    # finally
    if (show_progress_bar): 
        progress_bar.close()


#########################
####   MAIN METHOD   ####
#########################

# main method - do nothing
if __name__ == '__main__':
    pass
