# preprocess.py
#   Preprocessing functions
#


# imports from Python standard library


# data science packages
import demoji
import numpy as np
import pandas as pd


###################
#### CONSTANTS ####
###################



#################################
#### PREPROCESSING FUNCTIONS ####
#################################

"""
    General format of preprocessing functions (not required):

        def func_name(tweet_series: pd.Series) -> [return type]:
            # access feature of tweet_series with `tweet_series['feature_name']`
            # perform some function on that feature
            # return the feature transformed as a Python primitive type (int, str, etc.)

    Example:
    
        def lowercase_tweet_text(tweet_series: pd.Series) -> str:
            tweet_text: str = tweet_series['content']
            return tweet_text.lower()

    The context calling these functions can then adopt a "map/apply" approach:

        For some pandas.DataFrame `df` with tweets stored in it:
        >>> new_column: pd.Series = df.apply(lowecase_tweet_text, axis='columns')
        >>> df['new_column'] = new_column.astype('str')

    If (later) these preprocessing functions are applied in a parallelized/distributed
    manner, e.g. through PySpark, this preparation to use a "map/apply" approach sets
    the computing portion of that parallelization up for success.
"""

def is_retweet(tweet_series: pd.Series) -> int:
    """Determines whether a tweet is a retweet.
        Assumes upstream filter step is performed to remove "NaN" values in field `referenced_tweets`.
        Returns 1 if a provided tweet (as pandas Series) is a retweet, returns 0 otherwise.
    """
    return int(tweet_series['referenced_tweets'][0]['type'] in ['retweeted', 'quoted'])


def get_post_type(tweet_series: pd.Series) -> str:
    """Examines a tweet series, returns whether it is a generic tweet, a retweet, or a quote tweet"""
    ref_twt = tweet_series['referenced_tweets']

    if (ref_twt is None):
        return None
    else:
        return ref_twt[0]['type']


def has_url(tweet_series: pd.Series, search_str: str = 'http') -> int:
    """Looks for the text `http` in a tweet's content as a means of determining 
        whether it contains a URL."""
    if (tweet_series['content'] is not None):
        return int(search_str in tweet_series['content'])
    else:
        return 0


def convert_emoji_list(tweet_series: pd.Series) -> list:
    ''' The following converts a text string with emojis into a list of descriptive text strings.
        Duplicate emojis are captured as each emoji converts to 1 text string.'''
    return demoji.findall_list(tweet_series['content'])


def convert_emoji_text(tweet_series: pd.Series, enclosing_char: str = '') -> str:
    ''' The following converts an emoji in a text string to a str '''
    return demoji.replace_with_desc(tweet_series['content'], enclosing_char)


def remove_emoji_text(tweet_series: pd.Series) -> str:
    ''' The following removes emoji from a string. '''
    return demoji.replace(tweet_series['content'], "")


def emoji_count(tweet_series: pd.Series) -> int:
    """Counts the number of emoji in an `emoji_text` field."""
    return len(tweet_series['emoji_text'])


def capture_emojis_list(series_emojis):
    """Extracts lists from a series of lists."""
    t=[]
    for i in series_emojis:
        if len(i) >1:
            t.append(i)
    return t


def flatten_emoji_list(two_dimensional_list):
    """This function takes a nested list of emoji text and flattens the list for a dataframe"""
    return [ j for i in two_dimensional_list for j in i ]


def print_emoji_top_10(emoji_flat_list):
    ''' This function takes a flattened list of emoji text and plot the value counts as a bar chart'''
    flat_array = np.array((emoji_flat_list), dtype=object)
    return pd.value_counts(flat_array).nlargest(10).plot(kind='barh',title='top 10 most frequently used emojis')


#########################
####   MAIN METHOD   ####
#########################

# main method - do nothing
if __name__ == '__main__':
    pass
