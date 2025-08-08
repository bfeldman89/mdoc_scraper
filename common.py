#!/usr/bin/env /python
"""This module provides a function for shipping logs to Airtable."""
import os
import time
from pyairtable import Api
from documentcloud import DocumentCloud
import tweepy

api = Api(os.environ['AIRTABLE_PAT'])

airtab_log = api.table(os.environ['log_db'],
                      'log')

airtab_mdoc = api.table(os.environ['other_scrapers_db'],
                       'mdoc')

airtab_covid = api.table(os.environ['xxxp_db'],
                        'mdoc_covid')

airtab_tweets = api.table(os.environ['botfeldman89_db'],
                         'scheduled_tweets')

airtab_mdoc2 = api.table(os.environ['other_scrapers_db'],
                        'covid cases per facility')

dc = DocumentCloud(username=os.environ['MUCKROCK_USERNAME'],
                   password=os.environ['MUCKROCK_PW'])



def get_twitter_conn_v1(api_key, api_secret, access_token, access_token_secret) -> tweepy.API:
    """Get twitter conn 1.1"""
    auth = tweepy.OAuth1UserHandler(api_key, api_secret)
    auth.set_access_token(
        access_token,
        access_token_secret,
    )
    return tweepy.API(auth)

def get_twitter_conn_v2(api_key, api_secret, access_token, access_token_secret) -> tweepy.Client:
    """Get twitter conn 2.0"""
    client = tweepy.Client(
        consumer_key=api_key,
        consumer_secret=api_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
    )
    return client

client_v1 = get_twitter_conn_v1(os.environ['TWITTER_APP_KEY'], os.environ['TWITTER_APP_SECRET'], os.environ['TWITTER_OAUTH_TOKEN'], os.environ['TWITTER_OAUTH_TOKEN_SECRET'])
client_v2 = get_twitter_conn_v2(os.environ['TWITTER_APP_KEY'], os.environ['TWITTER_APP_SECRET'], os.environ['TWITTER_OAUTH_TOKEN'], os.environ['TWITTER_OAUTH_TOKEN_SECRET'])



muh_headers = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
}


my_funcs = {'scrape_daily_pop': 'receET4GbZsIHZ1ap',
            'scrape_monthly_fact_scheets': 'recl93NdV0nvxiP6B',
            'scrape_press_releases': 'rec6iaRXOshuZ3OwY',
            'scrape_mdoc_stuff': 'recdI7ExzeoiTYI6A',
            'mdoc_covid_main': 'recudgPMqkvu2fmve', }


def wrap_from_module(module):
    def wrap_it_up(t0, new=None, total=None, function=None):
        this_dict = {
            'module': module,
            'function': function,
            '_function': my_funcs[function],
            'duration': round(time.time() - t0, 2),
            'total': total,
            'new': new
        }
        airtab_log.insert(this_dict, typecast=True)

    return wrap_it_up
