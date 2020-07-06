#!/usr/bin/env /python
"""This module provides a function for shipping logs to Airtable."""
import os
import time
from airtable import Airtable
from documentcloud import DocumentCloud
from twython import Twython


airtab_log = Airtable(base_key=os.environ['log_db'],
                      table_name='log',
                      api_key=os.environ['AIRTABLE_API_KEY'])

airtab_mdoc = Airtable(base_key=os.environ['other_scrapers_db'],
                       table_name='mdoc',
                       api_key=os.environ['AIRTABLE_API_KEY'])

airtab_tweets = Airtable(base_key=os.environ['botfeldman89_db'],
                         table_name='scheduled_tweets',
                         api_key=os.environ['AIRTABLE_API_KEY'])


dc = DocumentCloud(username=os.environ['MUCKROCK_USERNAME'],
                   password=os.environ['MUCKROCK_PW'])


tw = Twython(os.environ['TWITTER_APP_KEY'], os.environ['TWITTER_APP_SECRET'],
             os.environ['TWITTER_OAUTH_TOKEN'], os.environ['TWITTER_OAUTH_TOKEN_SECRET'])


muh_headers = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}


my_funcs = {'scrape_daily_pop': 'receET4GbZsIHZ1ap',
            'scrape_monthly_fact_scheets': 'recl93NdV0nvxiP6B',
            'scrape_press_releases': 'rec6iaRXOshuZ3OwY',
            'scrape_mdoc_stuff': 'recdI7ExzeoiTYI6A'}


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
