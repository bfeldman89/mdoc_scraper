# !/usr/bin/env python
"""This module does blah blah."""
import time
from io import BytesIO
from urllib.parse import urljoin, quote
import requests
from bs4 import BeautifulSoup
from common import airtab_mdoc as airtab, dc, tw, muh_headers, wrap_from_module

wrap_it_up = wrap_from_module('mdoc_scraper/mdoc_scraper.py')


def upload_to_documentcloud(pdf, this_dict, data):
    obj = dc.documents.upload(pdf)
    while obj.status != "success":
        time.sleep(5)
        obj = dc.documents.get(obj.id)
    obj.access = "public"
    obj.data = data
    obj.title = this_dict['raw_title']
    obj.source = 'MDOC'
    obj.put()
    this_dict['dc_id'] = str(obj.id)
    this_dict['dc_title'] = obj.title
    this_dict['dc_access'] = obj.access
    this_dict['dc_pages'] = obj.pages
    this_dict['dc_url'] = obj.canonical_url
    return obj


def tweet_it(obj, tweet_txt):
    media_ids = []
    image_list = obj.normal_image_url_list[:4]
    try:
        for image in image_list:
            r = requests.get(image)
            r.raise_for_status()
            uploadable = BytesIO(r.content)
            response = tw.upload_media(media=uploadable)
            media_ids.append(response['media_id'])
        tweet = tw.update_status(status=tweet_txt, media_ids=media_ids)
    except requests.exceptions.HTTPError:
        print('error uploading page image to twitter! wtf!')
        tweet = tw.update_status(status=tweet_txt)
    return tweet['id_str']


def scrape_mdoc_stuff(url, doc_type):
    t0, i = time.time(), 0
    try:
        r = requests.get(url, headers=muh_headers)
    except requests.ConnectionError as err:
        print(f"Skipping {url}: {err}")
        time.sleep(5)
        return False
    soup = BeautifulSoup(r.text, 'html.parser')
    rows = soup.select("td.ms-vb > a[href]")
    for row in rows[0:12]:
        this_dict = {'doc_type': doc_type}
        this_dict['url'] = urljoin(url, quote(row.get('href')))
        if row.string != None:
            this_dict['raw_title'] = row.string
        else:
            this_dict['raw_title'] = 'TK'
        m = airtab.match('url', this_dict['url'])
        if not m:
            i += 1
            dc_data = {'doc_type': doc_type}
            obj = upload_to_documentcloud(this_dict['url'], this_dict, dc_data)
            new_record = airtab.insert(this_dict, typecast=True)
            tweet_txt = new_record['fields']['draft tweet']
            try:
                this_dict['tweet_id'] = tweet_it(obj, tweet_txt)
            except:
                print('tweet error')
            airtab.update(new_record['id'], this_dict, typecast=True)
    wrap_it_up(t0, new=i, total=12, function='scrape_mdoc_stuff')


def main():
    scrape_mdoc_stuff('https://www.mdoc.ms.gov/Admin-Finance/Pages/Monthly-Facts.aspx', 'monthly_fact_sheet')
    scrape_mdoc_stuff('https://www.mdoc.ms.gov/News/Pages/Press-Releases.aspx', 'press_release')
    scrape_mdoc_stuff('https://www.mdoc.ms.gov/Admin-Finance/Pages/Daily-Inmate-Population.aspx', 'daily_population')


if __name__ == "__main__":
    main()
