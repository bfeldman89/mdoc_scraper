#!/usr/bin/env python
import io
import os
import time

import requests
import send2trash
from pdf2image import convert_from_bytes
from PyPDF2 import PdfFileReader

from common import airtab_covid as airtab
from common import tw

url = 'https://www.mdoc.ms.gov/Documents/Inmates%20cases%20chart%20July%2014.pdf'


def extract_information():
    response = requests.get(url)
    if response.status_code != 200:
        print(f'The url is broken. status code: {response.status_code}')
        return False
    this_dict = {}
    with io.BytesIO(response.content) as f:
        this_pdf = PdfFileReader(f)
        this_dict['number_of_pages'] = this_pdf.getNumPages()
        information = dict(this_pdf.getDocumentInfo())
        this_dict['p1_txt'] = this_pdf.getPage(0).extractText()
    this_dict['author'] = information.get('/Author')
    this_dict['creator'] = information.get('/Creator')
    this_dict['modification_datetime'] = information.get('/ModDate').replace("'", '')
    this_dict['creation_datetime'] = information.get('/CreationDate').replace("'", '')
    this_dict['producer'] = information.get('/Producer')

    s_date = this_dict['p1_txt'].find('Last Update:') + 12
    if s_date != 11:
        this_dict['raw_datetime'] = this_dict['p1_txt'][s_date:].strip().replace('\n', ' ')
        s_total = this_dict['p1_txt'].find('TOTAL') + 5
        this_dict['total_cases'] = this_dict['p1_txt'][s_total:s_date-12].strip()
    # this_dict['pdf'] = [{"url": url}]
    matching_record = airtab.match('modification_datetime', this_dict['modification_datetime'])
    if matching_record:
        print("mdoc covid pdf hasn't been updated")
        return False
    new_record = airtab.insert(this_dict, typecast=True)
    return new_record['id']


def get_images():
    the_media_ids = []
    os.chdir(f"/{os.getenv('HOME')}/code/mdoc_scraper/output")
    response = requests.get(url)
    pages = convert_from_bytes(response.content)
    for idx, page in enumerate(pages):
        this_fn = f'page_{idx + 1}.jpg'
        page.save(this_fn, 'JPEG')
        time.sleep(2)
        photo = open(this_fn, 'rb')
        response = tw.upload_media(media=photo)
        the_media_ids.append(response['media_id'])
    return the_media_ids


def tweet_with_images(rid, mids):
    record = airtab.get(rid)
    tw.update_status(status=record['fields']['msg'], media_ids=mids)
    os.chdir(f"/{os.getenv('HOME')}/code/mdoc_scraper/output")
    for fn in os.listdir('.'):
        send2trash.send2trash(fn)


if __name__ == '__main__':
    new_rid = extract_information()
    if new_rid:
        media_ids = get_images()
        tweet_with_images(rid=new_rid, mids=media_ids)
    else:
        print('no updates to mdoc covid pdf')
