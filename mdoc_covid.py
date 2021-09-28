# !/usr/bin/env python
"""This module does blah blah."""
import re
import time
import unicodedata

from io import BytesIO

import requests

from PyPDF2 import PdfFileReader

from common import airtab_mdoc, airtab_mdoc2, dc, tw, muh_headers, wrap_from_module

wrap_it_up = wrap_from_module('mdoc_scraper/mdoc_covid.py')


def tweet_it(obj, tweet_txt):
    media_ids = []
    image_list = obj.normal_image_url_list[:2]
    for image in image_list:
        r = requests.get(image)
        r.raise_for_status()
        uploadable = BytesIO(r.content)
        response = tw.upload_media(media=uploadable)
        media_ids.append(response['media_id'])
    tweet = tw.update_status(status=tweet_txt, media_ids=media_ids)
    return tweet['id_str']


def web_to_dc(this_dict):
    obj = dc.documents.upload(this_dict['url'])
    while obj.status != "success":
        time.sleep(7.5)
        obj = dc.documents.get(obj.id)
    obj.access = "public"
    obj.data = {'doc_type': 'covid_update'}
    obj.title = this_dict['raw_title']
    obj.source = 'MDOC'
    obj.put()
    time.sleep(5)
    this_dict['dc_id'] = str(obj.id)
    this_dict['dc_title'] = obj.title
    this_dict['dc_access'] = obj.access
    this_dict['dc_pages'] = obj.pages
    this_dict['dc_p1_txt'] = unicodedata.normalize("NFKD", obj.get_page_text(1))
    this_dict['dc_full_text'] = unicodedata.normalize("NFKD", obj.full_text)
    this_dict['dc_pdf'] = obj.pdf_url
    this_dict['dc_url'] = obj.canonical_url
    this_dict['dc_txt_url'] = obj.full_text_url
    full_txt_lines = this_dict['dc_p1_txt'].splitlines()
    if full_txt_lines[0] in {'COVID‐19 Confirmed Inmate Cases', 'COVID‐19 Confirmed Cases', 'COVID-19 Confirmed Inmate Cases'}:
        date_hit = re.search(r'Last Update.*', this_dict['dc_p1_txt'])
        this_dict['last_updated'] = date_hit[0].replace(
            'Last Update:',
            ''
        ).replace(
            '2020 ',
            '2020 at '
        ).strip().replace(
            '\x00',
            ''
        ).replace('December', 'Dec.')
        numbers_hit = re.search(r'TOTALS\s\d,\d+\*+\s\d+', this_dict['dc_p1_txt'])
        m = re.match(r"TOTALS\s(\d,\d+)\*+\s(\d+)", numbers_hit[0])
        this_dict['total_cases'] = m.group(1).replace(',', '')
        this_dict['total_active_cases'] = m.group(2)
        this_dict['tweet_msg'] = (
            f"According to MDOC, as of {this_dict['last_updated']}, a total of {this_dict['total_cases']} MS inmates "
            f"have tested positive for COVID-19, and {this_dict['total_active_cases']} cases are active. {this_dict['dc_url']}"
        )
        this_dict['tweet_id'] = tweet_it(obj, this_dict['tweet_msg'])
        new_rec = airtab_mdoc.insert(this_dict, typecast=True)
        time.sleep(5)
        scrape_covid_cases_per_facility(record_id=new_rec['id'])
    elif full_txt_lines[0].strip() == 'Answers to some of the most frequently asked questions:':
        this_dict['last_updated'] = full_txt_lines[1].replace(
            'Last Update:',
            ''
        ).replace(
            ', 2020, ',
            ', 2020 at '
        ).strip().replace(
            '\x00',
            ''
        )
        this_dict['last_updated_abrev'] = this_dict['last_updated'][:this_dict['last_updated'].find(', ')].replace('December', 'Dec.')
        scrape_q_and_a(this_dict)
    else:
        print('WTF! The first line of the pdf was: ', full_txt_lines[0].strip())


def scrape_q_and_a(this_dict):
    obj = dc.documents.get(int(this_dict['dc_id']))
    txt = re.sub(r"(?<=\d),(?=\d)", "", this_dict['dc_full_text']).replace(' one ', ' 1 ').replace(' two ', ' 2 ').replace(' three ', ' 3 ').replace(' four ', ' 4 ').replace(' five ', ' 5 ').replace(' six ', ' 6 ').replace(' seven ', ' 7 ').replace(' eight ', ' 8 ').replace(' nine ', ' 9 ')
    this_dict['inmates_pos'] = re.search(r'(\d+) confirmed positive cases', txt)[1]
    this_dict['inmates_pos_active'] = re.search(r'and of that number,.*(\d+)', txt)[1]
    this_dict['inmates_neg'] = re.search(r'(\d+) inmates to test negative', txt)[1]
    this_dict['staff_pos'] = re.search(r'(\d+) positive cases among staff', txt)[1]
    this_dict['staff_neg'] = re.search(r'(\d+) negative tests have been reported', txt)[1]
    excerpt = f"There's been {this_dict['inmates_pos']} confirmed positive cases among inmates ({this_dict['inmates_pos_active']} active). {this_dict['inmates_neg']} inmates have tested negative. MDOC reports {this_dict['staff_pos']} employees have tested positive & {this_dict['staff_neg']} negative."
    this_dict['tweet_msg'] = f"As of {this_dict['last_updated_abrev']}, {excerpt} {this_dict['dc_url']}"
    this_dict['tweet_id'] = tweet_it(obj, this_dict['tweet_msg'])
    airtab_mdoc.insert(this_dict, typecast=True)


def scrape_covid_cases_per_facility(record_id):
    rec = airtab_mdoc.get(record_id)
    this_dict = {}
    this_dict['iso'] = rec['fields']['iso']
    txt = re.sub(r"(?<=\d),(?=\d)", "", rec['fields']['dc_p1_txt'])
    m1 = re.search('Last Update:', txt)
    lines = txt[:m1.start()].splitlines()
    for line in lines:
        m = re.findall(r'\d+', line)
        if m:
            if len(m) == 2:
                cases = m[0]
                active_cases = m[1]
                facility = line.replace(cases, '').replace(active_cases, '').replace('*', '').replace('-', '‐').strip()
                this_dict[facility] = cases
                this_dict[f"{facility} (active)"] = active_cases
    airtab_mdoc2.insert(this_dict, typecast=True)


def main():
    t0, i = time.time(), 0
    urls = {'Confirmed Cases: State, Private and Regional Facilities': 'https://www.mdoc.ms.gov/Documents/covid-19/Inmates%20cases%20chart.pdf',
            'MDOC Questions and Answers for COVID-19': 'https://www.mdoc.ms.gov/Documents/covid-19/QA-Questions%20and%20Answers.pdf'}
    for raw_title, url in urls.items():
        this_dict = {'doc_type': 'covid_update'}
        r = requests.get(url, headers=muh_headers)
        if r.status_code != 200:
            print(f'The url is broken. status code: {r.status_code}')
            return False
        this_dict['raw_title'] = raw_title
        this_dict['url'] = url
        with BytesIO(r.content) as f:
            this_pdf = PdfFileReader(f)
            information = dict(this_pdf.getDocumentInfo())
        this_dict['pdf_author'] = information.get('/Author')
        this_dict['pdf_creator'] = information.get('/Creator')
        this_dict['pdf_mod_datetime'] = information.get('/ModDate').replace("'", '')
        this_dict['pdf_creation_datetime'] = information.get('/CreationDate').replace("'", '')
        this_dict['pdf_producer'] = information.get('/Producer')
        matching_record = airtab_mdoc.match('pdf_mod_datetime', this_dict['pdf_mod_datetime'])
        if matching_record:
            print('nothing new. nothing changed. --> ', this_dict['url'])
        else:
            i += 1
            web_to_dc(this_dict)
    wrap_it_up(t0, new=i, total=2, function='mdoc_covid_main')


if __name__ == "__main__":
    main()
