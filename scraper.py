import sys
import argparse
import collections
import datetime
import hashlib
import json
import time

import dateutil
import pandas as pd
import pymongo
import pytz
import requests


ENDPOINT = 'https://maps.northwestern.edu/api/shuttles'
TZ_CHI = pytz.timezone('America/Chicago')


def get():
    response = requests.get(ENDPOINT, headers={'Accept': 'application/json'})
    assert response.status_code == 200
    return response.json()['bus']


def rowhash(row):
    rowhash = hashlib.md5(json.dumps(row, separators=',:', sort_keys=True).encode('utf-8'))
    return rowhash.hexdigest()


def parse(raw):
    data = []
    for bus in raw:
        datum = {
            'id': bus['bus_id'],
            'lat': round(float(bus['geocode']['lat']), 5),
            'lon': round(float(bus['geocode']['lon']), 5),
            'route_id': bus['geocode']['route_id'],
            'route_no': bus['geocode']['route_no'],
        }

        # usually nothing here.
        # try:
        #     if bus['geocode']['speed']:
        #         datum['speed'] = bus['geocode']['speed']
        # except KeyError:
        #     pass

        try:
            if bus['geocode']['type'].lower().strip() == 'cta':
                datum['type'] = 'CTA'
            else:
                datum['type'] = 'NU'
        except KeyError:
            datum['type'] = 'NU'

        # CTA data seems to omit this
        try:
            if bus['geocode']['lastStop']:
                datum['last_stop'] = bus['geocode']['lastStop']
        except KeyError:
            pass

        if datum['type'] == 'NU':
            datum['direction'] = bus['geocode']['direction']
            t = datetime.datetime.fromtimestamp(float(bus['geocode']['lastUpdate']))

        elif datum['type'] == 'CTA':
            t = datetime.datetime.strptime(bus['geocode']['lastUpdate'], '%Y%m%d %H:%M')

        datum['updated'] = int(TZ_CHI.localize(t).timestamp())

        datum['hash'] = rowhash(datum)

        data.append(datum)

    return data


def insert_data(collection, data):
    bulk = pymongo.bulk.BulkOperationBuilder(collection, ordered=False)

    for doc in data:
        doc['_id'] = doc.pop('hash')[:24]
        bulk.find({'_id': doc['_id']}).upsert().update_one({'$setOnInsert': doc})

    return bulk.execute()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('runtime', type=int)
    args = parser.parse_args()

    with open('credentials.json') as f:
        creds = json.load(f)

    client = pymongo.MongoClient('mongodb://{user}:{pass}@{server}/'.format(**creds))
    collection = client['transit']['nubus']

    run_til = time.monotonic() + args.runtime

    T = 10 # seconds
    Tmax = 5 * 60 # seconds

    no_results = 0

    known = collections.deque([], 1024)

    while time.monotonic() < run_til:

        loop_time = min(T * (2 ** min(10, no_results)), Tmax)
        time.sleep(loop_time)

        data = get()

        if len(data):
            no_results = 0
        else:
            no_results += 1
            continue

        data = parse(data)

        data = [r for r in data if rowhash(r) not in known]
        known.extendleft(rowhash(r) for r in data)

        insert_data(collection, data)


if __name__ == '__main__':
    sys.exit(main())
