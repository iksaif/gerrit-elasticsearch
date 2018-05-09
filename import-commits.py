"""Import data to elasticsearch."""

from concurrent import futures
import argparse
import datetime
import elasticsearch
import json
import os
import sys
import threading
import glob


INDEX = 'gerrit-commits'
INDEX_BODY = {
    "settings": {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 2,
        },
    },
    "mappings": {
        "metric": {
            "properties": {
                "createdOn": {
                    "type": "date",
                    "format": "epoch_second"
                },
                "lastUpdated": {
                    "type": "date",
                    "format": "epoch_second"
                },
            }
        }
    },
}


def read_commits(filename):
    """Read metrics from the filename."""
    for entry in glob.glob(filename):
        with open(entry) as fp:
            data = json.load(fp)
            for commit in data['commits']:
                yield commit


def create(es, commit):
    """Creates a metric."""

    # Make things easier to use for ES, get the last approvals by type.
    if 'patchSets' in commit:
        patchset = commit['patchSets'][-1]
        if 'approvals' in patchset:
            ret = {}
            for approval in patchset['approvals']:
                ret[approval['type']] = approval
            commit['approvalsByType'] = ret

    es.create(
        index=INDEX,
        doc_type="commit",
        id=commit['number'],
        body=commit,
        ignore=409,
    )


def callback(future, sem):
    """Consume the future."""
    sem.release()
    try:
        future.result()
    except Exception as e:
        print(str(e))


def parse_opts(args):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Import data to ElasticSearch.')
    parser.add_argument('--max_workers', default=75, type=int)
    parser.add_argument('--username', default=os.getenv('ES_USERNAME'))
    parser.add_argument('--password', default=os.getenv('ES_PASSWORD'))
    parser.add_argument('--cluster', default='127.0.0.1')
    parser.add_argument('--sniff', action='store_true', default=False)
    parser.add_argument('--port', default=9002, type=int)
    parser.add_argument('--cleanup', action='store_true', default=False)
    parser.add_argument('--input', required=True)
    return parser.parse_args(args[1:])


def main():
    """Import .csv data from a metrics_metadata dump."""
    opts = parse_opts(sys.argv)
    max_workers = opts.max_workers
    sem = threading.Semaphore(max_workers * 2)

    es = elasticsearch.Elasticsearch(
        [opts.cluster],
        port=opts.port,
        http_auth=(opts.username, opts.password),
        sniff_on_start=opts.sniff,
        sniff_on_connection_fail=opts.sniff,
    )
    print('Connected:', es.info())
    if opts.cleanup:
        print('Cleanup')
        es.indices.delete(index=INDEX, ignore=[400, 404])
    ret = es.indices.create(
        index=INDEX,
        body=INDEX_BODY,
        ignore=400
    )
    print(ret)

    # TODO: Also create directories.
    rows = read_commits(opts.input)
    executor = futures.ThreadPoolExecutor(max_workers=max_workers)
    for row in rows:
        # We use the semaphore to avoid reading *all* the file.
        sem.acquire()
        future = executor.submit(create, es, row)
        future.add_done_callback(lambda f: callback(f, sem))
    executor.shutdown()

    print(es.cluster.health(wait_for_status='yellow', request_timeout=1))
    # print(es.search(index=INDEX))


if __name__ == '__main__':
    main()
