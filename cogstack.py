import getpass
import elasticsearch
import elasticsearch.helpers
import pandas as pd
from typing import Dict, List


class CogStack(object):
    def __init__(self, host, port=9200, username=None, password=None, scheme='https'):
        username, password = self._check_auth_details(username, password)

        self.elastic = elasticsearch.Elasticsearch(hosts=[{'host': host, 'port': port}],
                                         http_auth=(username, password),
                                         scheme=scheme,
                                         verify_certs=False)

    def _check_auth_details(self, username=None, password=None):
        if username is None:
            username = input("Username:")
        if password is None:
            password = getpass.getpass("Password:")
            
        return username, password


    def get_docs_generator(self, query: Dict, index: str, es_gen_size: int=800, request_timeout: int=840000):
        docs_generator = elasticsearch.helpers.scan(self.elastic,
            query=query,
            index=index,
            size=es_gen_size,
            request_timeout=request_timeout)

        return docs_generator


def cogstack2df(cogstack_search_gen, column_headers=None):
    """
    Returns DataFrame from CogStack search

    :param cogstack_search_gen: CogStack output generator object
    :param column_headers: specify column headers
    :return: DataFrame
    """
    results = [column_headers]
    for i, doc in enumerate(cogstack_search_gen):
        if column_headers is None:
            column_headers = ['id'] + list(doc['_source'].keys())
            results = [column_headers]
        result = []
        for col in column_headers:
            if col == 'id':
                result.append(doc['_id'])
            else:
                result.append(doc['_source'].get(col, ''))
        results.append(result)
    df_results = pd.DataFrame(results[1:], columns=results[0])
    return df_results


def cogstack2csv(cogstack_search_gen, filename, column_headers=None):
    # TODO: Complete function for chunking large searches.
    """
    Returns CSV from CogStack search

    :param cogstack_search_gen: CogStack output generator object
    :param filename: .CSV filename
    :param column_headers: specify column headers
    :return: CSV
    """
    counter = 100000
    results = [column_headers]
    for i, doc in enumerate(cogstack_search_gen):
        if column_headers is None:
            column_headers = ['id'] + list(doc['_source'].keys())
            results = [column_headers]
        result = []
        for col in column_headers:
            if col == 'id':
                result.append(doc['_id'])
            else:
                result.append(doc['_source'].get(col, ''))
        results.append(result)

        if len(results) > counter:
            counter += 100000
            print(f"{counter} to csv")
            df_results = pd.DataFrame(results[1:], columns=results[0])
            df_results.to_csv(filename, header=column_headers, index=False, mode='a', chunksize=10000)
            results = []

    if df_results is None:
        pd.DataFrame({}).to_csv(filename, header=column_headers, index=False)  # TODO check if necessary
    df_results = pd.DataFrame(results[1:], columns=results[0])
    df_results.to_csv(filename, header=column_headers, index=False, mode='a', chunksize=10000)
    return

