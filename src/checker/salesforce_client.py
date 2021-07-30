from collections import OrderedDict

import requests


class SalesforceDownloader(object):
    Version = "v43.0"
    Query_Path = f"/services/data/{Version}/"
    Bulk_Query = "query/"
    Bulk_Query_All = "queryAll/"
    Timeout = 60
    Retries = 3

    def __init__(self, client_id, client_secret,
                 username_salesforce, password_salesforce,
                 security_token_salesforce,
                 sf_url):

        params = {
            "grant_type": "password",
            "client_id": client_id,
            "client_secret": client_secret,
            "username": username_salesforce,
            "password": f"{password_salesforce}{security_token_salesforce}"
        }

        self.sf_url = sf_url
        self.session = requests.Session()
        self.login_credentials = self.generate_login_credentials(params=params)
        self.access_token = self.login_credentials["access_token"]
        self.instance_url = self.login_credentials["instance_url"]
        self.session.headers = {
            "Content-type": "application/json",
            "Accept-Encoding": "gzip",
            "Authorization": f"Bearer {self.access_token}"
        }

    def generate_login_credentials(self, params):
        token_response = requests.post(self.sf_url, params=params)
        if token_response.status_code == 200:
            access_token = token_response.json().get("access_token")
            instance_url = token_response.json().get("instance_url")
        else:
            raise token_response.raise_for_status()
        return {"access_token": access_token, "instance_url": instance_url}

    def generate_query_endpoint(self, get_deleted_records=False):
        end_point = f"{self.instance_url}{self.Query_Path}"
        if get_deleted_records:
            return f"{end_point}{self.Bulk_Query_All}"
        else:
            return f"{end_point}{self.Bulk_Query}"

    def execute_requests(self, url, metedata=None):
        retries = 0

        while retries < SalesforceDownloader.Retries:
            try:
                response = self.session.get(url=url, timeout=SalesforceDownloader.Timeout, params=metedata)
                if response.status_code < 300:
                    return response.json(object_pairs_hook=OrderedDict)
                else:
                    raise Exception(f"{response.status_code} {response.text} {response.url}")

                if retries == SalesforceDownloader.Retries:
                    raise Exception(f"""Salesforce API failed to connect after 
                                        {SalesforceDownloader.Retries} retries.""")
            except requests.exceptions.RequestException:
                retries += 1

    def get_object_metadata(self, salesforce_object):
        end_point = f"{self.instance_url}{self.Query_Path}sobjects/{salesforce_object}/describe"
        return self.execute_requests(url=end_point)

    def get_object_schema(self, salesforce_object):
        salesforce_metadata = self.get_object_metadata(salesforce_object=salesforce_object)
        return {metadata_record["name"]: metadata_record["type"]
                for metadata_record in salesforce_metadata['fields']}

    def query_salesforce_data(self, query, get_deleted_records=False, batch_size=None):
        query_url = self.generate_query_endpoint(get_deleted_records=get_deleted_records)
        query_results = self.execute_requests(url=query_url, metedata={'q': query})
        if query_results:
            batch_result = []
            iter_results = query_results['records']
            iter_size = len(query_results['records'])
            for index in range(iter_size):
                if batch_size:
                    if batch_size and index < batch_size:
                        batch_result.append(iter_results[index])
                    elif index == batch_size:
                        yield batch_result
                        batch_result = batch_result[-1:1]
                    elif batch_size and index + 1 == iter_size:
                        yield batch_result
                        batch_result = batch_result[-1:1]
                else:
                    yield iter_results[index]
        while not query_results['done']:
            query_results = self.execute_requests(
                f"{self.instance_url}{query_results['nextRecordsUrl']}")
            batch_result = []
            iter_results = query_results['records']
            iter_size = len(iter_results)
            for index in range(iter_size):
                if batch_size:
                    if index < batch_size:
                        batch_result.append(iter_results[index])
                    elif index == batch_size:
                        yield batch_result
                        batch_result = batch_result[-1:1]
                    elif index + 1 == iter_size:
                        yield batch_result
                        batch_result = batch_result[-1:1]
                else:
                    yield iter_results[index]
