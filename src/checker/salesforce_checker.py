import json
import os
from datetime import date
from time import sleep

import pandas as pd
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO


class SalesforceBulkQuery:

    def __init__(self, instance: str):
        if instance == "origin":
            self.bulk = SalesforceBulk(username=os.getenv("SF_D2C_BETA_USERNAME"),
                                       password=os.getenv("SF_D2C_BETA_PASSWORD"),
                                       security_token=os.getenv("SF_D2C_BETA_SECURITY_TOKEN"),
                                       client_id=os.getenv("SF_D2C_BETA_CLIENT_ID"),
                                       domain="test"
                                       )
        else:
            self.bulk = SalesforceBulk(username=os.getenv("SF_AON_US_BETA_USERNAME"),
                                       password=os.getenv("SF_AON_US_BETA_PASSWORD"),
                                       security_token=os.getenv("SF_AON_US_BETA_SECURITY_TOKEN"),
                                       client_id=os.getenv("SF_AON_US_BETA_CLIENT_ID"),
                                       domain="test"
                                       )

    def query(self, object_name: str, start_date: date = None, end_date: date = None):
        job = self.bulk.create_query_job(object_name, contentType='JSON')
        batch = self.bulk.query(job, f"SELECT Id, Phone FROM {object_name} LIMIT 10")
        self.bulk.close_job(job)
        while not self.bulk.is_batch_done(batch):
            sleep(10)

        for result in self.bulk.get_all_results_for_query_batch(batch):
            result = json.load(IteratorBytesIO(result))
            for row in result:
                print(row)
        return pd.DataFrame(result)
