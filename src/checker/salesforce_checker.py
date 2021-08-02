import os
from datetime import date

import pandas as pd
from simple_salesforce import Salesforce


class SalesforceBulkQuery:

    def __init__(self, instance: str):
        if instance == "origin":
            self.sf = Salesforce(username=os.getenv("SF_D2C_BETA_USERNAME"),
                                 password=os.getenv("SF_D2C_BETA_PASSWORD"),
                                 security_token=os.getenv("SF_D2C_BETA_SECURITY_TOKEN"),
                                 client_id=os.getenv("SF_D2C_BETA_CLIENT_ID"),
                                 domain="test"
                                 )
        else:
            self.sf = Salesforce(username=os.getenv("SF_AON_US_BETA_USERNAME"),
                                 password=os.getenv("SF_AON_US_BETA_PASSWORD"),
                                 security_token=os.getenv("SF_AON_US_BETA_SECURITY_TOKEN"),
                                 client_id=os.getenv("SF_AON_US_BETA_CLIENT_ID"),
                                 domain="test"
                                 )

    def query(self, object_name: str, start_date: date = None, end_date: date = None):
        results = self.sf.bulk.Account.query(f"SELECT Id, Phone FROM {object_name} LIMIT 10")
        for result in results:
            print(result)
        return pd.DataFrame(results)
