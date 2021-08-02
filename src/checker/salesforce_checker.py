import os
from datetime import date

import pandas as pd
from simple_salesforce import Salesforce, format_soql


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
        desc = self.sf.Account.describe()
        field_names = [field['name'] for field in desc['fields']]
        soql = "SELECT {} FROM ".format(','.join(field_names)) + object_name + " LIMIT 10"
        results = self.sf.query(format_soql(soql))
        return pd.DataFrame(results)
