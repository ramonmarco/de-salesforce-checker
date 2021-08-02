import os
from datetime import date

import pandas as pd
from simple_salesforce import Salesforce, format_soql

from src.checker.salesforce_query import generate_salesforce_query


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
        salesforce_object_columns = self.sf.Account.describe()
        query = generate_salesforce_query(salesforce_columns=salesforce_object_columns, salesforce_object=object_name,
                                          update_field="test", start_date=start_date, end_date=end_date)
        results = self.sf.query(format_soql(query))
        return pd.DataFrame(results)
