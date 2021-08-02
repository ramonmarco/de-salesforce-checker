import os
from datetime import date

from src.checker.salesforce_client import SalesforceDownloader
from src.checker.salesforce_query import generate_salesforce_query


class SalesforceBulkQuery:

    def __init__(self, instance: str):
        if instance == "origin":
            self.salesforce_client = SalesforceDownloader(
                client_id=os.getenv("SF_D2C_BETA_CLIENT_ID"),
                client_secret=os.getenv("SF_D2C_BETA_CLIENT_SECRET"),
                username_salesforce=os.getenv("SF_D2C_BETA_USERNAME"),
                password_salesforce=os.getenv("SF_D2C_BETA_PASSWORD"),
                security_token_salesforce=os.getenv("SF_D2C_BETA_SECURITY_TOKEN"),
                sf_url=os.getenv("SF_BETA_URL")
            )
        else:
            self.salesforce_client = SalesforceDownloader(
                client_id=os.getenv("SF_AON_US_BETA_CLIENT_ID"),
                client_secret=os.getenv("SF_AON_US_BETA_CLIENT_SECRET"),
                username_salesforce=os.getenv("SF_AON_US_BETA_USERNAME"),
                password_salesforce=os.getenv("SF_AON_US_BETA_PASSWORD"),
                security_token_salesforce=os.getenv("SF_AON_US_BETA_SECURITY_TOKEN"),
                sf_url=os.getenv("SF_BETA_URL")
            )

    def query(self, object_name: str, start_date: date = None, end_date: date = None):
        salesforce_object_columns = self.salesforce_client.get_object_schema(salesforce_object=object_name)
        query = generate_salesforce_query(salesforce_columns=salesforce_object_columns, salesforce_object=object_name,
                                          update_field="test", start_date=start_date, end_date=end_date)
        results = self.salesforce_client.query_salesforce_data(query=query, get_deleted_records=True)
        for result in results:
            print(result)
        return result
