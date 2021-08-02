from datetime import date

import cli
from src.checker.salesforce_checker import SalesforceBulkQuery


def run_check(object_name: str, start_date: date = None, end_date: date = None):
    print(f'Running check for table:{object_name} from {start_date} to {end_date}...')

    salesforce_bulk_query_origin = SalesforceBulkQuery("origin")
    result_origin = salesforce_bulk_query_origin.query(object_name, start_date, end_date)

    salesforce_bulk_query_destination = SalesforceBulkQuery("destination")
    result_destination = salesforce_bulk_query_destination.query(object_name, start_date, end_date)


if __name__ == '__main__':
    kwargs = cli.cli()
    run_check(**kwargs)
