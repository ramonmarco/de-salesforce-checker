from datetime import date

import cli
from checker.salesforce_checker import SalesforceBulkQuery


def run_check(object_name: str, start_date: date = None, end_date: date = None):
    print(f'Running check for table:{object_name}')
    if start_date or end_date:
        print(f'from {start_date} to {end_date}...')

    salesforce_bulk_query_origin = SalesforceBulkQuery("origin")
    result_origin = salesforce_bulk_query_origin.query(object_name, start_date, end_date)

    salesforce_bulk_query_destination = SalesforceBulkQuery("destination")
    result_destination = salesforce_bulk_query_destination.query(object_name, start_date, end_date)

    return result_origin.compare(result_destination)


if __name__ == '__main__':
    kwargs = cli.cli()
    result = run_check(**kwargs)
    print(result)
