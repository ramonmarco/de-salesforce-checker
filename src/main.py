from datetime import date

import cli
from src.checker.salesforce_checker import SalesforceChecker


def run_check(object_name: str, start_date: date = None, end_date: date = None):
    print(f'Running check for table:{object_name} from {start_date} to {end_date}...')

    salesforce_checker = SalesforceChecker()
    salesforce_checker.check(object_name, start_date, end_date)


if __name__ == '__main__':
    kwargs = cli.cli()
    run_check(**kwargs)
