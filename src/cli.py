import argparse


def cli():
    """
    Command Line Interface of the job. Data migration Salesforce checker.
    """

    parser = argparse.ArgumentParser(
        description="Command Line Interface of the job. Data migration Salesforce checker.",
    )

    parser.add_argument(
        "-o",
        "--object_name",
        help="Object to be checked",
        required=True
    )

    parser.add_argument(
        "-sd",
        "--start_date",
        help="Start date of range to be checked",
        required=False
    )

    parser.add_argument(
        "-nd",
        "--end_date",
        help="End date of range to be checked",
        required=False
    )

    args = parser.parse_args()
    return vars(args)
