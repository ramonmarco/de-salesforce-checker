def strip_string(string):
    if len(string.split(' ')) > 1:
        return f"'{string}'"
    else:
        return string


def generate_salesforce_query(salesforce_columns, salesforce_object, update_field, start_date=None, end_date=None):
    """Method to generate a salesforce query string."""

    query_statement = f"""SELECT {','.join(strip_string(column) for column in salesforce_columns)} 
                        FROM {salesforce_object}"""

    if start_date:
        query_statement = f"{query_statement} where {update_field} >= {start_date}"

    if start_date and end_date:
        query_statement = f"{query_statement} and {update_field} <= {end_date}"

    if not start_date and end_date:
        query_statement = f"{query_statement} where {update_field} <= {end_date}"

    # TODO remove LIMIT or parametrize it ask requirement
    return query_statement + " LIMIT 10"
