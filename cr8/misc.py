

def parse_table(fq_table):
    """ parses a tablename and returns a (<schema>, <table>) tuple

    schema defaults to doc if the table name doesn't contain a schema

    >>> parse_table('x.users')
    ('x', 'users')

    >>> parse_table('users')
    ('doc', 'users')
    """

    parts = fq_table.split('.')
    if len(parts) == 1:
        return 'doc', parts[0]
    elif len(parts) == 2:
        return parts[0], parts[1]
    else:
        raise ValueError
