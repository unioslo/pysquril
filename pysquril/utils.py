
AUDIT_SEPARATOR = "/"
AUDIT_SUFFIX = "audit"

def audit_table(table_name: str) -> str:
    """
    Construct audit table name from table_name.

    """
    return table_name + AUDIT_SEPARATOR + AUDIT_SUFFIX

def audit_table_src(audit_table_name: str) -> str:
    """
    Construct name of source table for a given audit table.

    """
    return audit_table_name[:-(len(AUDIT_SEPARATOR + AUDIT_SUFFIX))]
