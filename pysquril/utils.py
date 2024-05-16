
AUDIT_SEPARATOR = "_"
AUDIT_SUFFIX = "audit"

def audit_table(table_name: str) -> str:
    """
    Construct audit table name from table_name.

    """
    return table_name + AUDIT_SEPARATOR + AUDIT_SUFFIX
