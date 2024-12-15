import os
from pyiceberg.catalog import load_catalog
from datetime import date

# Get the directory of the current script
script_dir = os.path.dirname(__file__)

# Construct the relative path to the SQLite database
warehouse_path = os.path.join(script_dir, 'rizzoli_warehouse')
sqlite_db_path = os.path.join(warehouse_path, 'pyiceberg_catalog.db')

# Load the catalog
catalog = load_catalog(
    name='acme_corp',
    **{
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
)

# Print the catalog to verify connection
print(catalog)

assert catalog.table_exists('registry.employees')

# Query the registry.employee table
employee_table = catalog.load_table('registry.employees')

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from soluzione import rows_at_second_last_snapshot

assert rows_at_second_last_snapshot(employee_table) == 3, "Expected 3 rows in the second last snapshot of the employee table."

print("Validation successful.")