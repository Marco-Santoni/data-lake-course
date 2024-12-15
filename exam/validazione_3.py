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

# Print the records in the employee table
table = employee_table.scan().to_arrow()

assert table.shape[0] == 5, "Expected 5 records in the employee table."

expected_records = [
    {"id": 1, "name": "Alice", "hire_date": date(2020, 1, 1)},
    {"id": 2, "name": "Bob", "hire_date": date(2020, 1, 2)},
    {"id": 3, "name": "Charlie", "hire_date": date(2020, 1, 3)},
    {"id": 4, "name": "David", "hire_date": date(2020, 1, 4)},
    {"id": 5, "name": "Eve", "hire_date": date(2020, 1, 5)},
]

pandas_table = employee_table.scan().to_pandas()
for record in expected_records:
    assert record in pandas_table.to_dict(orient='records'), f"Expected record {record} not found in the table."

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from soluzione import count_table_snapshots

assert count_table_snapshots(employee_table) == 2, "Expected 2 snapshots in the employee table."

print("Validation successful.")