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

assert table.shape[0] == 3, "Expected 3 records in the employee table."

# Assert that the record at the first row, column 'id' has value 1
assert table['id'][0].as_py() == 1, "Expected the first row 'id' to be 1."
assert table['name'][1].as_py() == 'Bob', "Expected the second row 'name' to be 'Bob'."
assert table['hire_date'][2].as_py() == date(2020, 1, 3), "Expected the third row 'hire_date' to be 2020-01-03."

print("Validation successful.")