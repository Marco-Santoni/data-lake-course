import os
import sqlite3

# Define the folder name
folder_name = 'rizzoli_warehouse'

# Get the current path
current_path = os.path.dirname(os.path.abspath(__file__))

# Construct the full path to the folder
folder_path = os.path.join(current_path, folder_name)

# Check if the folder exists
assert os.path.exists(folder_path), f"Folder '{folder_name}' does not exist in the current path."

# Define the file name
file_name = 'pyiceberg_catalog.db'

# Construct the full path to the file
file_path = os.path.join(folder_path, file_name)

# Check if the file exists
assert os.path.exists(file_path), f"File '{file_name}' does not exist in the folder '{folder_name}'."

# Connect to the SQLite database
conn = sqlite3.connect(file_path)
cursor = conn.cursor()

# Query the iceberg_namespace_properties table
cursor.execute("SELECT * FROM iceberg_namespace_properties WHERE namespace = 'registry'")
record = cursor.fetchone()

# Check if the record exists
assert record is not None, "No record found with namespace 'registry' in the iceberg_namespace_properties table."

# Close the connection
conn.close()

print("Validation successful.")