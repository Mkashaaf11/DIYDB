# /DBMS/models/database.py

import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
models_dir = os.path.abspath(os.path.join(current_dir))
sys.path.append(models_dir)

# Now import Table from table.py within models
from table import Table

class Database:
    def __init__(self):
        """
        Initialize a new Database.
        """
        self.tables = {}
    
    def create_table(self,name:str)->str:
        """
        Create a new table in the database.

        Parameters:
        name (str): The name of the table.

        Returns:
        str: A message indicating success or failure of the operation.
        """
        if name not in self.tables:
            self.tables[name] = Table(name)
            return f"Table {name} created"
        else:
            return f"Table {name} already exist"
        
    def define_columns(self,table_name:str,columns:list,datatypes:list,constraints:dict = None)->str:
        """
        Define columns for a table in the database.

        Parameters:
        table_name (str): The name of the table.
        columns (list): A list of column names.
        datatypes (list): A list of data types for the columns.
        constraints (dict, optional): A dictionary of constraints for the columns.

        Returns:
        str: A message indicating success or failure of the operation.
        """

        if table_name in self.tables:
            self.tables[table_name].define_columns(columns,datatypes,constraints)
            return f"columns inserted succesfully in table {table_name}"
        else:
            return f"table {table_name} dont exist"
            
            
    
    def insert(self,name:str,content:list)->str:
        """
        Insert a record into a table.

        Parameters:
        name (str): The name of the table.
        content (list): A list of values to insert into the table.

        Returns:
        str: A message indicating success or failure of the operation.
        """
        if name in self.tables:
            return self.tables[name].insert_record(content)
        else:
            return f"Error inserting record. Table {name} doesn't exist"    
                
    def select_table(self,name:str)->list:
        
        """
        Retrieve all records from a table.

        Parameters:
        name (str): The name of the table.

        Returns:
        list: A list of records in the table or an error message if the table doesn't exist.
        """
        if name in self.tables:
            records = self.tables[name].select()
            return records
        else:
            return f"Table {name} doesnt exist"   
        
    def update(self,name:str,primary_key,new_record:list)->str:
        """
        Update a record in a table.

        Parameters:
        name (str): The name of the table.
        primary_key: The primary key of the record to update.
        new_record (list): A list of new values for the record.

        Returns:
        str: A message indicating success or failure of the operation.
        """
        if name in self.tables:
            return self.tables[name].update_record(primary_key,new_record)
        else:
            return f"Error Updating record. Table {name} doesn't exist"
        
    def delete(self,name:str,primary_key)->str:
        """
        Delete a record from a table.

        Parameters:
        name (str): The name of the table.
        primary_key: The primary key of the record to delete.

        Returns:
        str: A message indicating success or failure of the operation.
        """
        if name in self.tables:
            return self.tables[name].delete_record(primary_key)    
        else:
            return f"Error deleting record. Table {name} doesn't exist "    
        
if __name__ == "__main__":
    db = Database()

    # Create a table
    print(db.create_table('users'))

    # Define columns for the table
    print(db.define_columns('users', ['id', 'name', 'email']))

    # Insert records
    print(db.insert('users', [1, 'Alice', 'alice@example.com']))
    print(db.insert('users', [2, 'Bob', 'bob@example.com']))

    # Try inserting a record with an incorrect number of columns
    print(db.insert('users', [3, 'Charlie']))
    
    print(db.select_table('users'))
        
        