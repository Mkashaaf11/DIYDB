import os
import json

from index import index

class Table:
    def __init__(self, name: str, db_path : str):
        """
        Initialize a new Table with a given name.
        
        Parameters:
        name (str): The name of the table.
        """
        self.name = name
        self.columns = []
        self.record_id_counter = 1
        self.records = {}
        self.primary_key_values = set()
        self.column_datatype = {}
        self.column_constraints = {}
        self.table_file =os.path.join (db_path,self.name + '.json')
        index(self.name)
        self.load_data()

    @staticmethod
    def convert_to_type(value: str, dtype: type):
        """
        Convert a string value to the specified data type.
        
        Parameters:
        value (str): The string value to convert.
        dtype (type): The target data type.
        
        Returns:
        The converted value if successful, else raises a ValueError.
        """
        try:
            if dtype == int:
                return int(value)
            elif dtype == float:
                return float(value)
            elif dtype == str:
                return value
            else:
                raise ValueError(f"Unsupported data type: {dtype}")
        except ValueError as e:
            raise ValueError(f"Conversion error: {e}")

    def convert_datatype(self, str_datatype: str):
        datatype_mapping = {
            'int': int,
            'str': str,
            'float': float,
        }
        return datatype_mapping.get(str_datatype, str)
    
    def load_data(self):
        if os.path.exists(self.table_file):
            file_path = self.table_file
            with open(file_path, 'r') as file:
                self.records = json.load(file)
                if self.records:
                    # Set record_id_counter to one more than the maximum record_id in the file
                    self.record_id_counter = max(map(int, self.records.keys())) + 1
                else:
                    # If no records exist, start the counter at 1
                    self.record_id_counter = 1
        else:
            self.records = {}
            self.record_id_counter = 1   
    
    def save_data(self):
        try:
            os.makedirs(os.path.dirname(self.table_file),exist_ok=True)
            with open(self.table_file, 'w') as file:
                json.dump(self.records,file,indent = 4)
        except IOError as e:
            return f"error saving data {e}"    
    
    def insert_record(self, content: list) -> str:
        """
        Insert a new record into the table.
        
        Parameters:
        content (list): A list of values to insert into the table.
        
        Returns:
        str: A message indicating success or failure of the operation.
        """
        if len(content) != len(self.columns):
            return {"success": False, "message": f"Values missing for some columns"}
        
        # Check for unique primary key
        primary_key_value = content[0]
        if primary_key_value in self.primary_key_values:
            return {"sucess": False, "message": f"Primary key {primary_key_value} should be unique"}

        for column, value in zip(self.columns, content):
            

            # Check for NOT NULL constraint
            if (value is None or value == '') and 'NOT NULL' in self.column_constraints.get(column, []):
                return {"success":False,"message":f"Column {column} doesn't allow NULL values"}

            # Check for UNIQUE constraint
            if 'UNIQUE' in self.column_constraints.get(column, []):
                if any(record[self.columns.index(column)] == value for record in self.records.values()):
                    return {"success": False , "message": f"Column {column} only allows unique values"}
            
            
            datatype = self.column_datatype.get(column)
            try:
                value = self.convert_to_type(value, datatype)
            except ValueError as e:
                return {'success': False, "message": str(e) }
            # Check for Data Type
            if not isinstance(value, datatype):
                return {"success": False , "message": f"Invalid Data type of column {column}. Expected {datatype.__name__}"}

        

        self.primary_key_values.add(primary_key_value)
        record_id = self.record_id_counter
        self.records[record_id] = content
        self.record_id_counter+=1
        self.save_data()
        index.insert_index(primary_key_value,record_id)
        return {"success": True , "message": f"Record inserted into the table", "record_id":record_id}

    def define_columns(self, columns: list, datatype: list, constraints: dict = None) -> str:
        """
        Define columns for the table.
        
        Parameters:
        columns (list): A list of column names.
        datatypes (list): A list of data types corresponding to the columns.
        constraints (dict, optional): A dictionary of constraints for each column.
        
        Returns:
        str: A message indicating success or failure of the operation.
        """
        if len(columns) != len(datatype):
            return {"success": False, "message": "Invalid Datatype | Datatype missing"}
           

        self.column_datatype = {col: self.convert_datatype(data) for col, data in zip(columns, datatype)}

        for column in columns:
            self.columns.append(column)
            if constraints:
                self.column_constraints[column] = constraints.get(column, [])
            else:
                self.column_constraints[column] = []
        return {"success": True, "message": "Columns inserted successfully"}

    def select(self) -> list:
        """
        Select all records from the table.
        
        Returns:
        list: A list of records in the table.
        """
        return self.records

    def update_record(self, primary_key: any, new_record: list) -> str:
        """
        Update an existing record in the table.

       Parameters:
       primary_key (any): The primary key of the record to update.
       new_record (list): A list of new values for the record.

        Returns:
        str: A message indicating success or failure of the operation.
        """
        # Find the index of the record with the given primary key
        
        
        record_id = index.find_index(primary_key)
        
        #indexing failed, now manual search
        if record_id == None:
            for index, record in self.records.items():
                if record[0] == primary_key:
                   record_id = index
                   break

        if record_id is None:
            return {'success': False, 'message':f"Record with primary key {primary_key} doesn't exist" }

    # Check if the length of the new record matches the number of columns
        if len(new_record) != len(self.columns):
            return {'success': False, 'message':f"Values missing for some columns" } 
        
        
         # Check if the new primary key value already exists (for primary key update)
        new_primary_key = new_record[0]
        if new_primary_key != primary_key and new_primary_key in self.primary_key_values:
            return {'success': False, 'message':f"Primary key {new_primary_key} should be unique" }

        # Validate the new record values and constraints
        for col, value in zip(self.columns, new_record):
            datatype = self.column_datatype.get(col)

        # Check for NOT NULL constraint
            if value == '' and 'NOT NULL' in self.column_constraints.get(col, []):
               return {'success': False, 'message':f"Column {col} doesn't allow NULL values" }

        # Check for UNIQUE constraint
            if 'UNIQUE' in self.column_constraints.get(col, []):
               for index,record in self.records.items():
                   if record[self.columns.index(col)] == value and record[0] != primary_key:
                       return {'success': False, 'message': f"Column {col} only allows unique values"}

            # Check for valid datatype
            try:
                value = self.convert_to_type(value, datatype)
            except ValueError as e:
                return str(e)

            if not isinstance(value, datatype):
                return {'success': False,'message': f"Invalid Data type for column {col}. Expected {datatype.__name__}" }

            

            # Update the primary key set if the primary key is changed
            if new_primary_key != primary_key:
                old_pri_key = primary_key
                new_pri_key = new_primary_key
                
                self.primary_key_values.remove(primary_key)
                self.primary_key_values.add(new_primary_key)
            else:
                old_pri_key = None
                new_pri_key = None    

            # Update the record
            original_record = self.records[record_id]
            self.records[record_id] = new_record
            self.save_data()
            return {
                    'success': True, 
                    'message': f"Record with primary key {primary_key} has been updated successfully", 
                    'record_id': record_id,
                    'original_record': original_record,
                    'old_pri_key': old_pri_key,
                    'new_pri_key':new_pri_key 
                    }


    def delete_record(self, primary_key: any) -> str:
        """
        Delete a record from the table.
        
        Parameters:
        primary_key (any): The primary key of the record to delete.
        
        Returns:
        str: A message indicating success or failure of the operation.
        """
        
        record_id = index.find_index(primary_key)
         
        if record_id == None:
            for index,record in self.records.items():
                if record[0] == primary_key:
                    record_id = index
                    break
            
        if record_id is not None:
            record = self.records[record_id]
            del self.records[record_id]
            self.primary_key_values.remove(primary_key)
            old_pri_key = primary_key
            self.save_data()
            return {'success': True,
                    'message':f"Record with primary key {primary_key} deleted successfully", 
                    'record_id': record_id,
                    'record': record,
                    'old_pri_key': primary_key
                    }
        return {'success': False, 'message': f"Record with primary key {primary_key} not found" }
