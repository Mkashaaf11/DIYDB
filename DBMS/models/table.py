class Table:
    def __init__(self, name: str):
        """
        Initialize a new Table with a given name.
        
        Parameters:
        name (str): The name of the table.
        """
        self.name = name
        self.columns = []
        self.records = []
        self.primary_key_values = set()
        self.column_datatype = {}
        self.column_constraints = {}

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

    def insert_record(self, content: list) -> str:
        """
        Insert a new record into the table.
        
        Parameters:
        content (list): A list of values to insert into the table.
        
        Returns:
        str: A message indicating success or failure of the operation.
        """
        if len(content) != len(self.columns):
            return f"Values missing for some columns"

        for column, value in zip(self.columns, content):
            datatype = self.column_datatype.get(column)
            try:
                value = self.convert_to_type(value, datatype)
            except ValueError as e:
                return str(e)

            # Check for NOT NULL constraint
            if (value is None or value == '') and 'NOT NULL' in self.column_constraints.get(column, []):
                return f"Column {column} doesn't allow NULL values"

            # Check for UNIQUE constraint
            if 'UNIQUE' in self.column_constraints.get(column, []):
                if any(record[self.columns.index(column)] == value for record in self.records):
                    return f"Column {column} only allows unique values"

            # Check for Data Type
            if not isinstance(value, datatype):
                return f"Invalid Data type of column {column}. Expected {datatype.__name__}"

        # Check for unique primary key
        primary_key_value = content[0]
        if primary_key_value in self.primary_key_values:
            return f"Primary key {primary_key_value} should be unique"

        self.primary_key_values.add(primary_key_value)
        self.records.append(content)
        return f"Record inserted into the table"

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
            return f"Invalid Datatype | Datatype missing"

        self.column_datatype = {col: self.convert_datatype(data) for col, data in zip(columns, datatype)}

        for column in columns:
            self.columns.append(column)
            if constraints:
                self.column_constraints[column] = constraints.get(column, [])
            else:
                self.column_constraints[column] = []
        return f"Columns inserted successfully"

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
        record_index = None
        for index, record in enumerate(self.records):
            if record[0] == primary_key:
                record_index = index
                break

        if record_index is None:
            return f"Record with primary key {primary_key} doesn't exist"

    # Check if the length of the new record matches the number of columns
        if len(new_record) != len(self.columns):
            return f"Values missing for some columns"

        # Validate the new record values and constraints
        for col, value in zip(self.columns, new_record):
            datatype = self.column_datatype.get(col)

        # Check for NOT NULL constraint
            if value == '' and 'NOT NULL' in self.column_constraints.get(col, []):
               return f"Column {col} doesn't allow NULL values"

        # Check for UNIQUE constraint
            if 'UNIQUE' in self.column_constraints.get(col, []):
               for record in self.records:
                   if record[self.columns.index(col)] == value and record[0] != primary_key:
                       return f"Column {col} only allows unique values"

            # Check for valid datatype
            try:
                value = self.convert_to_type(value, datatype)
            except ValueError as e:
                return str(e)

            if not isinstance(value, datatype):
                return f"Invalid Data type for column {col}. Expected {datatype.__name__}"

             # Check if the new primary key value already exists (for primary key update)
            new_primary_key = new_record[0]
            if new_primary_key != primary_key and new_primary_key in self.primary_key_values:
                return f"Primary key {new_primary_key} should be unique"

            # Update the primary key set if the primary key is changed
            if new_primary_key != primary_key:
                self.primary_key_values.remove(primary_key)
                self.primary_key_values.add(new_primary_key)

            # Update the record
            self.records[record_index] = new_record
            return f"Record with primary key {primary_key} has been updated successfully"


    def delete_record(self, primary_key: any) -> str:
        """
        Delete a record from the table.
        
        Parameters:
        primary_key (any): The primary key of the record to delete.
        
        Returns:
        str: A message indicating success or failure of the operation.
        """
        for record in self.records:
            if record[0] == primary_key:
                self.records.remove(record)
                self.primary_key_values.remove(primary_key)
                return f"Record with primary key {primary_key} deleted successfully"
        return f"Record with primary key {primary_key} not found"
