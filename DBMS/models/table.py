# /DBMS/models/table.py

class Table:
    def __init__(self,name:str):
        
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
                
        
    def insert_record(self,content:list) -> str:
        
        """
        Insert a new record into the table.

        Parameters:
        content (list): A list of values to insert into the table.

        Returns:
        str: A message indicating success or failure of the operation.
        """
        
        if len(content)!=len(self.columns):
            return f"Values missing of some column"
        
        for column,value in zip(self.columns,content):
            #check for NOT NULL constraint
            if value is None and 'NOT NULL' in self.column_constraints.get(col,[]):
                return f"column {column} doesn't allow NULL values"
            
            #check for UNIQUE constraint  
            flag = 0
            index = self.columns.index(column)
            for record in self.records:
                if record[index] == value:
                    flag = 1
                    break
             
              
            if 'UNIQUE' in self.column_constraints.get(column,[]) and flag == 1:
                return f"column {column} only allows unique values"
            
             #check for Datatype
            datatype = self.column_datatype.get(column)  
            if not isinstance(value,datatype):
                return f"Invalid Data type of column {column}.  Expected {datatype.__name__}"
        
        #check for unique primary key || This requires dynamic logic
        primary_key_value =  content[0] 
        if primary_key_value in self.primary_key_values:
            return f"primary key {primary_key_value} should be unique"
        
        self.primary_key_values.add ( primary_key_value ) 
        self.records.append(content)
        return f"record inserted in to the table"    
    
    def define_columns(self,columns:list,datatype:list,constraint:dict = None) -> str:
        
        """
        Define columns for the table.

        Parameters:
        columns (list): A list of column names.
        datatypes (list): A list of data types corresponding to the columns.
        constraints (dict, optional): A dictionary of constraints for each column.

        Returns:
        str: A message indicating success or failure of the operation.
        """
        
        if len(columns)!=len(datatype):
            return f"Invalid Datatype | Datatype missing"
        
        self.column_datatype = {col: data     for col,data in zip(columns,datatype)} 
        
        
        for column in columns:
            self.columns.append(column)
            if constraints:
               self.column_constraints[column] = constraints.get(column, [])
            else:
                self.column_constraints[column] = []
        return f"column inserted succesfully"  
    
    def select(self)->list:
        """
        Select all records from the table.

        Returns:
        list: A list of records in the table.
        """
        
        return self.records
          
    def update_record(self,primary_key:any,new_record:list)->str:
        """
        Update an existing record in the table.

        Parameters:
        primary_key (any): The primary key of the record to update.
        new_record (list): A list of new values for the record.

        Returns:
        str: A message indicating success or failure of the operation.
        """
        for record in self.records:
            if record[0] == primary_key:
                if len(record)!= len(new_record):
                    return f"Value missing of some column"
                
                for col,value in zip(self.columns,new_record):
                    datatype = self.column_datatype.get(col)
                    
                    #check for NOT NULL constraint
                    if value is None and 'NOT NULL' in self.column_constraints.get(col,[]):
                         return f"column {column} doesn't allow NULL values"
                    
                    #check for UNIQUE constraint
                    flag = 0
                    index = self.columns.index(column)
                    for record in self.records:
                        if record[index] == value:
                           flag = 1
                           break
                
                    if 'UNIQUE' in self.column_constraints.get(column,[]) and flag == 1:
                           return f"column {column} only allows unique values"
                    
                    #check for valid Datatype
                    if not isinstance(value,datatype):
                        return f"Invalid Data type of column {column}.  Expected {datatype.__name__}"
                    
                index = self.records.index(record)   
                self.records[index] = new_record
                return f"record with primary key {primary_key} has been updated succesfully"
        return f"record with primary key {primary_key} doesn't exist"     

    def delete_record(self,primary_key:any)->str:
        """
        Delete a record from the table.

        Parameters:
        primary_key (any): The primary key of the record to delete.

        Returns:
        str: A message indicating success or failure of the operation.
        """
        
        for record in self.records:
            if record[0]==primary_key:
                self.records.remove(record)
                self.primary_key_values.remove(primary_key)
                return f"Record with primary key {primary_key} deleted successfully"
        return f"Record with primary key {primary_key} not found"    
                                                