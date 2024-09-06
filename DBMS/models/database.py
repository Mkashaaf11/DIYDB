# /DBMS/models/database.py

import sys
import os
import json
current_dir = os.path.dirname(os.path.realpath(__file__))
models_dir = os.path.abspath(os.path.join(current_dir))
sys.path.append(models_dir)

# import Table from table.py within models
from table import Table

class Database:
    def __init__(self,db_name: str,owner:str):
        """
        Initialize a new Database.
        """
        self.tables = {}
        self.db_name = db_name
        self.owner = owner
        self.transaction_log = []
        self.db_path = os.path.join('databases',self.db_name)
        self.meta_data_file = os.path.join(self.db_path,'metadata.json')
        self.load_metadata()
    
    
    def save_metadata(self):
        metadata = {'owner': self.owner,'tables': {}}
        for table_name,table in self.tables.items():
            metadata['tables'][table_name] = {
                'name' : table_name,
                'columns': table.columns,
                'primary_key_value':list(table.primary_key_values),
                'datatype':[dtype.__name__ for dtype in table.column_datatype.values()],
                'constraint': table.column_constraints
                
            }
        try: 
            os.makedirs(self.db_path,exist_ok=True)
            with open(self.meta_data_file,'w') as file:
                json.dump(metadata,file,indent=4) 
        except IOError as e:
            return f'error occured while saving metadata {e}'           
             
    def load_metadata(self):
        if os.path.exists(self.meta_data_file):
            if os.path.getsize(self.meta_data_file) > 0:
                with open(self.meta_data_file,'r') as file:
                    metadata=json.load(file)
                    self.owner = metadata.get('owner')
                    for table_name,schema in metadata['tables'].items():
                        table = Table(table_name,self.db_path)
                        table.columns = schema['columns']
                        table.column_datatype = {col: table.convert_datatype(dtype) for col,dtype in zip(schema['columns'],schema['datatype'])}
                        table.primary_key_values = set(schema['primary_key_value'])
                        table.column_constraints = schema['constraint']
                        self.tables[table_name] = table
                    
                         
    
    def start_transaction(self):
        
        self.transaction_log = []
        
    def log_operation(self,operation,table_name,record_id=None,record = None, old_pri_key = None, new_pri_key = None):
        
        print(f"Logging operation: {operation} for table: {table_name}, record_id: {record_id}")
        self.transaction_log.append({
            
            "operation":operation,
            "table_name":table_name,
            "record_id":record_id,
            "record":record,
            "old_pri_key": old_pri_key,
            "new_pri_key":new_pri_key,
            
        })
        
    def commit_transaction(self):
        
        self.transaction_log = []    
    
    def rollback_transaction(self):
        #print("inside rollback")
        #print(self.transaction_log)
        for log in reversed(self.transaction_log):
            if log['operation']=='insert':
                #print(self.tables[log['table_name']].records)
                del self.tables[log['table_name']].records[log['record_id']]
                #print(self.tables[log['table_name']].records)
                self.tables[log['table_name']].save_data()
                
            elif log['operation'] == 'update':
                self.tables[log['table_name']].records[log['record_id']] = log['record']
                if log['old_pri_key'] is not None and log['new_pri_key'] is not None and log['old_pri_key'] != log['new_pri_key']:
                    self.tables[log['table_name']].primary_key_values.remove(log['new_pri_key'])
                    self.tables[log['table_name']].primary_key_values.add(log['old_pri_key'])
                    self.save_metadata()
                self.tables[log['table_name']].save_data()  
                 
            elif log['operation']=='delete':
                self.tables[log['table_name']].records[log['record_id']] = log['record']
                self.tables[log['table_name']].primary_key_values.add(log['old_pri_key'])
                self.save_metadata()
                self.tables[log['table_name']].save_data()
                
        self.transaction_log = []             
    
    def create_table(self,name:str,columns:list,datatypes:list,constraints:dict = None)->str:
        """
        Create a new table in the database.

        Parameters:
        name (str): The name of the table.
        columns (list): A list of column names.
        datatypes (list): A list of data types for the columns.
        constraints (dict, optional): A dictionary of constraints for the columns.

        Returns:
        str: A message indicating success or failure of the operation.
        """
        if name in self.tables:
            return {"success": False, "message": f"Table {name} already exists"}
    
        self.tables[name] = Table(name, self.db_path)
        result = self.tables[name].define_columns(columns, datatypes, constraints)
    
        if result["success"]:
            self.save_metadata()
            return {"success": True, "message": f"Table {name} created successfully"}
        else:
            return {"success": False, "message": result["message"]}
        
        
                           
    
    def insert(self,name:str,content:list)->str:
        """
        Insert a record into a table.

        Parameters:
        name (str): The name of the table.
        content (list): A list of values to insert into the table.

        Returns:
        str: A message indicating success or failure of the operation.
        """
        self.start_transaction()
        try:
            if name in self.tables:
                
                message = self.tables[name].insert_record(content)
                if message['success']:
                    record_id = message['record_id']
                    self.log_operation('insert', name, record_id=record_id, record=content)
                    
                    #print(self.transaction_log)
                    #raise Exception("Intentional Error: This is a test for rollback functionality")
                    self.save_metadata()
                    self.commit_transaction()
                    
                    return message
                else:
                    return {'success': False, 'message':f'Table {name} doesnt exist'}
        except Exception as e:
            self.rollback_transaction()
            return {"success": False, "message": f"{e}"}    
                
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
        self.start_transaction()
        try:
            
            if name in self.tables:
                message = self.tables[name].update_record(primary_key,new_record)
                if message['success']:
                    record_id = message['record_id']
                    original_record = message['original_record']
                    old_pri_key = message['old_pri_key']
                    new_pri_key = message['new_pri_key']
                    self.log_operation('update', name, record_id=record_id, record=original_record, old_pri_key = old_pri_key,new_pri_key= new_pri_key)
                    
                    #print(self.transaction_log)
                    #raise Exception("Intentional Error: This is a test for rollback functionality")
                    
                    self.save_metadata()
                    self.commit_transaction()
                    
                    return message
        except Exception as e:
            self.rollback_transaction()
            return {'success': False, 'message': f"Error Updating record {e}" }
        
    def delete(self,name:str,primary_key)->str:
        """
        Delete a record from a table.

        Parameters:
        name (str): The name of the table.
        primary_key: The primary key of the record to delete.

        Returns:
        str: A message indicating success or failure of the operation.
        """
        self.start_transaction()
        try:
            
            if name in self.tables:
                message = self.tables[name].delete_record(primary_key)
                if message['success']:
                    record_id = message['record_id']
                    record = message['record']
                    old_pri_key = message['old_pri_key']
                    self.log_operation('delete',name,record_id,record,old_pri_key)
                   
                    #print(self.transaction_log)
                    #raise Exception("Intentional Error: This is a test for rollback functionality")
                    self.save_metadata()
                    self.commit_transaction()
                   
                    return message    
        except Exception as e:
            self.rollback_transaction()
            return {'success': False, 'message':f"Error deleting record {e}. Table {name} doesn't exist "  }   
      
    def drop_table(self, table_name: str) -> str:
        """
        Drop an existing table from the database.
        
        Parameters:
        table_name (str): The name of the table to drop.
        
        Returns:
        str: A message indicating success or failure of the operation.
        """
        if table_name not in self.tables:
            return f"Table {table_name} does not exist"

        del self.tables[table_name]
        os.remove(os.path.join(self.db_path, table_name + '.json'))
        self.save_metadata()
        return f"Table {table_name} dropped successfully"  
        
if __name__ == "__main__":
    db = Database()

        