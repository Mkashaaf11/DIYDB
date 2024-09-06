#DBMS/models/index.py

import os
import json


class index:
    def __init__(table_name):
        self.index_dict = {}
        self.table_name = table_name
        self.index_path = os.path.join('index','indexes.json')
        self.load_index()
        
    
    def insert_index(primary_key, record_id):
        self.index_dict[self.table_name][primary_key] = record_id
        self.save_index()
    
    def find_index(primary_key):
        record_id = self.index_dict.get(self.table_name,{}).get(primary_key)
        return record_id
    
    def save_index():
        try:
            os.makedirs('index',exist_ok=True)
            with open('index.json','w') as file:
                json.dump(self.index_dict,file,indent=4)

        except IOError as e:
            return {'success': False, 'message': e}      
    
    def load_index():
        if os.path.exists(self.index_path):
            if os.path.getsize(self.index_path) > 0:
                with open(self.index_path,'r') as file:
                    self.index_dict=json.load(file)
                                        
            