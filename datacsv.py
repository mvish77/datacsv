import os
import csv
from typing import Collection

class CSVDatabase:
    """
    Creating a lightweight, zero-dependency, file-based database system in 
    Python using CSV as the storage backend is a great idea for small-scale applications or CLI tools.
    It helpful to create application with lightweight file based database.
    It helps to store: 
        logs and retrieve 
        basic details 
        microservices logs
        basic user data inputs
    """

    def __init__(self, db_name: str, fields:Collection[str]):
        """
        We need 2 inputs as an argument to create object of pcsvdb.
            1. Database name - You have to provide database name in string format
            2. Fields - Headers for table that needed to perform query and update operation
        """
        self.db_name = db_name if db_name.endswith(".csv") else db_name + ".csv"
        self.headers = fields

        if not os.path.exists(self.db_name):
            if fields:
                with open(self.db_name, mode='w', newline='', encoding='UTF-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fields)
                    writer.writeheader()
            else:
                raise ValueError("""PCSVDB: Database does not exist, provide fields to create it. 
                                    You must provide Database name in String and fields in List 
                                    For example: db = PCSVDB('users.csv',['name,'age','course'])""")


    def insert(self, row:dict,fill_missing:bool=False)-> bool:
        """
        Perform single insert operation to database
        """
        #check for intance
        if not isinstance(row, dict):
            raise ValueError("""
            PCSVDB: Your provided row is not in dictionary format. 
            Your input row: {row}
            Check your input row is valid dict or not.
            """)
        #check for existance
        if not row:
            raise ValueError("""
            PCSVDB: Your provided row empty. 
            Your input row: {row}
            Check your input row has valid data or not.
            """)
        
        if fill_missing:
            row = {key: row.get(key, "") for key in self.headers}
        else:
            missing_keys = [key for key in self.headers if key not in row]
            if missing_keys:
                raise ValueError(f"Missing keys in row: {missing_keys}")
        # validate extra keys and raise error if user entered extra keys
        extra_keys = [key for key in row if key not in self.headers]
        
        if extra_keys:
            raise ValueError("""
            PCSVDB: Unexpected keys in row: {extra_keys}.
            Your keys must be same as your fields.
            """)

        with open(self.db_name, mode='a', newline='',encoding='UTF-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writerow(row)
            return True


    def find_all(self)->list:
        """
        find_all is used to find all rows in database. It return in list format.
        """
        
        with open(self.db_name, mode='r', newline='', encoding='UTF-8') as f:
            reader = csv.DictReader(f)
            return list(reader)



    def find(self, key:str, value):
        """
        used to find row with specific key value
        """
        if not key or not value:
            raise ValueError("""
            You must provide valid key and value to run find method.
            Your input data: KEY: {key} AND VALUE: {value}
            Key or value is missing in your inputs
            """)
        
        with open(self.db_name, mode='r', newline='', encoding='UTF-8') as f:
            reader = csv.DictReader(f)
            return [row for row in reader if row.get(key) == value]


    def find_where(self, condition)->list:
        """
        find_where() condition accept collable function or dictionary to filter the row data from database and return list as output.
        You can pass argument like:
        
        1. dictionary argument: find_where({"name": "mahesh"})
        
        2. function: def name_starts_with_a(row):
                        return row["name"].startswith("m")
                    find_where(name_starts_with_a)
        
        3. lambda:  find_where(lambda row: int(row["age"]) > 25)

        find_where() has data called 'row' which you can use to perform a conditional operation.
        """
        
        if condition is None:
            raise ValueError("The condition should be valid function or dictionary. None is provided")

        with open(self.db_name, mode='r', newline='', encoding='UTF-8') as f:
            reader = csv.DictReader(f)

            if callable(condition):
                return [row for row in reader if condition(row)]

            elif isinstance(condition, dict):
                if not condition:
                    raise ValueError(
                    """Provided dictionary is an empty
                    Your input: {condition}
                    Check wether something missing in your input. 
                    """)

                for key in condition:
                    if key not in self.headers:
                        raise ValueError(
                            """Invalid key in condition: '{key}'
                            Check your provided key is valid field name
                            """)

                return [
                    row for row in reader
                    if all(row.get(k) == v for k, v in condition.items())
                ]

            else:
                raise TypeError("Condition must be a dictionary or a callable function. Provided condition not matching one of them")

    
    def delete_where(self, condition)->bool:
        """
        same as find_where, delete_where() accept collable function or dictionary to filter the row data from database and return boolean as output.
        You can pass argument like:
        
        1. dictionary argument: delete_where({"name": "mahesh"})
        
        2. function: def name_starts_with_a(row):
                        return row["name"].startswith("m")
                    delete_where(name_starts_with_a)
        
        3. lambda:  delete_where(lambda row: int(row["age"]) > 25)

        find_where has data called 'row' which you can use to perform a conditional operation.
        It return Boolean True or False
        """
        
        if condition is None:
            raise ValueError("The condition should be valid function or dictionary. None is provided")

        removed = False
        rows = []

        with open(self.db_name, mode='r', newline='', encoding='UTF-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if self._match(row, condition):
                    removed = True
                    continue
                rows.append(row)

        if removed:
            with open(self.db_name, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writeheader()
                writer.writerows(rows)

        return removed




    def update(self, key, value, new_data: dict):
        """
        update function
        """
        updated = False
        rows = []

        with open(self.db_name, mode='r', newline='', encoding='UTF-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get(key) == value:
                    row.update(new_data)
                    updated = True
                rows.append(row)

        if updated:
            with open(self.db_name, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writeheader()
                writer.writerows(rows)

        return updated

    def delete(self, key, value):
        removed = False
        rows = []

        with open(self.db_name, mode='r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get(key) == value:
                    removed = True
                    continue
                rows.append(row)

        if removed:
            with open(self.db_name, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writeheader()
                writer.writerows(rows)

        return removed

    def delete_db(self):
        if os.path.exists(self.db_name):
            os.remove(self.db_name)
            return True
        return False
    


    #private function to match condition and perform update & delete operation
    def _match(self, row, condition):
        if callable(condition):
            return condition(row)
        elif isinstance(condition, dict):
            if not condition:
                raise ValueError("""Provided dictionary is an empty
                    Your input: {condition}
                    Check wether something missing in your input.""")
            for key in condition:
                if key not in self.headers:
                    raise ValueError("""Invalid key in condition: '{key}'
                            Check your provided key is valid field name
                            """)
            return all(row.get(k) == v for k, v in condition.items())
        else:
            raise TypeError("Condition must be a dictionary or a function.")

