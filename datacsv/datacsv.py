"""
datacsv is very lightweight and zero dependency file based database system 
that store data in csv file and provide various operation to perform on database.
"""
import os
import csv
from typing import Collection

class CSVDatabase:
    """😎 Creating a lightweight, zero-dependency, file-based database system in 
    Python using CSV as the storage backend is a great idea for small-scale applications or CLI tools.
    It helpful to create application with lightweight file based database.
    It helps to store: 
        logs and retrieve 
        basic details 
        microservices logs
        basic user data inputs"""

    def __init__(self, db_name: str, headers:Collection[str]=[]):
        """You need🤞 2 inputs as an argument to create object of datacsv.
            1. Database name - You have to provide database name in string format
            2. Fields - Headers for table that needed to perform query and update operation"""
        self.db_name = db_name if db_name.endswith(".csv") else db_name + ".csv"
        self.headers = headers

        if not os.path.exists(self.db_name):
            if headers:
                with open(self.db_name, mode='w', newline='', encoding='UTF-8') as f:
                    writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_MINIMAL)
                    writer.writeheader()
            else:
                raise ValueError("""CSVDatabase: Database does not exist, provide fields to create it.
                                    You must provide Database name in String and fields in List 
                                    For example: db = CSVDatabase('my_database.csv',['field1,'field2','field3'])""") 
        else:
            try:
                self.load()
            except:
                raise FileNotFoundError("""CSVDatabase: Database is empty. provide fields to create it.\n
                                    Your database should not be empty\n
                                    Create new database with: db = CSVDatabase('my_database.csv',['field1,'field2','field3'])""")


    def insert(self, row:dict,fill_missing:bool=False)-> bool:
        """
        Perform single insert operation to database.
        insert method accept row as dict, fill_missing in boolean
        `fill_missing` = True --> it fill null values
        example:
        ```
        insert(row,fill_missing=False)
        ```
        """
        #check for intance
        if not isinstance(row, dict):
            raise TypeError("""Your provided row is not in dictionary format.
            Your input row: {row}
            Check your input row is valid dict or not.""")
        #check for existance
        if not row:
            raise ValueError("""Your provided row empty.
            Your input row: {row}
            Check your input row has valid data or not.""")
        
        if fill_missing:
            row = {key: row.get(key, "") for key in self.headers}
        else:
            missing_keys = [key for key in self.headers if key not in row]
            if missing_keys:
                raise ValueError(f"Missing keys in row: {missing_keys}")
        # validate extra keys and raise error if user entered extra keys
        extra_keys = [key for key in row if key not in self.headers]
        if extra_keys:
            raise KeyError("""Unexpected keys in row: {extra_keys}.
            Your keys must be same as your fields.""")
        with open(self.db_name, mode='a', newline='',encoding='UTF-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(row)
            return True


    def find_all(self) -> list:
        """
        find_all is used to find all rows in database. It returns a list with auto type casting.
        """
        with open(self.db_name, mode='r', newline='', encoding='UTF-8') as f:
            reader = csv.DictReader(f, quoting=csv.QUOTE_MINIMAL)
            return [ {k: self._auto_cast(v) for k, v in row.items()} for row in reader ]


    def find(self, key:str, value):
        """
        used to find row(s) with a specific key and value. Supports type-safe search
        """
        if not key or not value:
            raise ValueError("""You must provide valid key and value to run find method.
            Your input data: KEY: {key} AND VALUE: {value}
            Key or value is missing in your inputs""")
        with open(self.db_name, mode='r', newline='', encoding='UTF-8') as f:
            reader = csv.DictReader(f, quoting=csv.QUOTE_MINIMAL)
            return [
                {k: self._auto_cast(v) for k, v in row.items()}
                for row in reader
                if self._auto_cast(row.get(key)) == value
            ]


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
            raise TypeError("Your condition function is none. Function should not be none")
        with open(self.db_name, 'r', newline='', encoding='UTF-8') as f:
            reader = csv.DictReader(f, quoting=csv.QUOTE_MINIMAL)
            return [
                {k: self._auto_cast(v) for k, v in row.items()}
                for row in reader if self._match(row, condition)
            ]


    def delete(self, key: str, value) -> bool:
        """
        perform delete operation to database
        """
        if key not in self.headers:
            raise KeyError(f"Invalid column name '{key}'")
        deleted = False
        with open(self.db_name, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, quoting=csv.QUOTE_MINIMAL)
            rows = [r for r in reader if not (r.get(key) == value and (deleted := True))]
        if deleted:
            with open(self.db_name, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.headers, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                writer.writerows(rows)
        return deleted


    def delete_where(self, condition)-> bool:
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
            raise TypeError("The condition must be a valid dictionary or function. None provided.")
        found = False
        with open(self.db_name, 'r', newline='', encoding='UTF-8') as f:
            reader = csv.DictReader(f, quoting=csv.QUOTE_MINIMAL)
            rows = []
            for r in reader:
                if self._match(r, condition):
                    found = True
                    continue  
                rows.append(r)
        if found:
            with open(self.db_name, 'w', newline='', encoding='UTF-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.headers, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                writer.writerows(rows)
        return found


    def update(self, key, value, new_data: dict)->bool:
        """
        Update rows in database
        """
        updated = False
        rows = []
        with open(self.db_name, mode='r', newline='', encoding='UTF-8') as f:
            reader = csv.DictReader(f, quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                if row.get(key) == value:
                    row.update(new_data)
                    updated = True
                rows.append(row)
        if updated:
            with open(self.db_name, mode='w', newline='', encoding='UTF-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.headers, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                writer.writerows(rows)
        return updated

    
    def update_where(self, condition, new_data):
        """
        same as find_where, update_where() accept collable function or dictionary to filter the row data from database and return boolean as output.\n
        You can pass argument like:
        
        1. dictionary argument: 
        ```
        update_where({"name": "raj"},{"name": "mahesh"})
        ```
        2. function: 
        ```
        def name_starts_with_a(row):
                return row["name"].startswith("m")
        update_where(name_starts_with_a,{"city": "Mumbai"})
        ```
        3. lambda:  
        ```
        delete_where(lambda r: int(r["age"]) > 30, {"status": "senior"})
        ```
        find_where has data called 'row' which you can use to perform a conditional operation.\n
        It return Boolean True or False
        """
        if not isinstance(new_data, dict) or not new_data:
            raise ValueError("new_data must be a non-empty dictionary.")
        for k in new_data:
            if k not in self.headers:
                raise KeyError(f"Invalid column name: '{k}'")
        updated = False
        with open(self.db_name, 'r', newline='', encoding='UTF-8') as f:
            reader = csv.DictReader(f, quoting=csv.QUOTE_MINIMAL)
            rows = []
            for r in reader:
                if self._match(r, condition):
                    r.update({k: str(v) for k, v in new_data.items()})
                    updated = True
                rows.append(r)
        if updated:
            with open(self.db_name, 'w', newline='', encoding='UTF-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.headers, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                writer.writerows(rows)
        return updated


    def delete_db(self)->bool:
        """
        Delete whole database. Make sure to backup your database before running this method. It wipe out everything
        """
        if os.path.exists(self.db_name):
            os.remove(self.db_name)
            return True
        return False
    

    def to_json(self, indent:int|None=2):
        """This function accept one string argument `indent`.\n
        It accept number as string for applying indent space to output.\n
        Use case: 
        ```mydb = CSVDatabase('user.csv',['id','name'])
        mydb.to_json(indent=4)
        # OR
        mydb.to_json()
        """
        import json
        try:
            with open(self.db_name, 'r', newline='', encoding='UTF-8') as f:
                headers = f.readline().strip().split(",")
                data = [dict(zip(headers, [self._auto_cast(l) for l in line.strip().split(",")])) for line in f if line.strip()]
            return json.dumps(data, indent=indent)
        except Exception as e:
            raise RuntimeError(f"Error while converting CSV to JSON: {e}") from e


    def to_html(self, table_class:str|None=None):
        """This function accept one string argument `table_class`.\n
        It accept class-name as string for applying css to this class.\n
        Use case: 
        ```mydb = CSVDatabase('user.csv',['id','name'])
        mydb.to_html(table_class='my-class')
        # OR
        mydb.to_html()
        ```"""
        try:
            with open(self.db_name, 'r', newline='', encoding='UTF-8') as f:
                lines = [line.strip().split(",") for line in f if line.strip()]
                if not lines:
                    return '<table></table>'
            headers = lines[0]
            rows = lines[1:]
            html =  f'<table border="1" class="{table_class}">\n,<tr>' if table_class else "<table border='1'>\n<tr>"
            html += "".join(f"<th>{h}</th>" for h in headers) + "</tr>\n"
            for row in rows:
                html += "<tr>" + "".join(f"<td>{v}</td>" for v in row) + "</tr>\n"
            html += "</table>"
            return html
        except Exception as e:
            raise RuntimeError(f"Error while rendering to HTML. Here is error log: {e}") from e


    #private function to match condition and perform update & delete operation
    def _match(self, row, condition):
        """_match checks if a row satisfies the given condition.
        Supports function or dictionary as condition.
        Auto-casts row values for type-safe comparisons.
        """
        # Apply auto_cast to row values
        casted_row = {k: self._auto_cast(v) for k, v in row.items()}
        if callable(condition):
            try:
                return condition(casted_row)
            except Exception as e:
                raise RuntimeError(f"Error in condition function: {e}") from e
        elif isinstance(condition, dict):
            if not condition:
                raise ValueError("""Provided dictionary is an empty
                    Your input: {condition}
                    Check wether something missing in your input.""")
            for k in condition:
                if k not in self.headers:
                    raise ValueError(f"Invalid key in condition: '{k}' — Not found in fields")
            return all(casted_row.get(k) == v for k, v in condition.items())
        raise TypeError("Condition must be a dictionary or a callable function. Provided condition doesn't match.")


    # advance feature - type casting for more better filter
    def _auto_cast(self, value):
        """This function try to cast value to boolean, integer or float. If value is not castable then return in string"""
        if value.lower() in {"true", "false"}:
            return value.lower() == "true"
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            return value
        

    def load(self):
        with open(self.db_name, mode='r', newline='', encoding='UTF-8') as f:
            reader = csv.reader(f)
            default_headers = next(reader)
            self.headers = default_headers