# 📊 datacsv.py – Lightweight CSV Database in Pure Python

> A minimalist, zero-dependency, file-based CSV database for local Python automation.

![datacsv demo](https://raw.githubusercontent.com/mvish77/datacsv/main/assets/logo.svg)

---

## 📦 What is datacsv?

`datacsv` is a **pure Python** utility that lets you treat CSV files like simple databases. No need for Pandas, SQLite, or Excel. Just plug in your CSV File and get fast, safe read/write operations – directly from Python script.


---

## 🔧 Features

- ✅ A very Lightweight, no dependencies, beginner-friendly
- ✅ Auto-casts types: strings, integers, floats, booleans
- ✅ Insert, update, delete rows like a database
- ✅ Query/filter/search with ease
- ✅ Advance searching with function pass
- ✅ Clean and simple python methods to perform operations
- ✅ JSON & HTML export with indentation support
- ✅ Custom error handling and type safety

---
## 📈 Common Use Cases

- ✅ Maintain a local CSV-based "database"
- ✅ Build lightweight CLI tools
- ✅ Prototype data models quickly without installing SQL
- ✅ Store and export user logs or events
- ✅ Analyze data with filters and conditions
- ✅ Share flat file databases easily across environments
- ✅ Store server logs according to different userbase
- ✅ It also helpful to create blog post with no database setup
- ✅ You can create multipage csv database just by creating its object

---

## 💻 Installation

```bash
# Download the single Python file
git clone https://github.com/mvish77/datacsv.git
cd datacsv
```
## 🐍 Usage in Python
### 1. Create a new CSV database
```python
from datacsv import CSVDatabase

db = CSVDatabase('users.csv', ['id', 'name', 'email'])
db.insert({'id': 1, 'name': 'Alice', 'email': 'alice@example.com'})
print(db.find_all())
```
### 1. OR Load existing CSV
```python
from datacsv import CSVDatabase

db = CSVDatabase('users.csv')  # Automatically loads headers
print(db.find_all())
```

## 🧠 API Reference – All Functions with Examples
Here are the core methods provided by `CSVDatabase`, along with their usage.

### 1. `insert(data: dict)` - 
Inserts a new row into the CSV.
```python
db.insert({'id': 1, 'name': 'Alice', 'email': 'alice@example.com'})
```
### 2. `find(field: str, value: Any)` → dict or None
Returns the first row where the field matches the given value.
```python
result = db.find('id', 1)
print(result)  # {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'}
```
### 3. `find_all(field: str)` → List[Any]
Returns a list of all values from the specified column.
```python
all = db.find_all() # return everything from database in list
emails = db.find_all('email') # return only specific key values
print(emails)  # ['alice@example.com', 'bob@example.com']
```
### 4. `find_where(condition: Callable[[dict], bool])` → List[dict]
Returns all rows where the condition returns True.
```python
results = db.find_where(lambda row: row['name'].startswith('A'))
print(results)  # [{'id': 1, 'name': 'Alice', ...}]
```
OR
```python
def gt_id(row):
    return row['id'] > 5
results = db.find_where(gt_id)
print(results)  # [{'id': 1, 'name': 'Alice', ...}]
```
### 5. `update(key,value, new_data: dict)`
Updates rows where a field matches the value.
```python
db.update('id',1,{'name': 'Alicia'})
```

### 6. `update_where(condition: Callable[[dict], bool], new_data: dict)`
Updates all rows where condition returns True, replacing fields with new_data.
```python
db.update_where(lambda row: row['name'].startswith('B'), {'email': 'bob@newmail.com'})
```
OR
```python
def gt_name(row):
    return row['name'].startswith('B')
results = db.update_where(gt_name)
print(results)  # [{'id': 2, 'name': 'Bob', ...}]
```
### 7. `delete(key, value)`
Deletes all rows where field == value.
```python
db.delete('id',1)
```
### 8. `delete_where(condition: Callable[[dict], bool])`
Deletes all rows where the condition returns True.
```python
def gt_name(row):
    return row['name'].startswith('B')
db.delete_where(gt_name) # return True else False
```
### 9. `delete_db()`
Permanently deletes the CSV file from disk.
```python
db.delete_db()
```
## 📤 Export Methods
Methods to export or print database in JSON or HTML format
### 1. `to_json(indent: int = 4)`
Exports the entire CSV content as JSON string.
```python
json_output = db.to_json()
print(json_output)
```
### 2. `to_html(table_class:str)`
```python
html_output = db.to_html()
print(html_output)
```
---

## 🚀 Future Enhancements

Here are some features planned for future versions:

- ✅ Type-safe schema validation for rows
- ✅ Auto-generate unique IDs for primary key fields
- ✅ Indexing support for faster reads on large files
- ✅ Date/time field parsing and conversion
- ✅ Built-in CSV to SQLite converter
- ✅ Import/export to Excel (XLSX)

#### Feel free to suggest more by opening an issue!
---

## 🤝 Contributions Welcome!

Your contributions are welcome to make this project even better.

### To contribute:

1. Fork the repository
2. Create a new branch (`git checkout -b feature/some-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin feature/some-feature`)
5. Create a new Pull Request

If you're fixing bugs or enhancing features, include relevant tests.


## 📌 Badge & Visual

![DataCSV](https://img.shields.io/badge/datacsv-csv--database-blueviolet?style=flat-square)

> A minimal Python class to manage CSV files like a lightweight database.  
> Ideal for prototyping, quick CLI tools, and managing structured flat data with ease.
---

## 📜 License
MIT license
