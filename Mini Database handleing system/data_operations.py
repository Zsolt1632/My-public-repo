import json
import os
import re
from collections import defaultdict
from datetime import datetime

import pymongo

from server_files.database_structure import database_folder, get_current_database_name

client = pymongo.MongoClient("mongodb://localhost:27017/")


# index letrehozasa
# table_name=tabla neve
# values=oszlopok nevei
def create_index(index_name, table_name, values):
    try:
        # Construct the database path
        database_path = f"{os.path.join(database_folder, get_current_database_name())}.json"

        if not os.path.exists(database_path):
            return 1

        with open(database_path, "r") as database_file:
            database = json.load(database_file)

            if table_name not in database:
                return 1

            columns = list(database[table_name].keys())

            values = list(map(lambda x: x.strip(), values.removeprefix("(").removesuffix(")").split(",")))
            indeces = []
            # Validate column names
            for value in values:
                if value not in columns:
                    return 1
                indeces.append(get_index_of_value(columns, value))

            # Create collection of indices
            index_collection = {value: [] for value in values}

            db = client[get_current_database_name()]

            # Select the collection
            collection = db[table_name]

            # Retrieve all documents from the collection
            documents = collection.find()

            # Convert documents to a list
            key_values = {}
            for document in documents:
                # Access the "_id" and "Value" fields
                row = []
                _id = document["_id"]
                row.append(_id)
                for c in document["Value"].split("#"):
                    row.append(c)

                idx = columns.index(values[0])
                if row[idx] not in key_values:
                    key_values[row[idx]] = row[0]
                else:
                    key_values[row[idx]] += "#" + row[0]

            # Join all index data strings

            # Add the "index" constraint to the specified columns
            for column_name in values:
                add_index_constraint_to_column(table_name, column_name)

            # Insert index data into the table
            if index_name == '':
                index_name = f'{table_name}.{"+".join(values)}_index'
            insert_indices(index_name, key_values)

            print(f"Index Collection for {index_name}:", index_collection)

            return 0

    except Exception as e:
        print(e)
        return 1


def insert_indices(index_name, index_data, docs, key_values):
    try:
        # Connect to your MongoDB client
        client = pymongo.MongoClient("mongodb://localhost:27017/")

        db = client[get_current_database_name()]

        collection = db[index_name]

        # Insert the document into the collection
        #    collection.insert_one(index_document)
        for key, value in key_values.items():
            index_document = {
                "_id": key,
                "value": value
            }
            collection.insert_one(index_document)

        print(f"Indices inserted successfully for {index_name}")
        return 0
    except Exception as e:
        print(f"Error inserting indices for {index_name}: {e}")
        return 1


def add_index_constraint_to_column(table_name, column_name):
    try:
        # Construct the database path
        database_path = f"{os.path.join(database_folder, get_current_database_name())}.json"

        if not os.path.exists(database_path):
            return 1

        with open(database_path, "r") as database_file:
            database_structure = json.load(database_file)

            if table_name in database_structure and column_name in database_structure[table_name]:
                if "index" not in database_structure[table_name][column_name]:
                    database_structure[table_name][column_name]["index"] = "true"

                # Write the updated JSON back to the file
                with open(database_path, "w") as file:
                    json.dump(database_structure, file, indent=4)

                return 0
            else:
                return 1

    except Exception as e:
        print(e)
        return 1


def get_index_of_value(lst, value):
    try:
        ind = lst.index(value)
        return ind
    except ValueError:
        return -1


def get_table_names(json_path):
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"The file {json_path} does not exist.")

    with open(json_path, 'r') as file:
        data = json.load(file)

    table_names = list(data.keys())
    return table_names


# adatok beillesztese MongoDBn keresztul
# table_name=tabla neve
# data=oszlopok nevei es ertekei
def insert_into_table(table_name, data):
    if not os.path.exists(f"{os.path.join(database_folder, get_current_database_name())}.json"):
        return 1

    with open(f"{os.path.join(database_folder, get_current_database_name())}.json", "r") as database_file:
        database = json.load(database_file)
    if table_name not in database:
        return 1
    # table value
    columns = database[table_name]

    # database declaring
    db = client[get_current_database_name()]
    column = db[table_name]

    # key name
    key = []
    # searching for primary key constraint
    for column_name, column_info in columns.items():
        if 'primary key' in column_info.get('constraints', '') if column_info.get('constraints') is not None else '':
            key.append(column_name)
    key = list(map(lambda x: x.strip(), key))
    # data splitting based on names and values
    data = data.split("values")
    names = data[0].strip().removeprefix("(").removesuffix(")").replace("'", "").split(",")
    names = list(map(lambda x: x.strip(), names))
    values = data[1].strip().removeprefix("(").removesuffix(")").replace("'", "").split(",")
    values = list(map(lambda x: x.strip(), values))

    # getting key(id) value
    key_index = [names.index(k) for k in key]
    key_values = [values[i] for i in key_index]

    # If any key value is missing, return error
    if len(key_values) != len(key):
        return 2

    # extracting the correct value types
    expected_types = [columns[column]['type'] for column in names]
    # testing all the types of the inserted values to the correct values
    for i, (value, expected_type) in enumerate(zip(values, expected_types)):
        if expected_type.startswith("varchar"):
            if not (isinstance(value, str) or not len(value) <= int(expected_type.split("(")[1][:-1])):
                return 3
        elif expected_type.startswith("int"):
            if not value.isdigit():
                return 3
            if re.match(r".*\(\d+\)$", expected_type) and not len(value) <= int(expected_type.split("(")[1][:-1]):
                return 3
        elif expected_type == "float":
            try:
                float(value)
            except ValueError:
                return 3
        elif expected_type == "bit":
            if value.lower() not in ('0', '1', 'true', 'false'):
                return 3
        elif expected_type == "date":
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                return 3
        elif expected_type == "datetime":
            try:
                datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return 3
        else:
            return 4

    # _id value searching based on found key name
    id_vals = "#".join(key_values)
    # joining the column values and finally inserting into mongoDB
    all_data = "#".join([x for x in values if x not in key_values])
    if column.find_one({"_id": id_vals}):
        return 1
    else:
        column.insert_one({"_id": id_vals, "Value": all_data})
        return 0


def get_table_data(database_name, table_name):
    database_file = os.path.join("Databases", f"{database_name}.json")
    with open(database_file, "r") as file:
        json_data = json.load(file)
    if table_name not in json_data:
        raise ValueError(f"Table {table_name} does not exist in the database {database_name}.")
    return json_data[table_name]


def get_referenced_table_and_column(table_name):
    database_name = get_current_database_name()
    database_file = os.path.join("Databases", f"{database_name}.json")

    # Read the JSON data from the file
    with open(database_file, "r") as file:
        json_data = json.load(file)

    references = []

    # Check if the specified table exists in the JSON data
    if table_name not in json_data:
        raise ValueError(f"Table {table_name} does not exist in the database {database_name}.")

    # Iterate over the columns of the specified table
    columns = json_data[table_name]
    for column, properties in columns.items():
        constraints = properties.get("constraints")
        if constraints and constraints.startswith("references"):
            ref_table_col = constraints.split("references")[1].strip()
            ref_table, ref_column = ref_table_col.split("(", 1)
            ref_column = ref_column.strip(")")
            references.append((column, ref_table.strip(), ref_column.strip()))

    return references


def check_referenced_value_exists(database_name, ref_table, ref_column, value):
    table_data = get_table_data(database_name, ref_table)
    for row in table_data.values():
        if ref_column in row and row[ref_column] == value:
            return True
    return False


def delete_from_table(table_name, data):
    if not os.path.exists(f"{os.path.join(database_folder, get_current_database_name())}.json"):
        return 1

    with open(f"{os.path.join(database_folder, get_current_database_name())}.json", "r") as database_file:
        database = json.load(database_file)
    if table_name not in database:
        return 1
    columns = database[table_name]

    db = client[get_current_database_name()]
    column = db[table_name]

    key = []
    for column_name, column_info in columns.items():
        if 'primary key' in column_info.get('constraints', '') if column_info.get('constraints') is not None else '':
            key.append(column_name)
    key = list(map(lambda x: x.strip(), key))
    key = ",".join(key)

    if data:
        id_vals = select_from(key, table_name + " " + data)
        if isinstance(id_vals, int):
            return id_vals

        del id_vals[0]
        for ID_val in id_vals:
            pk = "#".join(ID_val)
            result = column.find_one({"_id": pk})
            print(result)
            if not result:
                return 1
            else:
                references = get_referenced_table_and_column(table_name)
                for ref in references:
                    ref_table, ref_column = ref[1], ref[2]
                    if check_referenced_value_exists(get_current_database_name(), ref_table, ref_column, pk):
                        print(f"Cannot delete: Value {pk} is referenced in table {ref_table}({ref_column})")
                        return 1
                column.delete_one({"_id": pk})
        return 0
    else:
        column.drop()
        return 0


def update_table(table_name, data):
    if not os.path.exists(f"{os.path.join(database_folder, get_current_database_name())}.json"):
        return 1

    with open(f"{os.path.join(database_folder, get_current_database_name())}.json", "r") as database_file:
        database = json.load(database_file)
    if table_name not in database:
        return 1
    columns = database[table_name]

    db = client[get_current_database_name()]
    column = db[table_name]

    key = []
    for column_name, column_info in columns.items():
        if 'primary key' in column_info.get('constraints', '') if column_info.get('constraints') is not None else '':
            key.append(column_name)

    key = list(map(lambda x: x.strip(), key))
    keynum = len(key)
    key = ",".join(key)

    data = data.removeprefix("set").split('where')
    change = data[0]
    query = data[1]
    if query:
        id_vals = select_from(key, table_name + " where " + query)
        del id_vals[0]

        for ID_val in id_vals:
            pk = "#".join(ID_val)
            result = column.find_one({"_id": pk})
            print(result)
            if not result:
                return 1
            else:
                # update documents
                changes = change.split("and")
                for c in changes:
                    c_name, val = c.split("=")
                    c_name = c_name.strip().replace("'", "")
                    val = val.strip().replace("'", "")
                    current_values = result['Value'].split('#')
                    for i, name, in enumerate(columns):
                        if name.strip() == c_name:
                            current_values[i - keynum] = val
                            break
                    new_value = '#'.join(current_values)
                    column.update_one({"_id": pk}, {"$set": {"Value": new_value}})
        return 0
    else:
        # update everything if no where clause
        changes = change.split("and")
        for doc in column.find():
            for c in changes:
                c_name, val = c.split("=")
                c_name = c_name.strip().replace("'", "")
                val = val.strip()
                current_values = doc['Value'].split('#')
                for i, col_value in enumerate(current_values):
                    if c_name == columns[i]:
                        current_values[i] = val
                        break
                new_value = '#'.join(current_values)
                column.update_one({"_id": doc["_id"]}, {"$set": {"Value": new_value}})
        return 0


def get_column_names(json_file_path):
    with open(json_file_path, 'r') as file:
        database_schema = json.load(file)

    column_names = {}

    for table_name, table_schema in database_schema.items():
        column_names[table_name] = list(table_schema.keys())

    return column_names


def select_simple(table_name, column_name):
    # Get column names and indices
    columns = get_column_names(f"{os.path.join(database_folder, get_current_database_name())}.json")
    column_indices = {col: idx for idx, col in enumerate(columns[table_name])}
    distinct = False
    if column_name[0] not in columns:
        if column_name[0] == "distinct":
            distinct = True
            column_name = " ".join(column_name[1:])
    else:
        return 1
    column_values = []
    print(column_indices)

    db = client[get_current_database_name()]
    collection = db[table_name]
    documents = collection.find()

    # Retrieve column values from documents
    for document in documents:
        value = " ".join(document['_id'].split("#")) + " " + " ".join(document['Value'].split("#"))
        value = value.split(" ")
        column_values.append(value)

    # Find based on tuple value in column_values
    if column_name == "*":
        matching_documents = [columns[table_name]]
    else:
        column_name = column_name.split(",")
        column_name = list(map(lambda x: x.strip(), column_name))
        matching_documents = [column_name]
    for value_set in column_values:
        match = True
        if match:
            if column_name == "*":
                matching_documents.append(value_set)
            else:
                value = ""
                for column in column_name:
                    value += " ".join([value_set[column_indices[column.strip()]]]) + "#"
                selected_values = value.split("#")[:-1]
                if distinct:
                    if selected_values not in matching_documents:
                        matching_documents.append(selected_values)
                else:
                    matching_documents.append(selected_values)
    print("matched values = ", matching_documents)
    return matching_documents


def select_with_conditions(sql_parts, column_name):
    table_name = sql_parts["table_name"]
    aliases = sql_parts["aliases"]
    conditions = sql_parts["specifications"]["conditions"]
    group_by_columns = sql_parts["specifications"]["group_by_columns"]
    aggregate_functions = sql_parts["specifications"]["aggregate_functions"]
    # Get column names and indices
    columns = get_column_names(f"{os.path.join(database_folder, get_current_database_name())}.json")
    if table_name not in columns:
        raise ValueError(f"Invalid table name: {table_name}")

    column_indices = {col: idx for idx, col in enumerate(columns[table_name])}

    def parse_condition(condition):
        operators = ['>=', '<=', '!=', '=', '>', '<']
        for operator in operators:
            if operator in condition:
                parts = condition.split(operator)
                if len(parts) == 2:
                    column, value = parts
                    column = column.strip()
                    value = value.strip()
                    if value.isdigit():
                        value = int(value)
                    else:
                        value = value.strip('\'"')
                    return column, operator, value
        return None, None, None

    def evaluate_condition(value, operator, condition_value):
        # Check if value and condition_value can be interpreted as numbers
        try:
            value_num = float(value)
            condition_value_num = float(condition_value)
            is_numeric = True
        except ValueError:
            is_numeric = False

        # Handle numeric comparison
        if is_numeric:
            if operator == '=':
                return value_num == condition_value_num
            elif operator == '!=':
                return value_num != condition_value_num
            elif operator == '>':
                return value_num > condition_value_num
            elif operator == '<':
                return value_num < condition_value_num
            elif operator == '>=':
                return value_num >= condition_value_num
            elif operator == '<=':
                return value_num <= condition_value_num
        # Handle string comparison
        else:
            if operator == '=':
                return value == str(condition_value)
            elif operator == '!=':
                return value != str(condition_value)
            elif operator == '>':
                return value > str(condition_value)
            elif operator == '<':
                return value < str(condition_value)
            elif operator == '>=':
                return value >= str(condition_value)
            elif operator == '<=':
                return value <= str(condition_value)

    def evaluate_document(document, conditions):
        logical_ops = {'and', 'or'}
        if not conditions:
            result = True
        else:
            result = None
        current_op = 'and'

        for condition in conditions:
            if condition in logical_ops:
                current_op = condition.lower()
                continue
            column, operator, condition_value = parse_condition(condition)
            if column is None or operator is None or condition_value is None:
                raise ValueError(f"Invalid condition: {condition}")
            doc_value = document[column_indices[column]]
            condition_result = evaluate_condition(doc_value, operator, condition_value)
            if result is None:
                result = condition_result
            elif current_op == 'and':
                result = result and condition_result
            elif current_op == 'or':
                result = result or condition_result
        return result

    # Check for distinct
    distinct = False
    if column_name.startswith("distinct "):
        distinct = True
        column_name = column_name[len("distinct "):].strip()

    column_names = [col.strip() for col in column_name.split(",")]
    if not column_name:
        column_names.remove("")

    all_columns = set(column_names)
    if "specifications" in sql_parts and "aggregate_functions" in sql_parts["specifications"]:
        for func, col, _ in sql_parts["specifications"]["aggregate_functions"]:
            all_columns.add(col)

    # Check if column_name is valid
    if column_name != "*" and not all(col in columns[table_name] for col in all_columns):
        raise ValueError(f"Invalid column name: {column_name}")

    column_values = []
    print(column_indices)

    db = client[get_current_database_name()]
    collection = db[table_name]
    documents = collection.find()

    # Retrieve column values from documents
    for document in documents:
        value = " ".join(document['_id'].split("#")) + " " + " ".join(document['Value'].split("#"))
        value = value.split(" ")
        column_values.append(value)

    matching_indices = {}
    matching_documents = []
    if column_name == "*":
        matching_documents.append(columns[table_name])
    else:
        displayed_names = []
        for col in column_names:
            if sql_parts["aliases"][col]:
                displayed_names.append(sql_parts["aliases"][col])
            else:
                displayed_names.append(col)
        matching_documents.append(displayed_names)

    for document in column_values:
        if evaluate_document(document, conditions):
            if column_name == "*":
                matching_documents.append(document)
            else:
                selected_values = ""
                for i, column in enumerate(all_columns):
                    selected_values += " ".join([document[column_indices[column.strip()]]]) + "#"
                    matching_indices[column] = i
                selected_values = selected_values.split("#")[:-1]
                if distinct:
                    if selected_values not in matching_documents:
                        matching_documents.append(selected_values)
                else:
                    matching_documents.append(selected_values)

    if group_by_columns:
        selected_columns = [col.strip() for col in column_name.split(",")]
        if not all(col in group_by_columns for col in selected_columns):
            raise ValueError("Selected columns must be included in the GROUP BY clause.")

        grouped_results = defaultdict(list)

        for document in matching_documents[1:]:
            key = tuple(document[matching_indices[idx]] for idx in group_by_columns)
            grouped_results[key].append(document)

        def calculate_aggregates(group, indices):
            aggregate_results = []
            for func, col,_ in aggregate_functions:
                col_idx = indices[col]
                values = [float(doc[col_idx]) for doc in group]
                if func == "sum":
                    aggregate_results.append(str(sum(values)))
                elif func == "count":
                    aggregate_results.append(str(len(values)))
                elif func == "avg":
                    aggregate_results.append(str(sum(values) / len(values) if values else 0))
                elif func == "max":
                    aggregate_results.append(str(max(values)))
                elif func == "min":
                    aggregate_results.append(str(min(values)))
            return aggregate_results

        grouped_documents = [[*key] for key, group in grouped_results.items() if key[0] != 'default_factory']
        matching_documents = [group_by_columns] + grouped_documents

        grouped_documents = []
        for key, group in grouped_results.items():
            aggregates = calculate_aggregates(group, matching_indices)
            grouped_documents.append([*key, *aggregates])

        aggregate_columns = [f"{alias}" if alias else f"{func}({col})" for func, col, alias in
                             aggregate_functions]
        matching_documents = [group_by_columns + aggregate_columns] + grouped_documents

    else:
        def calculate_overall_aggregates(indices):
            aggregate_results = []
            for func, col, _ in aggregate_functions:
                col_idx = indices[col]
                values = [float(doc[col_idx]) for doc in matching_documents[1:] if doc[col_idx]]
                if func == "sum":
                    aggregate_results.append(str(sum(values)))
                elif func == "count":
                    aggregate_results.append(str(len(values)))
                elif func == "avg":
                    aggregate_results.append(str(sum(values) / len(values) if values else 0))
                elif func == "max":
                    aggregate_results.append(str(max(values)))
                elif func == "min":
                    aggregate_results.append(str(min(values)))
            return aggregate_results

        aggregates = calculate_overall_aggregates(matching_indices)
        if aggregates:
            aggregate_columns = [f"{alias}" if alias else f"{func}({col})" for func, col, alias in
                                 aggregate_functions]
            matching_documents = [aggregate_columns] + [aggregates]

    print("matched values =", matching_documents)
    return matching_documents


def parse_sql_statement(column_name, data):
    sql_parts = {
        "table_name": None,
        "aliases": {},
        "specifications": {
            "conditions": [],
            "group_by_columns": None,
            "aggregate_functions": []
        },
    }
    aggregate_functions = ['sum', 'count', 'avg', 'min', 'max']

    # Split the SQL statement into parts
    split_data = re.split(r'\bWHERE\b|\bGROUP BY\b', data, flags=re.IGNORECASE)
    sql_parts["table_name"] = split_data[0].strip()

    if len(split_data) > 1:
        if 'WHERE' in data.upper():
            where_part = split_data[1].strip()
            sql_parts["specifications"]["conditions"] = re.split(r'\s+(AND|OR)\s+', where_part, flags=re.IGNORECASE)
            if len(split_data) > 2:
                sql_parts["specifications"]["group_by_columns"] = [col.strip() for col in
                                                                   split_data[2].strip().split(',')]
        elif 'GROUP BY' in data.upper():
            sql_parts["specifications"]["group_by_columns"] = [col.strip() for col in split_data[1].strip().split(',')]

    columns = [col.strip() for col in column_name.split(',')]
    parsed_columns = []

    for col in columns:
        if 'as ' in col:
            col_name, alias = [c.strip() for c in col.split(' as ')]
        else:
            col_name = col
            alias = None

        # Check if the column is an aggregate function
        is_aggregate = any(
            col_name.lower().startswith(func + '(') and col_name.endswith(')') for func in aggregate_functions)
        if is_aggregate:
            # Extract the aggregate function and column name
            for func in aggregate_functions:
                if col_name.lower().startswith(func + '(') and col_name.endswith(')'):
                    inner_col = col_name[len(func) + 1:-1].strip()  # Extract the column inside the function
                    sql_parts["specifications"]["aggregate_functions"].append((func, inner_col, alias))
        else:
            sql_parts["aliases"][col_name] = alias
            parsed_columns.append(col_name.strip())

    return ', '.join(parsed_columns), sql_parts


def select_from(column_name, data):
    column_name, sql_parts = parse_sql_statement(column_name, data)

    if sql_parts["specifications"]:
        matching_documents = select_with_conditions(sql_parts, column_name)
    else:
        matching_documents = select_simple(sql_parts["table_name"], column_name)

    if len(matching_documents) > 1:
        return matching_documents
    return 1
