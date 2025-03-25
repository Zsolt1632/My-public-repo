import json
import os

database_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Databases")  # dynamic folder path
current_database_name = "master"


def get_folder_structure(starting_path):
    folder_structure = {}
    for item in os.listdir(starting_path):
        item_path = os.path.join(starting_path, item)
        if os.path.isdir(item_path):
            folder_structure[item] = get_folder_structure(item_path)
        else:
            folder_structure[item] = None
    return folder_structure


def get_table_columns(table_name):
    try:
        with open(f"{os.path.join(database_folder, current_database_name)}.json", "r") as database_file:
            database = json.load(database_file)
        if table_name in database:
            return list(database[table_name].keys())
        else:
            return []
    except Exception as e:
        return []


def get_current_database_name():
    return current_database_name


def set_current_database_name(new_database_name):
    global current_database_name
    current_database_name = new_database_name


def load_table_names():
    table_names = []
    database_file = os.path.join('Databases', f'{get_current_database_name()}.json')
    try:
        with open(database_file, 'r') as file:
            database_data = json.load(file)
            table_names = list(database_data.keys())
    except FileNotFoundError:
        print(f"Database file '{database_file}' not found.")
    except Exception as e:
        print(f"Error loading table names: {e}")

    print("\n\ntable names: ", table_names)
    return table_names

