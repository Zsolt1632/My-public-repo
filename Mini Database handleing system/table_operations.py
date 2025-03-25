import json
import os
import re

from server_files.database_structure import database_folder, get_current_database_name


# tabla letrehozasa
# table_name=nev
# columns=parameterek
def create_table(table_name, columns):
    try:
        with open(f"{os.path.join(database_folder, get_current_database_name())}.json", "r") as database_file:
            database = json.load(database_file)

        if table_name in database:
            return 1
        values = {}
        columns = columns.strip().removeprefix("(").removesuffix(")")
        columns = re.split(r",(?![^(]*\))", columns)
        for column in columns:
            column = column.strip()
            if column.startswith("primary key"):
                key_columns = column.removeprefix("primary key").strip()
                key_columns = list(map(lambda x: x.strip(), key_columns.removeprefix("(").removesuffix(")").split(",")))
                for col in key_columns:
                    if values[col]["constraints"] != 'null' and "primary key" not in values[col]["constraints"]:
                        values[col]["constraints"] += " primary key"
                    else:
                        values[col]["constraints"] = "primary key"
            else:
                tokens = column.strip().split(" ")
                column_name = tokens[0]
                column_type = tokens[1]
                constraints = " ".join(tokens[2:]) if len(tokens) > 2 else None
                values[column_name] = {"type": column_type, "constraints": constraints}

        database[table_name] = values

        with open(f"{os.path.join(database_folder, get_current_database_name())}.json", "w") as database_file:
            json.dump(database, database_file, indent=4)
    except Exception:
        return 1
    return 0


# tabla torlese
# table_name=nev
def delete_table(table_name):
    try:
        with open(f"{os.path.join(database_folder, get_current_database_name())}.json", "r") as database_file:
            database = json.load(database_file)

        if table_name not in database:
            return 1

        del database[table_name]
        with open(f"{os.path.join(database_folder, get_current_database_name())}.json", "w") as database_file:
            json.dump(database, database_file, indent=4)
    except Exception:
        return 1
    return 0
