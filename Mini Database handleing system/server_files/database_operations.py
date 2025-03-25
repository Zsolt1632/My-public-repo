import json
import os

from server_files.database_structure import database_folder, set_current_database_name, get_current_database_name


# aktiv adatbazis beallitasa
# database_name = nev
def use(database_name):
    # utvonalvizsga
    if os.path.exists(f"{os.path.join(database_folder, database_name)}.json"):
        set_current_database_name(database_name)
        return 0
    else:
        return 1


# adatbazis letrehozasa
# database_name = nev
def create_database(database_name):
    if not os.path.exists(f"{os.path.join(database_folder, database_name)}.json"):
        # mappa letrehozasa ha meg nem letezett
        if not os.path.exists(database_folder):
            os.makedirs(database_folder)

        # file megnyitasa mint json irasra(w)
        with open(f"{os.path.join(database_folder, database_name)}.json", "w") as database_file:
            # kezdeti adat beirasa
            json.dump({}, database_file)
        return 0
    else:
        return 1


# adatbazis eltavolitasa
# database_name = nev
def delete_database(database_name):
    try:
        # adatbazis fajl torlese
        os.remove(f"{os.path.join(database_folder, database_name)}.json")

        # ha a torolt adatbazis volt aktiv akkor visszallitjuk defaultra(master)
        if database_name == get_current_database_name():
            set_current_database_name("master")
        return 0
    except Exception:
        return 1
