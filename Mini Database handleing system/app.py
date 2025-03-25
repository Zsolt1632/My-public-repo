import tkinter as tk
import subprocess as sub
from tkinter import ttk

from server_files.communication import receive_folder_structure, send_data_to_server
from app_files.db_menu import DeleteDatabase, CreateDatabase
from app_files.manip_menu import DeleteFromTable, InsertIntoTable, Select
from app_files.table_menu import CreateIndex, CreateTable, DeleteTable
from app_files.use_menu import UseDatabase


class MainApplication(tk.Tk):
    def __init__(self):
        tk.Tk.__init__(self)
        self.title("DBMS")
        self.geometry("1280x1024")
        self.container = tk.Frame(self)
        self.container.pack(side="top", fill="both", expand=True)

        self.frames = {}
        self.current_frame = None
        start_page = StartPage(self.container, self)
        self.frames["StartPage"] = start_page
        start_page.grid(row=0, column=0, sticky="nsew")
        for F in (CreateDatabase, DeleteDatabase, CreateTable, DeleteTable, CreateIndex, UseDatabase, InsertIntoTable,
                  DeleteFromTable, Select):
            frame = F(self.container, self, start_page)
            self.frames[f"{F.__name__}"] = frame
            frame.grid(row=0, column=0, sticky="nsew")

        self.show_frame("StartPage")

        self.protocol("WM_DELETE_WINDOW", self.on_closing)

    def show_frame(self, cont):
        if self.current_frame:
            self.current_frame.grid_forget()

        frame = self.frames[cont]
        frame.grid(row=0, column=0, sticky="nsew")
        frame.tkraise()

        self.current_frame = frame

        if cont == CreateTable:
            frame.reset(self)

    def on_closing(self):
        send_data_to_server("exit")
        self.destroy()


class StartPage(tk.Frame):
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        self.menu_setup(parent, controller)

    def menu_setup(self, parent, controller):
        menubar = tk.Menu(parent.master)

        db_menu = tk.Menu(menubar, tearoff=0)
        db_menu.add_command(label="Create Database", command=lambda: controller.show_frame("CreateDatabase"))
        db_menu.add_command(label="Delete Database", command=lambda: controller.show_frame("DeleteDatabase"))
        menubar.add_cascade(label="Database", menu=db_menu)

        table_menu = tk.Menu(menubar, tearoff=0)
        table_menu.add_command(label="Create Table", command=lambda: controller.show_frame("CreateTable"))
        table_menu.add_command(label="Delete Table", command=lambda: controller.show_frame("DeleteTable"))
        table_menu.add_command(label="Create Index", command=lambda: controller.show_frame("CreateIndex"))
        menubar.add_cascade(label="Table", menu=table_menu)

        use_menu = tk.Menu(menubar, tearoff=0)
        use_menu.add_command(label="Use Database", command=lambda: controller.show_frame("UseDatabase"))
        menubar.add_cascade(label="Use", menu=use_menu)

        manip_menu = tk.Menu(menubar, tearoff=0)
        manip_menu.add_command(label="Insert into table", command=lambda: controller.show_frame("InsertIntoTable"))
        manip_menu.add_command(label="Delete from table", command=lambda: controller.show_frame("DeleteFromTable"))
        manip_menu.add_command(label="Select", command=lambda: controller.show_frame("Select"))
        menubar.add_cascade(label="Data", menu=manip_menu)

        execute_menu = tk.Menu(menubar, tearoff=0)
        execute_menu.add_command(label="Execute", command=self.executeScript)
        menubar.add_cascade(label="Execute", menu=execute_menu)

        self.treeview = ttk.Treeview(self)
        self.treeview.pack(side=tk.LEFT, fill="both", expand=True)
        self.update_folder_treeview()
        
        # Text field + scrollbar for script input
        self.text_frame = tk.Frame(self)
        self.text_frame.pack(fill="both", expand=True)

        self.text_field = tk.Text(self.text_frame)
        self.text_field.pack(side=tk.LEFT, fill="both", expand=True)

        self.scrollbar = tk.Scrollbar(self.text_frame, orient=tk.VERTICAL, command=self.text_field.yview)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.text_field.configure(yscrollcommand=self.scrollbar.set)
        
        # Read-only text field + scrollbar for script output
        self.output_frame = tk.Frame(self)
        self.output_frame.pack(fill="both", expand=True)

        self.output_field = tk.Text(self.output_frame, state='disabled', bg='green')
        self.output_field.pack(side=tk.LEFT, fill="both", expand=True)

        self.scrollbar1 = tk.Scrollbar(self.output_frame, orient=tk.VERTICAL, command=self.output_field.yview)
        self.scrollbar1.pack(side=tk.RIGHT, fill=tk.Y)

        self.output_field.configure(yscrollcommand=self.scrollbar1.set)

        parent.master.config(menu=menubar)

    def executeScript(self):
        content = self.text_field.get("1.0", "end")

        lines = content.split("\n")

        statements = ""
        if lines:
            for line in lines:
                line = line.strip()
                if line and not line.startswith("/*"):
                    statements += " " + line
            statements = statements.strip().rstrip(";").split(";")

            self.output_field.configure(state='normal')
            self.output_field.delete("1.0", "end")
            self.output_field.configure(state='disabled')
            for statement in statements:
                response = send_data_to_server(statement)
                #responses.append(response)
                
                # Update the read-only text field with responses
                self.output_field.configure(state='normal')
                self.output_field.insert(tk.END, response + "\n")
                self.output_field.configure(state='disabled')
                self.output_field.yview(tk.END)
                
                if "create database" in statement.lower() or "drop database" in statement.lower():
                    self.update_folder_treeview()

    def update_folder_treeview(self):
        folder_structure = receive_folder_structure()
        self.treeview.delete(*self.treeview.get_children())
        self.populate_tree("", folder_structure)

    def populate_tree(self, parent, structure):
        for item, subitems in structure.items():
            if subitems:
                folder_id = self.treeview.insert(parent, "end", text=item, open=False)
                self.populate_tree(folder_id, subitems)
            else:
                self.treeview.insert(parent, "end", text=item)


def main():
    #runServer = 'server.py'
    #procces = sub.Popen(['python', runServer])

    app = MainApplication()
    app.mainloop()

    #procces.communicate()


if __name__ == "__main__":
    main()
