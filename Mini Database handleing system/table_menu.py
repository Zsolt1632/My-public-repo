import tkinter as tk

from app_files.communication import send_data_to_server


class CreateTable(tk.Frame):
    def __init__(self, parent, controller, start_page_instance):
        tk.Frame.__init__(self, parent)
        self.controller = controller
        self.labels_entries = []
        self.dropdown_menus = []
        self.type_text_fields = {}
        self.length_labels = {}
        self.checkbox_vars = []
        self.foreign_key_tables = []
        self.foreign_key_columns = []
        self.tables = []
        self.columns = {}
        self.reset(controller)

        self.output_field = start_page_instance.output_field

        add_button = tk.Button(self, text="Add Column", command=self.add_entry)
        add_button.grid(row=0, column=0, padx=5, pady=5, sticky="ew")

        del_button = tk.Button(self, text="Delete Column", command=self.del_entry)
        del_button.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        submit_button = tk.Button(self, text="Create Table",
                                  command=lambda: [self.insert(), controller.show_frame("StartPage")])
        submit_button.grid(row=0, column=2, padx=5, pady=5, sticky="ew")

        back_button = tk.Button(self, text="Back", command=lambda: controller.show_frame("StartPage"))
        back_button.grid(row=0, column=3, padx=5, pady=5, sticky="ew")

    def update_output_field(self, response):
        self.output_field.configure(state='normal')
        self.output_field.delete("1.0", "end")
        self.output_field.insert(tk.END, response + "\n")
        self.output_field.configure(state='disabled')
        self.output_field.yview(tk.END)

    def add_entry(self):
        row = len(self.labels_entries) + 3
        label_name = tk.Label(self, text="Column Name:")
        label_name.grid(row=row, column=0, sticky="w", padx=5, pady=5)

        entry_name = tk.Entry(self)
        entry_name.grid(row=row, column=1, sticky="ew", padx=5, pady=5)

        label_type = tk.Label(self, text="Column Type:")
        label_type.grid(row=row, column=2, sticky="w", padx=5, pady=5)

        selected_column_type = tk.StringVar(self)
        selected_column_type.set(self.column_types[0])
        selected_column_type.trace("w",
                                   lambda name, index, mode, var=selected_column_type, row=row: self.on_type_change(var,
                                                                                                                    row))

        type_dropdown = tk.OptionMenu(self, selected_column_type, *self.column_types)
        type_dropdown.grid(row=row, column=3, sticky="ew", padx=5, pady=5)

        self.dropdown_menus.append(type_dropdown)

        unique_var = tk.BooleanVar()
        unique_checkbox = tk.Checkbutton(self, text="Unique", variable=unique_var)
        unique_checkbox.grid(row=row, column=6, sticky="ew", padx=5, pady=5)

        primary_key_var = tk.BooleanVar()
        primary_key_checkbox = tk.Checkbutton(self, text="Primary Key", variable=primary_key_var)
        primary_key_checkbox.grid(row=row, column=7, sticky="ew", padx=5, pady=5)

        foreign_key_var = tk.BooleanVar()
        foreign_key_checkbox = tk.Checkbutton(self, text="Foreign Key", variable=foreign_key_var,
                                              command=lambda row=row: self.on_foreign_key_change(foreign_key_var, row))
        foreign_key_checkbox.grid(row=row, column=8, sticky="ew", padx=5, pady=5)

        self.checkbox_vars.append((unique_var, primary_key_var, foreign_key_var))

        self.labels_entries.append((label_name, entry_name, label_type, selected_column_type))

    def del_entry(self):
        if self.labels_entries:
            label_name, entry_name, label_type, selected_column_type = self.labels_entries.pop()

            label_name.destroy()
            entry_name.destroy()
            label_type.destroy()

            selected_column_type.set("")
            selected_column_type = None

            if self.dropdown_menus:
                last_dropdown_menu = self.dropdown_menus.pop()
                last_dropdown_menu.destroy()

            row = len(self.labels_entries) + 3

            if row in self.type_text_fields:
                self.type_text_fields[row].destroy()
                del self.type_text_fields[row]
            if row in self.length_labels:
                self.length_labels[row].destroy()
                del self.length_labels[row]

            if row in self.foreign_key_tables:
                self.foreign_key_tables[row].destroy()
                del self.foreign_key_tables[row]
            if row in self.foreign_key_columns:
                self.foreign_key_columns[row].destroy()
                del self.foreign_key_columns[row]

    def on_type_change(self, var, row):
        selected_type = var.get()
        if selected_type in ["INT", "VARCHAR"]:
            length_label = tk.Label(self, text="Length:")
            length_label.grid(row=row, column=4, sticky="w", padx=5, pady=5)
            self.length_labels[row] = length_label

            length_entry = tk.Entry(self)
            length_entry.grid(row=row, column=5, sticky="ew", padx=5, pady=5)
            self.type_text_fields[row] = length_entry
        else:
            if row in self.type_text_fields:
                self.type_text_fields[row].destroy()
                del self.type_text_fields[row]
            if row in self.length_labels:
                self.length_labels[row].destroy()
                del self.length_labels[row]

    def on_foreign_key_change(self, var, row):
        if var.get():
            response = send_data_to_server('get database_tables')
            print(response)
            try:
                if response:
                    tables = response.split(' ')
                    if tables:
                        foreign_table_label = tk.Label(self, text="Foreign Table:")
                        foreign_table_label.grid(row=row, column=9, sticky="w", padx=5, pady=5)

                        foreign_table_name = tk.StringVar(self)
                        foreign_table_name.set(tables[0])
                        foreign_table_menu = tk.OptionMenu(self, foreign_table_name, *tables)
                        foreign_table_menu.grid(row=row, column=10, sticky="ew", padx=5, pady=5)
                        self.foreign_key_tables.append(foreign_table_menu)

                        foreign_column_label = tk.Label(self, text="Foreign Column:")
                        foreign_column_label.grid(row=row, column=11, sticky="w", padx=5, pady=5)

                        self.update_foreign_columns(row, tables[0])  # Initially update columns for the first table
            except Exception as e:
                print("Connection to server failed:", e)
        else:
            if row < len(self.foreign_key_tables):
                self.foreign_key_tables[row].destroy()
                del self.foreign_key_tables[row]
            if row < len(self.foreign_key_columns):
                self.foreign_key_columns[row].destroy()
                del self.foreign_key_columns[row]

    def update_foreign_columns(self, row, table_name):
        response = send_data_to_server(f'get columns {table_name}')
        print(response)
        try:
            if response:
                columns = response.split(' ')
                if columns:
                    if row < len(self.foreign_key_columns):
                        # Clear existing options
                        menu = self.foreign_key_columns[row]
                        menu['menu'].delete(0, 'end')
                        # Add new options
                        for column in columns:
                            menu['menu'].add_command(label=column, command=tk._setit(menu['var'], column))
                    else:
                        foreign_column_var = tk.StringVar(self)
                        foreign_column_var.set(columns[0])  # Default value
                        foreign_column_menu = tk.OptionMenu(self, foreign_column_var, *columns)
                        foreign_column_menu.grid(row=row, column=12, sticky="ew", padx=5, pady=5)

                        # Label for the column dropdown menu
                        foreign_column_label = tk.Label(self, text="Foreign Column:")
                        foreign_column_label.grid(row=row, column=11, sticky="w", padx=5, pady=5)

                        self.foreign_key_columns.append(foreign_column_menu)
        except Exception as e:
            print("Connection to server failed:", e)

    def insert(self):
        table_name = self.table_name_entry.get()
        text_entries = []
        for index, (_, entry_name, _, selected_column_type) in enumerate(self.labels_entries):
            column_type = selected_column_type.get()
            entry = entry_name.get()
            if column_type in ["INT", "VARCHAR"]:
                row = index  # Adjust row calculation
                # Debugging: print row and self.type_text_fields
                print(f"Processing row {row}, self.type_text_fields: {self.type_text_fields}")
                # Check if the row exists in self.type_text_fields and is not empty
                if row in self.type_text_fields and self.type_text_fields[row].get():
                    length = self.type_text_fields[row].get()
                    if length:
                        column_type += f"({length})"
            unique_var, primary_key_var, foreign_key_var = self.checkbox_vars[index]

            column_definition = entry + " " + column_type
            if unique_var.get():
                column_definition += " UNIQUE"
            if primary_key_var.get():
                column_definition += " PRIMARY KEY"
            if foreign_key_var.get():
                # Check if index exists in foreign_key_tables and foreign_key_columns lists
                if index < len(self.foreign_key_tables) and index < len(self.foreign_key_columns):
                    foreign_table = self.foreign_key_tables[index].cget('text')
                    foreign_column = self.foreign_key_columns[index].cget('text')
                    column_definition += f' FOREIGN KEY REFERENCES ({foreign_table}.{foreign_column})'
                else:
                    print(f"Error: Index {index} is out of range for foreign key references")

            text_entries.append(column_definition)

        text = ", ".join(text_entries)
        query = f"CREATE TABLE {table_name} ({text})"
        print("Query:", query)
        # Sending data to the server
        res = send_data_to_server(query)
        self.update_output_field(res)

    def reset(self, controller):
        for widget_set in self.labels_entries:
            for widget in widget_set:
                if not isinstance(widget, tk.StringVar):
                    widget.destroy()

        self.labels_entries.clear()
        self.dropdown_menus.clear()
        self.type_text_fields.clear()
        self.length_labels.clear()
        self.checkbox_vars.clear()
        self.foreign_key_tables.clear()
        self.foreign_key_columns.clear()

        label_table_name = tk.Label(self, text="Table Name:")
        label_table_name.grid(row=1, column=0, sticky="w", padx=5, pady=5)

        self.table_name_entry = tk.Entry(self)
        self.table_name_entry.grid(row=1, column=1, sticky="ew", padx=5, pady=5)

        label2 = tk.Label(self, text="Column Name:")
        label2.grid(row=2, column=0, sticky="w", padx=5, pady=5)

        self.entry2 = tk.Entry(self)
        self.entry2.grid(row=2, column=1, sticky="ew", padx=5, pady=5)

        label3 = tk.Label(self, text="Column Type:")
        label3.grid(row=2, column=2, sticky="w", padx=5, pady=5)

        self.column_types = ["---", "INT", "FLOAT", "BIT", "DATE", "DATETIME", "VARCHAR"]

        self.selected_column_type = tk.StringVar(self)
        self.selected_column_type.set(self.column_types[0])
        self.selected_column_type.trace("w", lambda name, index, mode, var=self.selected_column_type,
                                                    row=2: self.on_type_change(var, row))

        type_menu = tk.OptionMenu(self, self.selected_column_type, *self.column_types)
        type_menu.grid(row=2, column=3, sticky="ew", padx=5, pady=5)

        unique_var = tk.BooleanVar()
        unique_checkbox = tk.Checkbutton(self, text="Unique", variable=unique_var)
        unique_checkbox.grid(row=2, column=6, sticky="ew", padx=5, pady=5)

        primary_key_var = tk.BooleanVar()
        primary_key_checkbox = tk.Checkbutton(self, text="Primary Key", variable=primary_key_var)
        primary_key_checkbox.grid(row=2, column=7, sticky="ew", padx=5, pady=5)

        foreign_key_var = tk.BooleanVar()
        foreign_key_checkbox = tk.Checkbutton(self, text="Foreign Key", variable=foreign_key_var,
                                              command=lambda: self.on_foreign_key_change(foreign_key_var, 2))
        foreign_key_checkbox.grid(row=2, column=8, sticky="ew", padx=5, pady=5)

        self.checkbox_vars.append((unique_var, primary_key_var, foreign_key_var))

        self.labels_entries.append((label2, self.entry2, label3, self.selected_column_type))


class DeleteTable(tk.Frame):
    def __init__(self, parent, controller, start_page_instance):
        tk.Frame.__init__(self, parent)

        self.output_field = start_page_instance.output_field

        label1 = tk.Label(self, text="Database Name:")
        label1.pack(pady=10, padx=10)
        self.entry1 = tk.Entry(self)
        self.entry1.pack(pady=10, padx=10)

        delete1 = tk.Button(self, text="Delete", command=lambda: [self.delete(), controller.show_frame("StartPage")])
        delete1.pack()
        button = tk.Button(self, text="Back", command=lambda: controller.show_frame("StartPage"))
        button.pack()

    def update_output_field(self, response):
        self.output_field.configure(state='normal')
        self.output_field.delete("1.0", "end")
        self.output_field.insert(tk.END, response + "\n")
        self.output_field.configure(state='disabled')
        self.output_field.yview(tk.END)

    def delete(self):
        text = "drop table " + self.entry1.get()
        res = send_data_to_server(text)
        self.update_output_field(res)


class CreateIndex(tk.Frame):
    def __init__(self, parent, controller, start_page_instance):
        tk.Frame.__init__(self, parent)

        self.output_field = start_page_instance.output_field

        label2 = tk.Label(self, text="Table Name")
        label2.pack(pady=10, padx=10)

        self.entry2 = tk.Entry(self)
        self.entry2.pack(pady=10, padx=10)

        label1 = tk.Label(self, text="Column Name")
        label1.pack(pady=10, padx=10)

        self.entry1 = tk.Entry(self)
        self.entry1.pack(pady=10, padx=10)

        submit_button = tk.Button(self, text="Submit",
                                  command=lambda: [self.insert(), controller.show_frame("StartPage")])
        submit_button.pack(pady=10, padx=10)

        button = tk.Button(self, text="Back",
                           command=lambda: [self.entry1.delete(0, tk.END), self.entry2.delete(0, tk.END),
                                            controller.show_frame("StartPage")])
        button.pack(pady=10)

    def update_output_field(self, response):
        self.output_field.configure(state='normal')
        self.output_field.delete("1.0", "end")
        self.output_field.insert(tk.END, response + "\n")
        self.output_field.configure(state='disabled')
        self.output_field.yview(tk.END)

    def insert(self):
        text = "Create index " + self.entry2.get() + " " + self.entry1.get()
        res = send_data_to_server(text)
        self.update_output_field(res)
