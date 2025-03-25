import tkinter as tk

from app_files.communication import fetch_column_names, send_data_to_server


class InsertIntoTable(tk.Frame):
    def __init__(self, parent, controller, start_page_instance):
        tk.Frame.__init__(self, parent)
        self.column_name_labels = []
        self.column_entries = []

        self.output_field = start_page_instance.output_field

        self.label1 = tk.Label(self, text="Table name:")
        self.label1.pack(pady=10, padx=10)
        self.entry1 = tk.Entry(self)
        self.entry1.pack(pady=10, padx=10)
        self.submit_button = tk.Button(self, text="Submit",
                                       command=lambda: [self.insert(), controller.show_frame("StartPage")])
        self.fetch_columns_button = tk.Button(self, text="Fetch Columns", command=self.fetch_columns)
        self.back_button = tk.Button(self, text="Back",
                                     command=lambda: [self.reset(), controller.show_frame("StartPage")])

        self.hide_widgets()

        self.fetch_columns_button.pack(pady=10, padx=10)
        self.back_button.pack(pady=10, padx=10)

    def update_output_field(self, response):
        self.output_field.configure(state='normal')
        self.output_field.delete("1.0", "end")
        self.output_field.insert(tk.END, response + "\n")
        self.output_field.configure(state='disabled')
        self.output_field.yview(tk.END)

    def insert(self):
        text = "insert into " + self.entry1.get() + " (" + ",".join(self.column_names) + ") values (" + ",".join(
            entry.get() for entry in self.column_entries) + ")"
        res = send_data_to_server(text)
        self.update_output_field(res)

    def fetch_columns(self):
        table_name = self.entry1.get()
        print(table_name)
        self.column_names = fetch_column_names(table_name)
        self.fetch_columns_button.pack_forget()
        self.column_name_labels = []
        self.column_entries = []

        for column_name in self.column_names:
            label = tk.Label(self, text=column_name + ":")
            label.pack(pady=5, padx=5)
            entry = tk.Entry(self)
            entry.pack(pady=5, padx=5)
            self.column_name_labels.append(label)
            self.column_entries.append(entry)

        self.submit_button.pack(pady=10, padx=10)
        self.back_button.pack(pady=10, padx=10)

    def reset(self):
        for entry in self.column_entries:
            entry.destroy()
        for label in self.column_name_labels:
            label.destroy()
        self.column_name_labels = []
        self.column_entries = []
        self.hide_widgets()

    def hide_widgets(self):
        self.submit_button.pack_forget()
        self.back_button.pack_forget()
        self.fetch_columns_button.pack(pady=10, padx=10)
        self.back_button.pack(padx=10, pady=10)


class DeleteFromTable(tk.Frame):
    def __init__(self, parent, controller, start_page_instance):
        tk.Frame.__init__(self, parent)

        self.output_field = start_page_instance.output_field

        label3 = tk.Label(self, text="Table name:")
        label3.pack(pady=10, padx=10)
        self.entry3 = tk.Entry(self)
        self.entry3.pack(pady=10, padx=10)
        label1 = tk.Label(self, text="ID:")
        label1.pack(pady=10, padx=10)
        self.entry1 = tk.Entry(self)
        self.entry1.pack(pady=10, padx=10)
        submit_button = tk.Button(self, text="Submit",
                                  command=lambda: [self.delete_row(), controller.show_frame("StartPage"), ])
        submit_button.pack(pady=10, padx=10)

        button = tk.Button(self, text="Back",
                           command=lambda: [self.entry1.delete(0, tk.END), controller.show_frame("StartPage"), ])
        button.pack(pady=10, padx=10)

    def update_output_field(self, response):
        self.output_field.configure(state='normal')
        self.output_field.delete("1.0", "end")
        self.output_field.insert(tk.END, response + "\n")
        self.output_field.configure(state='disabled')
        self.output_field.yview(tk.END)

    def delete_row(self):
        table_name = self.entry3.get()
        ID_val = self.entry1.get()
        text = f"delete column {table_name} {ID_val}"
        res = send_data_to_server(text)
        self.update_output_field(res)

class Select(tk.Frame):
    def __init__(self, parent, controller, start_page_instance):
        tk.Frame.__init__(self, parent)
        self.checkboxes = []
        self.column_name_vars = []
        self.column_names = []

        self.output_field = start_page_instance.output_field

        self.table = tk.Label(self, text="Table Name:")
        self.table.pack(pady=5, padx=5)
        self.table_name = tk.Entry(self)
        self.table_name.pack(pady=5, padx=5)

        self.distinct_var = tk.IntVar()
        self.distinct_checkbox = tk.Checkbutton(self, text="DISTINCT", variable=self.distinct_var)

        self.submit_button = tk.Button(self, text="Submit",
                                       command=lambda: [self.insert(), controller.show_frame("StartPage")])
        self.fetch_columns_button = tk.Button(self, text="Fetch Columns", command=self.add_checkboxes)
        self.back_button = tk.Button(self, text="Back",
                                     command=lambda: [self.reset(), controller.show_frame("StartPage")])

        self.fetch_columns_button.pack(pady=10, padx=10)
        self.back_button.pack(pady=10, padx=10)

    def add_checkboxes(self):
        table_name = self.table_name.get()
        self.column_names = fetch_column_names(table_name)
        self.clear_checkboxes()

        for column_name in self.column_names:
            var = tk.IntVar()
            checkbox = tk.Checkbutton(self, text=column_name, variable=var)
            checkbox.pack(pady=5, padx=5)
            self.checkboxes.append(checkbox)
            filter_entry = tk.Entry(self)
            filter_entry.pack(pady=5, padx=5)
            self.column_name_vars.append((column_name, var, filter_entry))

        self.distinct_checkbox.pack(pady=5, padx=5)
        self.group_by_var = tk.IntVar()
        self.group_by_checkbox = tk.Checkbutton(self, text="Group by", variable=self.group_by_var,
                                                 command=self.create_group_by_entry)
        self.group_by_checkbox.pack(pady=5, padx=5)
        self.submit_button.pack(pady=10, padx=10)

    def clear_checkboxes(self):
        for checkbox in self.checkboxes:
            checkbox.destroy()
        for _, _, filter_entry in self.column_name_vars:
            filter_entry.destroy()
        self.checkboxes = []
        self.column_name_vars = []
        self.submit_button.pack_forget()
        self.distinct_checkbox.pack_forget()

    def reset(self):
        self.clear_checkboxes()
        self.table_name.delete(0, tk.END)
        self.distinct_var.set(0)

    def update_output_field(self, response):
        self.output_field.configure(state='normal')
        self.output_field.delete("1.0", "end")
        self.output_field.insert(tk.END, response + "\n")
        self.output_field.configure(state='disabled')
        self.output_field.yview(tk.END)

    def insert(self):
        selected_columns = [col_name for col_name, var, _ in self.column_name_vars if var.get() == 1]
        distinct = "DISTINCT " if self.distinct_var.get() == 1 else ""
        text = f"SELECT {distinct}"
        if selected_columns:
            text += ", ".join(selected_columns)
        else:
            text += "*"

        text += " from " + self.table_name.get()

        filters = []
        for col_name, var, filter_entry in self.column_name_vars:
            if filter_entry.get().strip():
                filters.append(f"{col_name} {filter_entry.get().strip()}")
        if filters:
            text += " where " + " and ".join(filters)
        if self.group_by_var.get():
            text += " group by " + self.group_by_entry.get()
        res = send_data_to_server(text)
        self.update_output_field(res)
        self.reset()

    def create_group_by_entry(self):
        if self.group_by_var.get() == 1:
            self.submit_button.pack_forget()
            group_by_entry = tk.Entry(self)
            group_by_entry.pack(pady=5, padx=5)
            self.group_by_entry = group_by_entry
            self.submit_button.pack(pady=10, padx=10)
