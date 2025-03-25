import tkinter as tk

from app_files.communication import send_data_to_server


class CreateDatabase(tk.Frame):
    def __init__(self, parent, controller, start_page_instance):
        tk.Frame.__init__(self, parent)

        self.start_page_instance = start_page_instance
        self.output_field = start_page_instance.output_field

        label1 = tk.Label(self, text="Database Name:")
        label1.pack(pady=10, padx=10)
        self.entry1 = tk.Entry(self)
        self.entry1.pack(pady=10, padx=10)

        create = tk.Button(self, text="Submit", command=lambda: [self.submit(), controller.show_frame("StartPage")])
        create.pack()
        button = tk.Button(self, text="Back",
                           command=lambda: [self.entry1.delete(0, tk.END), controller.show_frame("StartPage")])
        button.pack()

    def update_output_field(self, response):
        self.output_field.configure(state='normal')
        self.output_field.delete("1.0", "end")
        self.output_field.insert(tk.END, response + "\n")
        self.output_field.configure(state='disabled')
        self.output_field.yview(tk.END)

    def submit(self):
        text = "create database " + self.entry1.get()
        res = send_data_to_server(text)
        self.update_output_field(res)
        self.start_page_instance.update_folder_treeview()
        return text


class DeleteDatabase(tk.Frame):
    def __init__(self, parent, controller, start_page_instance):
        tk.Frame.__init__(self, parent)

        self.start_page_instance = start_page_instance
        self.output_field = start_page_instance.output_field

        label1 = tk.Label(self, text="Database Name:")
        label1.pack(pady=10, padx=10)
        self.entry1 = tk.Entry(self)
        self.entry1.pack(pady=10, padx=10)

        delete1 = tk.Button(self, text="Delete", command=lambda: [self.delete, controller.show_frame("StartPage")])
        delete1.pack()
        button = tk.Button(self, text="Back",
                           command=lambda: [self.entry1.delete(0, tk.END), controller.show_frame("StartPage")])
        button.pack()

    def update_output_field(self, response):
        self.output_field.configure(state='normal')
        self.output_field.delete("1.0", "end")
        self.output_field.insert(tk.END, response + "\n")
        self.output_field.configure(state='disabled')
        self.output_field.yview(tk.END)

    def delete(self):
        text = "drop database " + self.entry1.get()
        res = send_data_to_server(text)
        self.update_output_field(res)
        self.start_page_instance.update_folder_treeview()
        return text
