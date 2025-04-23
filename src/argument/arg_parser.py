import tkinter as tk
from tkinter import filedialog, messagebox

def choose_path():
    path = filedialog.askopenfilename()  # or askdirectory()
    if path:
        print(f"You selected: {path}")
        messagebox.showinfo("Selected Path", path)

# Basic window
root = tk.Tk()
root.title("Path Picker")
root.geometry("300x100")

btn = tk.Button(root, text="Choose a File", command=choose_path)
btn.pack(pady=20)

root.mainloop()
