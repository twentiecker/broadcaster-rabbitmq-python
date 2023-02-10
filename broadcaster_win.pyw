import tkinter as tk
import pika
import os
from dotenv import load_dotenv


# membuat fungsi connect
def connect():
    credential = pika.PlainCredentials(os.getenv("user"), os.getenv("pass"))
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv("host"),
        port=int(os.getenv("port")),
        virtual_host=os.getenv("vhost"),
        credentials=credential
    ))

    # properti GUI
    txt_pesan.config(state="normal")
    txt_tujuan.config(state="normal")
    btn_connect.config(state="disabled")
    btn_kirim.config(state="active")
    btn_disconnect.config(state="active")
    lbl_status.config(text="Connected!", state="active")
    lbl_pesan.config(state="active")
    lbl_tujuan.config(state="active")
    lbl_list.config(state="active")
    return connection


# membuat fungsi kirim data
def kirim():
    channel = connect().channel()
    channel.exchange_declare(
        exchange=txt_tujuan.get(),  # menentukan nama queue
        exchange_type='fanout'  # param untuk mempertahankan queue meskipun server rabbitMQ berhenti
    )
    channel.basic_publish(
        exchange=txt_tujuan.get(),
        routing_key='',  # nama queue
        body=txt_pesan.get(),  # isi pesan dari queue yang dikirim
    )

    # properti GUI
    txt_list.config(state="normal")
    txt_list.insert(tk.INSERT, f""" [x] "{txt_pesan.get()}" Sent to {txt_tujuan.get()}\n""")
    txt_list.config(state="disabled")
    txt_pesan.delete(0, tk.END)


# membuat fungsi disconnect
def disconnect():
    connect().close()  # menutup koneksi setelah mengirim pesan

    # properti GUI
    txt_pesan.delete(0, tk.END)
    txt_pesan.config(state="disabled")
    txt_pesan.config(state="disabled")
    txt_tujuan.delete(0, tk.END)
    txt_tujuan.config(state="disabled")
    txt_list.config(state="normal")
    txt_list.delete('1.0', tk.END)
    txt_list.config(state="disabled")
    btn_connect.config(state="active")
    btn_kirim.config(state="disabled")
    btn_disconnect.config(state="disabled")
    lbl_status.config(text="Disconnected!", state="disabled")
    lbl_pesan.config(state="disabled")
    lbl_tujuan.config(state="disabled")
    lbl_list.config(state="disabled")


load_dotenv()
list_pesan = []

# membuat window
app = tk.Tk()
app.geometry("425x610")
app.title("Broadcaster")

# # membuat garis
# C = tk.Canvas(height=230, width=300)
# line = C.create_line(10, 170, 290, 170, fill="#a2a39c", width=2)
# C.pack()

# membuat label dan text tujuan
lbl_tujuan = tk.Label(text="Tujuan Broadcast", state="disabled")
lbl_tujuan.place(x=10, y=60)
txt_tujuan = tk.Entry(state="disabled", )
txt_tujuan.place(x=10, y=85)

# membuat label dan text pesan
lbl_pesan = tk.Label(text="Pesan (string)", state="disabled")
lbl_pesan.place(x=150, y=60)
txt_pesan = tk.Entry(state="disabled", width=43)
txt_pesan.place(x=153, y=85)

# membuat label dan list pesan
lbl_list = tk.Label(text="List Pesan Terkirim", state="disabled")
lbl_list.place(x=10, y=195)
txt_list = tk.Text(height=20, width=50, state="disabled")
txt_list.place(x=10, y=220)

# membuat status koneksi
lbl_status = tk.Label(text="Disconnected!", state="disabled")
lbl_status.place(x=70, y=20)

# membuat tombol connect
btn_connect = tk.Button(text="Connect", command=connect)
btn_connect.place(x=10, y=20)

# membuat tombol kirim data
btn_kirim = tk.Button(text="Kirim Data", command=kirim, state="disabled")
btn_kirim.place(x=10, y=125)

# membuat tombol disconnect
btn_disconnect = tk.Button(text="Disconnect", command=disconnect, state="disabled")
btn_disconnect.place(x=10, y=565)

# menjalankan window
app.mainloop()