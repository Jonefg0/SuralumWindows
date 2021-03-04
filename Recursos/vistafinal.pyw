from tkinter import *
import pika
import json
import cx_Oracle
import os
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\edgar\Documents\SuralumWindows\Recursos\instantclient-basic-windows.x64-19.10.0.0.0dbru\instantclient_19_10")
connection_ddbb = cx_Oracle.connect('system','erty8040', "localhost")
cursor = connection_ddbb.cursor()

def menu_pantalla():
    global ventanamain
    ventanamain = Tk()
    ventanamain.geometry("500x300")
    ventanamain.title("Bienvenido al Sistema Suralum")
    ventanamain.iconbitmap('Recursos/imagenes/log_1.ico')

    global imagen, imagen2 
    imagen = PhotoImage(file = "Recursos/imagenes/bg.png")
    imagen2 = PhotoImage(file = "Recursos/imagenes/login.png")

    ventanamain.resizable(width=False, height=False)

    Label(ventanamain, image = imagen2).place(x=-2,y=-2)

    Label(ventanamain,text = "Acceso al Sistema", bg='#d6d6d6', font = ("Helvetica",10,"bold")).place(x=290,y=20)
    #Label(ventanamain,text ="").pack()

    Button(ventanamain,text = "Iniciar sesión",height="3",width="30",bg = '#274a99', fg="white",command = inicio_sesion).place(x=240,y=80)
    Button(ventanamain,text = "Registro(Under construction)",height="3",width="30",bg='#d6d6d6').place(x=240, y=130)

    
    ventanamain.mainloop()


def validaciondatos():
        

    
        #print(nombreusuario_verify)
        user = str(nombreusuario_verify)
        #print(user,"dif/n")
        cursor.execute("SELECT password FROM  users WHERE username='"+nombreusuario_verify.get()+"' and password='"+contrasenausuario_verify.get()+"'")
        if (cursor.fetchall()):
            #print("INICIO CORRECTO")
            ventana_principal()
        else:
            #print("ERROR DE CONTRASEÑA")
            global ventana_error
            ventana_error = Toplevel(ventana_sesion)
            ventana_error.title("ERROR")
            ventana_error.geometry("300x100")
            ventana_error.iconbitmap('Recursos/imagenes/log_1.ico')
            Label(ventana_error, text="Usuario y/o Contraseña incorrecta ").pack()
            Button(ventana_error, text="OK", command=ventana_error.destroy).pack()


        

def inicio_sesion():
    ventanamain.withdraw()
    global ventana_sesion
    ventana_sesion = Toplevel(ventanamain)
    ventana_sesion.geometry("500x300")
    ventana_sesion.title("Inicio de sesion")
    ventana_sesion.iconbitmap('Recursos/imagenes/log_1.ico')
    ventana_sesion.resizable(width=False, height=False)

    Label(ventana_sesion, image = imagen2).place(x=-2,y=-2)
    
    Label(ventana_sesion,text = "Ingrese su Usuario y  Contraseña", bg='#d6d6d6', font = ("Helvetica",10,"bold")).place(x=240,y=20)
    #Label(ventana_sesion,text = "").pack()

    global nombreusuario_verify 
    global contrasenausuario_verify

    nombreusuario_verify = StringVar()
    contrasenausuario_verify=StringVar()

    global nombreusuario_entry 
    global constrasenausuario_entry

    Label(ventana_sesion,text = "Usuario", bg='#d6d6d6', font = ("Helvetica",10)).place(x=315,y=70)
    nombreusuario_entry = Entry(ventana_sesion,textvariable = nombreusuario_verify).place(x=280,y=100)
    #nombreusuario_entry.pack()
    #Label(ventana_sesion).pack()

    Label(ventana_sesion,text = "Contraseña", bg='#d6d6d6', font = ("Helvetica",10)).place(x=305,y=130)
    constrasenausuario_entry = Entry(ventana_sesion,textvariable = contrasenausuario_verify, show='*').place(x=280,y=160)
    #constrasenausuario_entry.pack()
    #Label(ventana_sesion).pack()
    Button(ventana_sesion,text ="Iniciar Sesión", bg = '#274a99', fg="white", command = validaciondatos).place(x=301,y=200)




def ventana_principal():
    ventana_sesion.withdraw()
    def generar_reporte():
        seleccionados = [0,0,0,0,0,0,0]
        seleccionados[0] = estadistico_1.get()
        seleccionados[1] = estadistico_2.get()
        seleccionados[2] = estadistico_3.get()
        seleccionados[3] = estadistico_4.get()
        seleccionados[4] = estadistico_5.get()
        seleccionados[5] = estadistico_6.get()
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='estadisticos')
        #print("los estadisticos son: ",seleccionados)
        #print(periodos)
        periodos.sort() 
        #print(periodos)
        msg = (periodos,"#",seleccionados)
        #print("mensaje que se envía es : ", msg)
        channel.basic_publish(exchange='', routing_key='estadisticos', body = json.dumps(msg))
        seleccionados = [0,0,0,0,0,0]
        

    def agregar_periodo():
        #print(periodo.get())
        lista.insert(END,periodo.get())
        periodos.append(periodo.get())


    def borrar_periodo():
        sel = lista.curselection()
        for index in sel[::-1]:
            lista.delete(index)
            periodos.pop(index)
            

    def fin_todo():
        ventana.destroy
        exit()

    #Ventana y variables
    ventana = Toplevel(ventanamain)
    ventana.geometry("800x600")
    ventana.title("Estadisticas Suralum")
    ventana.iconbitmap('Recursos/imagenes/log_1.ico')
    #frame_fechas = Frame(ventana,bg = "black").place(x = 500,y = 200)
    estadistico_1 = IntVar()
    estadistico_2 = IntVar()
    estadistico_3 = IntVar()
    estadistico_4 = IntVar()
    estadistico_5 = IntVar()
    estadistico_6 = IntVar()
    periodo = StringVar()
    periodos = []
    #imagen = PhotoImage(file = "bg.png")
    #Label(ventana,image=imagen).place(x=0,y=0)
    ventana.resizable(width=False, height=False)

    Label(ventana, image = imagen).place(x=-2,y=-2)

    #cabeceratitulo = Label(ventana,text = "Estadisticas Suralum", bg = '#FFF9C2',fg = '#274a99', font = ("Helvetica",24,"bold")).place(x=230,y=80)
    titulo = Label(ventana,text = "Estadisticas Suralum", bg = 'white',fg = "white", font = ("Helvetica",24,"bold")).place(x=230,y=80)
    #descriptores
    titulo_descriptores = Label(ventana,text = "Descriptores", font = ("Helvetica",18,"bold"),bg =  'white').place(x = 450, y = 170)

    vt_button = Checkbutton(ventana,text="Ventas Totales",onvalue = 1,offvalue = 0, variable = estadistico_1, font = ("Helvetica",14),bg = 'white').place(x = 450, y = 210)
    vxf_button = Checkbutton(ventana,text="Ventas por familia", onvalue = 1,offvalue = 0, variable = estadistico_2, font = ("Helvetica",14),bg =  'white').place(x = 450, y = 240)
    vxs_button = Checkbutton(ventana,text="Ventas Suralum", onvalue = 1,offvalue = 0, variable = estadistico_3, font = ("Helvetica",14),bg =  'white').place(x = 450, y = 270)
    vxh_button = Checkbutton(ventana,text="Ventas Huracán",onvalue = 1,offvalue = 0, variable = estadistico_4, font = ("Helvetica",14),bg =  'white').place(x = 450, y = 300)
    vxi_button = Checkbutton(ventana,text="Ventas Industrial",onvalue = 1,offvalue = 0, variable = estadistico_5, font = ("Helvetica",14),bg = 'white').place(x = 450, y = 330)
    pmv_button = Checkbutton(ventana,text="Producto más vendido",onvalue = 1,offvalue = 0, variable = estadistico_6, font = ("Helvetica",14),bg =  'white').place(x = 450, y = 360)


    #ingreso de periodos
    titulo_fechas = Label(ventana,text = "Periodos", font = ("Helvetica",18,"bold"),bg = 'white').place(x = 20, y = 140 )
    cta_fechas = Label(ventana, text = "Ingrese año: ", font = ("Helvetica",14),bg = 'white').place(x = 20, y = 180)
    caja_año = Entry(ventana, textvar = periodo, font = ("Helvetica",14),bg = "white",width = "6").place(x = 140, y = 180)
    f_button = Button(ventana, text ="Agregar",font = ("Helvetica",12),bg = '#274a99', fg="white", command = agregar_periodo).place(x = 220, y = 178)

    lista = Listbox(ventana, width=46)
    lista.pack() 
    lista.place(x = 20, y = 240)
        
    borrar_button = Button(ventana, text = "Borrar periodo", font = ("Helvetica",12),bg = '#274a99', fg="white", width=30, command = borrar_periodo).place(x = 20, y = 420)

    #botón generar reporte
    r_button = Button(ventana, text = "Generar reporte", padx = 10, pady = 10, bg = '#274a99', fg="white", width=13, command = generar_reporte, font = ("Helvetica",14)).place(x = 600, y = 520 )

    #botón cerrar sesión
    c_button = Button(ventana, text ="Cerrar Sesión",font = ("Helvetica",12),bg = '#274a99', fg="white", command=fin_todo).place(x = 580, y = 25)





menu_pantalla()