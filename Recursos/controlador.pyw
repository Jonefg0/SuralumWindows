import pika,sys,os
import json
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import cx_Oracle
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib.units import cm
from reportlab.graphics.charts.legends import Legend
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.shapes import Drawing, Rect, String, Group, Line 
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.colors import Color, PCMYKColor
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.validators import Auto
import locale


usr='system'
passw='erty8040'
logotipo = "Recursos/imagenes/logo.png"
head=('Producto','Total vendidos','Total Ventas')
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\edgar\Downloads\instantclient-basic-windows.x64-19.10.0.0.0dbru\instantclient_19_10")
def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host ='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='estadisticos')
    
    def callback(ch,method,properties,body):
        mensaje = json.loads(body)
        #print("se recibe: %r"% mensaje)
        alpdf(str(mensaje))
    
    def alpdf(mensaje):
        locale.getlocale()
        aux = mensaje.split('#')
        periodos = aux[0][2:-4].split(",")
        descriptores = aux [1][4:-2].split(",")
        doc = SimpleDocTemplate("PDFs/SURALUM.pdf", pagesize=letter)
        story = []
        imagen = Image(logotipo, 7 * cm, 3 * cm)
        story.append(imagen)
        story.append(Spacer(10, 20))
        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(name='Center', alignment=TA_CENTER))

        if (int(descriptores[0])):#VENTAS TOTALES
            #print("ventas_totales")
            story.append(Paragraph('Ventas Totales por Año', styles['title']))
            totales = []
            connection_ddbb = cx_Oracle.connect(usr, passw, "localhost")
            cursor = connection_ddbb.cursor()   
            for i in periodos:
                #print()
                cursor = connection_ddbb.cursor()
                cursor.execute("""select SUM(ROUND(ROUND(ROUND(ROUND(vp.cantidad * vp.precio * (CASE WHEN v.OpeId IN (33, 56) THEN 1 ELSE -1 END) * ((100 - v.Descuento1) / 100), 0) * ((100 - v.Descuento2) / 100), 0) * ((100 + v.recuperacion_flete) / 100), 0) * 1.19, 0))  Total  
                from Ventas v, Venta_Productos vp, Productos p
                where v.id_venta = vp.id_venta
                and EXTRACT(YEAR FROM v.fecha) = """+i+"""
                and v.OpeId IN (33, 56, 61)
                and vp.id_producto = p.id_producto
                and v.ParOpeEstado IN (2, 3, 5)""")
                total = 0
                for fname in cursor:
                    total = total+ fname[0]
                    #print ("Values:", fname[1])
                totales.append(total)
            #print(periodos)
            arreglo = [periodos,totales]

            table = Table(arreglo, colWidths=4* cm)
            table.setStyle([('ALIGN', (0, 0), (-1, -1), 'CENTER')])
            for index, row in enumerate(arreglo):
                ini, fin = (0, index), (len(row)-1, index)
                table.setStyle([
                    ("BOX", ini, fin, 0.25, colors.black),
                    ('INNERGRID', ini, fin, 0.25, colors.black),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.gray)
                ])
            story.append(table)
            d = Drawing(300, 200)
            data = [totales]
            bc = VerticalBarChart()
            bc.x = 50
            bc.y = 50
            bc.height = 125
            bc.width = 400
            bc.data = data
            bc.strokeColor = colors.black
            bc.valueAxis.valueMin = 0
            bc.valueAxis.valueMax = 700000000
            bc.valueAxis.valueStep = 100000000  #paso de distancia entre punto y punto
            bc.categoryAxis.labels.boxAnchor = 'ne'
            bc.categoryAxis.labels.dx = 8
            bc.categoryAxis.labels.dy = -2
            bc.categoryAxis.labels.angle = 0
            bc.categoryAxis.categoryNames = periodos
            bc.groupSpacing = 10
            bc.barSpacing = 4
            bc.barLabelFormat = '$%d'
            bc.valueAxis.labelTextFormat = ' $%d '
            bc.barLabels.nudge = 7
            d.add(bc)
            story.append(d)
            #print(story)
   #############################################################################################################################################################
        if (int(descriptores[1][1:])):
            #print("ventas por familia")
            story.append(Paragraph('Ventas por Familia', styles['title']))
            connection_ddbb = cx_Oracle.connect(usr, passw, "localhost")
            cursor = connection_ddbb.cursor()
            totales  = [('Periodo','Suralum','Huracan','Industrial')]
            valores_g = []
            anos=[]
            for i in periodos:
                cursor.execute("""SELECT
                productos.id_familia,
                SUM(venta_productos.cantidad * venta_productos.precio) as c     
                FROM
                    venta_productos INNER JOIN productos ON venta_productos.id_producto = productos.id_producto JOIN ventas ON venta_productos.id_venta=ventas.id_venta
                WHERE EXTRACT(YEAR FROM ventas.fecha) = """+i+"""AND(productos.id_familia=1 OR productos.id_familia=2 OR productos.id_familia=3)
                GROUP BY
                    productos.id_familia
                ORDER BY
                    productos.id_familia DESC
                """)
                vt=[(i)]
                anos.append(i)                
                vg =[]
                for valor in cursor:
                    vt.append(valor[1])
                    vg.append(valor[1])
                totales.append(vt)
                valores_g.append(vg)

            table = Table(totales, colWidths=4*cm)
            table.setStyle([('ALIGN', (0, 0), (-1, -1), 'CENTER')])
            for index, row in enumerate(totales):
                ini, fin = (0, index), (len(row)-1, index)
                table.setStyle([
                    ("BOX", ini, fin, 0.25, colors.black),
                    ('INNERGRID', ini, fin, 0.25, colors.black),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.gray),
                    ('BACKGROUND', (0, 0), (0,-1), colors.gray)
                ])
            story.append(table)
            d = Drawing(600, 200)
            data = valores_g
            bc = VerticalBarChart()
            bc.x = 50
            bc.y = 50
            bc.height = 125
            bc.width = 400
            bc.data = data
            bc.strokeColor = colors.black
            bc.valueAxis.valueMin = 0
            bc.valueAxis.valueMax = 500000000
            bc.valueAxis.valueStep = 100000000 #paso de distancia entre punto y punto
            bc.categoryAxis.labels.boxAnchor = 'ne'
            bc.categoryAxis.labels.dx = 8
            bc.categoryAxis.labels.dy = -2
            bc.categoryAxis.labels.angle = 0
            bc.categoryAxis.categoryNames = ('Suralum','Huracan','Industrial')
            bc.valueAxis.labelTextFormat = ' $%d '
            bc.groupSpacing = 10
            bc.barSpacing = 4
            bc.barLabelFormat = '$%d'
            bc.barLabels.nudge = 7

            legend=Legend()
            legend.x                       = 20
            legend.y                       = 0
            legend.boxAnchor               = 'se'
            legend.subCols[1].align        = 'right'
            legend.alignment               = 'right'
            legend.columnMaximum           = 9
            legend.fontSize                = 13
            # these data can be read from external sources
            legend.colorNamePairs  = [(bc.bars[i].fillColor, anos[i]) for i in range(len(anos))]
            d.add(bc)
            d.add(legend)
            story.append(d)

        

        if (int(descriptores[2][1:])):
            #print("Suralum")
            story.append(Paragraph('Ventas Suralum', styles['title']))
            connection_ddbb = cx_Oracle.connect(usr, passw, "localhost")
            cursor = connection_ddbb.cursor()
            
            for i in periodos:
                #print("para el periodo:",i)
                cursor.execute("""SELECT
                    productos.descripcion,
                    SUM(venta_productos.cantidad) as v,
                    SUM(venta_productos.cantidad * venta_productos.precio) as c
                FROM
                    venta_productos JOIN productos ON venta_productos.id_producto = productos.id_producto JOIN ventas ON venta_productos.id_venta=ventas.id_venta
                WHERE EXTRACT(YEAR FROM ventas.fecha) = """+i+"""AND productos.id_familia=1
                GROUP BY
                    productos.descripcion
                ORDER BY
                    v DESC""")
                #AGREGAR TITULO DEL PERIODO AL STORY PARA SEPARAR LAS TABLAS
                story.append(Paragraph('Año:'+i, styles['Center']))
                k= 0
                totales = []
                totales.append(head)
                for valor in cursor:
                    producto = []
                    if (k < 26):
                        producto.append(valor[0])#nombre
                        producto.append(valor[1])#totales_ccantidad
                        producto.append(valor[2])#totales_ventas
                        totales.append(producto)
                    k = k+1
                table = Table(totales, colWidths=4*cm)
                table.setStyle([('ALIGN', (0, 0), (-1, -1), 'CENTER')])
                for index, row in enumerate(totales):
                    bg_color = colors.green
                    ini, fin = (0, index), (len(row)-1, index)
                    table.setStyle([
                        ("BOX", ini, fin, 0.25, colors.black),
                        ('INNERGRID', ini, fin, 0.25, colors.black),
                        ('BACKGROUND', (0, 0), (-1, 0), colors.gray)
                    ])
                story.append(Spacer(10, 20))
                story.append(table)
                story.append(Spacer(10, 20))

        
############################################################################################################################
        if (int(descriptores[3][1:])):
            #print("Huracan")

            story.append(Paragraph('Ventas Huracan', styles['title']))
            connection_ddbb = cx_Oracle.connect(usr, passw, "localhost")
            cursor = connection_ddbb.cursor()
            
            for i in periodos:
                #print("para el periodo:",i)
                cursor.execute("""SELECT
                    productos.descripcion,
                    SUM(venta_productos.cantidad) as v,
                    SUM(venta_productos.cantidad * venta_productos.precio) as c
                FROM
                    venta_productos JOIN productos ON venta_productos.id_producto = productos.id_producto JOIN ventas ON venta_productos.id_venta=ventas.id_venta
                WHERE EXTRACT(YEAR FROM ventas.fecha) = """+i+"""AND productos.id_familia=2
                GROUP BY
                    productos.descripcion
                ORDER BY
                    v DESC""")
                #AGREGAR TITULO DEL PERIODO AL STORY PARA SEPARAR LAS TABLAS
                story.append(Paragraph('Año:'+i, styles['Center']))
                k= 0
                totales = []
                totales.append(head)
                for valor in cursor:
                    producto = []
                    if (k < 26):
                        producto.append(valor[0])#nombre
                        producto.append(valor[1])#totales_ccantidad
                        producto.append(valor[2])#totales_ventas
                        totales.append(producto)
                        k = k+1


                table = Table(totales, colWidths=4*cm)
                table.setStyle([('ALIGN', (0, 0), (-1, -1), 'CENTER')])
                for index, row in enumerate(totales):
                    bg_color = colors.violet
                    ini, fin = (0, index), (len(row)-1, index)
                    table.setStyle([
                        ("BOX", ini, fin, 0.25, colors.black),
                        ('INNERGRID', ini, fin, 0.25, colors.black),
                        ('BACKGROUND', (0, 0), (-1, 0), colors.gray)
                    ])
                story.append(Spacer(10, 20))
                story.append(table)
                story.append(Spacer(10, 20))
               


#######################################################################################################################################

        if (int(descriptores[4][1:])):
            #print("Industrial")

            story.append(Paragraph('Ventas Industrial', styles['title']))
            connection_ddbb = cx_Oracle.connect(usr, passw, "localhost")
            cursor = connection_ddbb.cursor()
            
            for i in periodos:
                #print("para el periodo:",i)
                cursor.execute("""SELECT
                    productos.descripcion,
                    SUM(venta_productos.cantidad) as v,
                    SUM(venta_productos.cantidad * venta_productos.precio) as c
                FROM
                    venta_productos JOIN productos ON venta_productos.id_producto = productos.id_producto JOIN ventas ON venta_productos.id_venta=ventas.id_venta
                WHERE EXTRACT(YEAR FROM ventas.fecha) = """+i+"""AND productos.id_familia=3
                GROUP BY
                    productos.descripcion
                ORDER BY
                    v DESC""")
                #AGREGAR TITULO DEL PERIODO AL STORY PARA SEPARAR LAS TABLAS
                story.append(Paragraph('Año:'+i, styles['Center']))
                k= 0
                totales = []
                totales.append(head)
                for valor in cursor:
                    producto = []
                    if (k < 26):
                        producto.append(valor[0])#nombre
                        producto.append(valor[1])#totales_ccantidad
                        producto.append(valor[2])#totales_ventas
                        totales.append(producto)
                    k = k+1
                table = Table(totales, colWidths=4*cm)
                table.setStyle([('ALIGN', (0, 0), (-1, -1), 'CENTER')])
                for index, row in enumerate(totales):
                    bg_color = colors.aqua
                    ini, fin = (0, index), (len(row)-1, index)
                    table.setStyle([
                        ("BOX", ini, fin, 0.25, colors.black),
                        ('INNERGRID', ini, fin, 0.25, colors.black),
                        ('BACKGROUND', (0, 0), (-1, 0), colors.gray)
                    ])
                story.append(Spacer(10, 20))
                story.append(table)
                story.append(Spacer(10, 20))
        
        
        if (int(descriptores[5][1:])):
            #print("mas vendido")
            story.append(Paragraph('PRODUCTOS MÁS VENDIDOS', styles['title']))
            connection_ddbb = cx_Oracle.connect(usr, passw, "localhost")
            cursor = connection_ddbb.cursor()
            for i in periodos:
                cursor.execute("""SELECT
                    productos.descripcion,
                    SUM(venta_productos.cantidad) as v,
                    SUM(venta_productos.cantidad * venta_productos.precio) as c
                FROM
                    venta_productos JOIN productos ON venta_productos.id_producto = productos.id_producto JOIN ventas ON venta_productos.id_venta=ventas.id_venta
                WHERE EXTRACT(YEAR FROM ventas.fecha) = """+i+"""
                GROUP BY
                    productos.descripcion
                ORDER BY
                    v DESC"""
                )
                story.append(Paragraph('Año:'+i, styles['Center']))
                k= 0
                totales = []
                totales.append(head)
                for valor in cursor:
                    producto = []
                    if (k < 26):
                        producto.append(valor[0])#nombre
                        producto.append(valor[1])#totales_ccantidad
                        producto.append(valor[2])#totales_ventas
                        #producto.append(valor[3])#familia
                        totales.append(producto)
                    k = k+1
                table = Table(totales, colWidths=4*cm)
                table.setStyle([('ALIGN', (0, 0), (-1, -1), 'CENTER')])
                for index, row in enumerate(totales):
                    bg_color = colors.aqua
                    ini, fin = (0, index), (len(row)-1, index)
                    table.setStyle([
                        ("BOX", ini, fin, 0.25, colors.black),
                        ('INNERGRID', ini, fin, 0.25, colors.black),
                        ('BACKGROUND', (0, 0), (-1, 0), colors.gray)
                    ])
                story.append(Spacer(10, 20))
                story.append(table)
                story.append(Spacer(10, 20))  
        doc.build(story)
        wd = os.getcwd()
        os.popen(wd+'\PDFs\SURALUM.pdf')

    
    channel.basic_consume(queue='estadisticos', on_message_callback = callback, auto_ack = True)
    #print("esperando mensajes: ")
    channel.start_consuming()

if __name__=='__main__':
    try:
        main()
    except KeyboardInterrupt:
        #print('Se interrumpe')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

