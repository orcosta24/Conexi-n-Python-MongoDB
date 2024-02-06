# CONEXION PYHTON MONGODB

import pymongo
import random
import time

MONGO_HOST = "localhost"  # NOMBRE HOST
MONGO_PUERTO = "27017"  # Nº PUERTO
MONGO_TIMEOUT = 1000  # TIEMPO EN MILISEGUNDOS
MONGO_BASEDATOS = "IFP"  # Nombre BDD A LA QUE NOS VAMOS A CONECTAR
MONGO_COLECCION = "Alumnos"  # Nombre DE COLECCION DE LA BDD

# ESTABLEZO DOS ARRAYS CON NOMBRES I APELLIDOS PARA RELLENAR LA BDD
nombres = ["Oriol", "Alejandro", "Valentina", "Gabriel", "Camila", "Sebastián", "Sofía", "Andrés", "Isabella", "Mateo", "Valeria", "Daniel", "Luciana", "Javier", "Martina", "Nicolás", "Gabriela", "Carlos", "Renata", "Felipe", "Ana", "Juan", "Catalina", "Diego", "Juliana", "Francisco", "Laura", "Manuel"]
apellidos = ["García", "Rodríguez", "López", "Martínez", "González", "Pérez", "Sánchez", "Ramírez", "Torres", "Flores", "Gómez", "Díaz", "Vargas", "Castro", "Jiménez", "Ruiz", "Herrera", "Medina", "Morales", "Romero", "Ortiz", "Álvarez", "Mendoza", "Torres", "Ramos", "Cruz", "Acosta", "Ríos"]

MONGO_URL = f"mongodb://{MONGO_HOST}:{MONGO_PUERTO}/"  # URL PARA CONECTARSE AL SERVIDOR

# CONECTARSE A LA BDD DE MONGO DB
try:
    cliente = pymongo.MongoClient(MONGO_URL, serverSelectionTimeoutMS=MONGO_TIMEOUT)
    bdd = cliente[MONGO_BASEDATOS]
    coleccion = bdd[MONGO_COLECCION]
    
# -------------------------------------------------------------------------------------------------------------------------------------------------------
# CREAR Y AÑADIR DATOS A LA BDD

    # CREO UNA VARIABLE DONDE LE AÑADO LOS NOMBRES I APELLIDOS DECLARADOS ANTERIRORMENTE Y UNA NOTA DEL 1 AL 10
    # EN EL BUCLE FOR UTILIZO EL ZIP PARA JUNTAR NOMBRES I APELLIDOS Y ASI SE REPITA LA MISMA VEZ QUE PARES DE NOMBRES I APELLIDOS HAY
    documentos = [{'nombre': nombre, 'apellido': apellido, 'calificacion': random.randrange(0, 10)} for nombre, apellido in zip(nombres, apellidos)]
    
    # INSERTO LOS DOCUMENTOS CREADOS EN COLECCION
    coleccion.insert_many(documentos)
    
    # PONGO UN MENSAJE DE DOCUMENTOS CREADO CON EXITO
    print(f'{len(documentos)} documentos insertados con éxito')
    
    # OBTENEMOS TODOS LOS DOCUMENTOS DE LA COLECCION
    documentos = coleccion.find() 
    
    # CON UN BUCLE PASAMOS POR CADA DOCUMENTO E IMPRIMIMOS SUS VALORES
    for documento in documentos:
        nombre = documento['nombre']
        apellido = documento['apellido' ]
        calificacion = documento['calificacion']
        
        # IMPRIMIMOS LA FRASE CON LOS DATOS DEL CDOCUMENTO EN QUE ESTAMOS SITUADOS
        print(f'{nombre} {apellido} tiene una calificación de {calificacion}')    
    
# -------------------------------------------------------------------------------------------------------------------------------------------------------   
 # MODIFICAR APELLIDOS       
    
    print("Ahora vamos a añadir 'ez' al final de los apellidos")
    
    # PONEMOS UNA ESPERA DE 7 SEGUNDOS
    time.sleep(7)
    
   # CREAMOS UNA NUEVA VARIABLE CON LOS APELLIDOS NUEVOS
    new_apellidos = [apellido + 'ez' for apellido in apellidos]

    # ITERAMOS SOBRE LOS APELLIDOS I LOS ACTUALIZAMOS
    for antiguo_apellido, nuevo_apellido in zip(apellidos, new_apellidos):
        
        # SELECIIONAMOS LOS APELLIDOS ANTIGUOS
        Antiguo_apellido = {'apellido': antiguo_apellido}
        
        # ACTUALIZAMOS LOS APELLIDOS CON $SET
        Nuevo_apellido = {'$set': {'apellido': nuevo_apellido}}
        
        # ACTUALIZAMOS LA BDD CON LOS NUEVOS APELLIDOS
        coleccion.update_many(Antiguo_apellido, Nuevo_apellido)
        
    print("¡Apellidos actualizados!")
    
    # PONEMOS UNA ESPERA DE 3 SEGUNDOS
    time.sleep(3)
    
    # OBTENEMOS TODOS LOS DOCUMENTOS DE LA COLECCION
    documentos = coleccion.find() 
     
    # CON UN BUCLE PASAMOS POR CADA DOCUMENTO E IMPRIMIMOS SUS VALORES PARA COMPROVAR LA ACTUALIZACION
    for documento in documentos:
        nombre = documento['nombre']
        apellido = documento['apellido' ]
        calificacion = documento['calificacion']
        
        # IMPRIMIMOS LA FRASE CON LOS DATOS DEL CDOCUMENTO EN QUE ESTAMOS SITUADOS
        print(f'{nombre} {apellido} tiene una calificación de {calificacion}')
    
# -------------------------------------------------------------------------------------------------------------------------------------------------------
# ELIMINAR MITAD DE USUARIOS
    
    print("Ahora vamos a eliminar la mitad de usuarios")
    
    # PONEMOS UNA ESPERA DE 7 SEGUNDOS
    time.sleep(7)
    
    # VAMOS A HACER UNA AGREGACION PARA SELECCIONAR LA MITAD DE LOS DOCUMENTOS
    pipeline = [
        {"$sample": {"size": 14}} # CON SAMPLE HACEMOS UNA SELECCION DE LO QUE QUEREMOS
    ]

    # EJECUTAMOS LA AGREGACION Y GUARDAMOS LOS USUARIOS A ELIMINAR EN UNA VARAIABLE
    # UTILIZAMOS LIST PARA EJECUTAR LA AGREGACIÓN, QUE ES UNA MODFICACION EL LA BDD DE MONGODB
    docs_eliminar = list(coleccion.aggregate(pipeline))

    # Eliminar los documentos seleccionados
    coleccion.delete_many({"_id": {"$in": [doc["_id"] for doc in docs_eliminar]}})
    
    print("¡Se han eliminado la mitad de usuarios!")
    
    # PONEMOS UNA ESPERA DE 3 SEGUNDOS
    time.sleep(3)
    
    # OBTENEMOS TODOS LOS DOCUMENTOS DE LA COLECCION
    documentos = coleccion.find() 
    
    # CON UN BUCLE PASAMOS POR CADA DOCUMENTO E IMPRIMIMOS SUS VALORES PARA COMPROVAR LA ACTUALIZACION
    for documento in documentos:
        nombre = documento['nombre']
        apellido = documento['apellido' ]
        calificacion = documento['calificacion']
        
        # IMPRIMIMOS LA FRASE CON LOS DATOS DEL CDOCUMENTO EN QUE ESTAMOS SITUADOS
        print(f'{nombre} {apellido} tiene una calificación de {calificacion}')
    
# -------------------------------------------------------------------------------------------------------------------------------------------------------
# ELIMINAR TODOS LOS DOCUMENTOS
    
    # resultado = coleccion.delete_many({})

    # # Imprimir el número de documentos eliminados
    # print(f'{resultado.deleted_count} documentos eliminados con éxito')
    
# -------------------------------------------------------------------------------------------------------------------------------------------------------
# CERRAR CLIENTE DONDE EJECUTAMOS CODIGO
    # cliente.server_info()
    
    cliente.close()

# EXCEPCION POR SI TARDA MUCHO EN CONECTARSE
except pymongo.errors.ServerSelectionTimeoutError as ErrorTiempo:
    print("Tiempo de espera excedido")

# EXCEPCION POR SI FALLA AL CONECTARSE
except pymongo.errors.ConnectionFailure as ErrorConexion:
    print("Fallo al conectar a MongoDB:", ErrorConexion)
