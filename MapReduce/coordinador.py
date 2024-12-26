
import re #se utilizan para encontrar coincidencias dentro de cadenas de texto
import os #proporciona funciones para interactuar con el sistema operativo
import random, time
import threading 
from threading import current_thread
from map import map_execution
from combiner_reducer import combiner, reducer

nodes_map = 4
nodes_combiner = 2
nodes_reducer = 2

print(f"MAP cores -> {nodes_map} \t"
      f"COMBINER cores -> {nodes_combiner} \t"
      f"REDUCER cores -> {nodes_reducer} \t")

#Funcion para limpiar los caracteres del texto
def clean_file(file):
    
    with open(file, "r", encoding='utf-8') as f:
        #reemplaza con 1 espacio:

        #caracteres no alfanumericos ni numeros
        text = re.sub(r'[^a-zA-Z\s]', ' ', f.read())
        #tabs
        text = re.sub(r'\t', ' ', text)
        #varios espacios
        text = re.sub(r'\s+', ' ', text)
        

    #guardar texto formateado en newLittleWomen.txt
    with open("ficheros/newLittleWomen.txt", "w", encoding='utf-8') as f:
        f.write(text)

#Funcion para dividir el fichero en chunks del mismo tamano
def split_file(clean_file, chunks_folder, num_chunks):
    total_size = os.path.getsize(clean_file)
    chunk_size = total_size // num_chunks

    with open(clean_file, "r", encoding='utf-8') as f:
        chunk_number = 1
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break

            output_file = os.path.join(chunks_folder, f'part_{chunk_number}.txt')
            with open(output_file, "w", encoding='utf-8') as output_f:
                output_f.write(chunk)

            chunk_number += 1

def ejecutar_hilos(lista, error_nodo_map, error_nodo_combiner, error_nodo_reducer, nombre):
    list_map_chunks=[]
    list_comb_chunks=[]
    print(f"{nombre} inicio")
    
    # Crear un lock para sincronizar el acceso a la lista
    lock = threading.Lock()
    if (error_nodo_map==False):
        print(f"Creacion Nodos en {nombre}")

        hilo1 = threading.Thread(target=map_execution, args=(list_map_chunks,lista,lock,False))
        hilo2 = threading.Thread(target=map_execution, args=(list_map_chunks,lista,lock,False))
   
        hilo1.start()
        hilo2.start()
    
        hilo1.join()
        hilo2.join()

    else:
        print(f"Creacion Nodos en {nombre}")
        hilo1 = threading.Thread(target=map_execution, args=(list_map_chunks,lista,lock,True))
        hilo2 = threading.Thread(target=map_execution, args=(list_map_chunks,lista,lock,False))
        print(f"Detener Hilo 1 en {nombre}")

        hilo1.start()
        hilo2.start()
        
        hilo1.join()
        hilo2.join()

        
    print(f"Inicio de Combiner en {nombre}")
    #Inicio de Combiner
    if (error_nodo_combiner==False):
        #crear hilo combiner
        print(f"Creacion Nodos Combiner en {nombre}")

        hilo3 = threading.Thread(target=combiner, args=(list_comb_chunks,list_map_chunks,lock,False))
        
        hilo3.start()
        hilo3.join()

    else:
        #crear hilo combiner
        print(f"Creacion Nodos Combiner en {nombre}")
        hilo3 = threading.Thread(target=combiner, args=(list_comb_chunks,list_map_chunks,lock,True))
        print(f"Detener Hilo 1 en {nombre}")

        hilo3.start()
        hilo3.join()

    print(f"Inicio de Reducer en {nombre}")
    #Inicio de Reduce
    sort("map_output_folder",list_comb_chunks,nombre)
    
    if (error_nodo_reducer==False):
        #crear hilo reduce
        print(f"Creacion Nodos Reducer en {nombre}")
        hilo4 = threading.Thread(target=reducer, args=("sorted_chunk_"+ nombre ,lock,False,"reduced_chunks/reduced_chunks_"+ nombre))
        
        hilo4.start()
        hilo4.join()
        

    else:
        #crear hilo reduce
        print(f"Creacion Nodos Reducer en {nombre}")
        hilo4 = threading.Thread(target=reducer, args=("sorted_chunk_{nombre}",lock,True,"reduced_chunks/reduced_chunks_"+ nombre))
        print(f"Detener Hilo 1 en {nombre}")

        hilo4.start()
        hilo4.join()
    
        
    print(f"{nombre} Finalizo, Esperando a los demas")
    
def sort(carpeta_busqueda,list_chunk_files,name_text):

    dict = {}

    print("Ordenar Combiner chunks")

    for file in list_chunk_files:
        with open(f"ficheros/" + carpeta_busqueda + "/" + file , "r") as reader:
            KVCount = reader.read().split("\n")

            for KV in KVCount:

                if KV == "":
                    continue
                KV_clean=re.sub(r'^<|>$', '', KV)
                KV_split = KV_clean.split(', ')
                key = KV_split[0]
                value = int(KV_split[1])


                if key in dict.keys():
                    try:
                        dict[key].append(value)
                    except:
                        print('value', value)
                else:
                    arr = []
                    arr.append(value)

                    if arr is None:
                        print("a")

                    dict[key] = arr


    sorted_dict = sorted(dict.items())

    with open(f"ficheros/sorted_chunks/sorted_chunk_{name_text}.txt", "a") as writer:
            for k, v in sorted_dict:
                writer.write(f"<{k},{v}>\n")

    print("Termino ordenamiento combined chunks")
    
def merge_files(file1_path, file2_path, output_path):
    # Diccionario para almacenar las sumas de valores para cada clave
    merged_dict = {}

    # Procesar el primer archivo
    with open(file1_path, 'r') as file1:
        for line in file1:
            key, value = line.strip().strip('<>').split(',')
            key = key.strip()
            value = int(value.strip())
            merged_dict[key] = merged_dict.get(key, 0) + value

    # Procesar el segundo archivo
    with open(file2_path, 'r') as file2:
        for line in file2:
            key, value = line.strip().strip('<>').split(',')
            key = key.strip()
            value = int(value.strip())
            merged_dict[key] = merged_dict.get(key, 0) + value

    # Escribir los resultados en el archivo de salida
    with open(output_path, 'w') as output_file:
        for key, value in merged_dict.items():
            output_file.write(f"<{key},{value}>\n")

if __name__ == "__main__":

    # paso 1 Limpieza del archivo

    #Para probar con archivo 1GB
    # filepath = "ficheros/LittleWomen.txt"
    # numChunls = 10000

    #Para probar con archivo corto
    filePath = "ficheros/LittleWomenShortVersion.txt"
    chunksPath = 'ficheros/chunks'
    cleanFilePath = "ficheros/newLittleWomen.txt"
    numChunks = 20
    clean_file(filePath)

    #Inicio de actividad nodo coordinador
    #->Inducir error al nodo Coordinador?
    stateCoordinator = input("Inducir un error al nodo coordinador? S/N: ").upper()

    # Paso 2 Dividir el archivo en chunks mas pequenios

    if stateCoordinator == 'N':
        print(f'DIVISION DE FICHERO: {cleanFilePath} EN CHUNKS')
        split_file(cleanFilePath,chunksPath, numChunks)
        print("Chunks Creados")

        # Paso 3 Creacion de las instancias de map 
    
        print("----- MAPEO -----")
        
        #->Inducir Error al Nodo Map?
        state_map = input('Inducir un error al nodo map? S/N: ').upper()
        
        #lectura de archivos chunks dentro de la carpeta ficheros
        chunks_list = os.listdir("ficheros/chunks")
        chunks_list_size = len(chunks_list)

        #lectura de archivos map
        for f in os.listdir("ficheros/mapped_chunks"):
            os.remove(os.path.join("ficheros/mapped_chunks", f))

        #lectura de archivos combiner
        for f in os.listdir("ficheros/map_output_folder"):
            os.remove(os.path.join("ficheros/map_output_folder", f))

        #lectura de archivos reduce
        for f in os.listdir("ficheros/reduced_chunks"):
            os.remove(os.path.join("ficheros/reduced_chunks", f))

        #lectura de archivos de ordenamiento
        for f in os.listdir("ficheros/sorted_chunks"):
            os.remove(os.path.join("ficheros/sorted_chunks", f))
        
        
        if state_map == 'S':
            #Elegir al azar el nodo a parar
            map_random = random.randint(0, nodes_map)
            
            #Bandera de parar nodo
            if map_random<=1:
                print("El Error ocurrira en el hilo de grupo 1  \t")
                error_map1 = True
                error_map2 = False
            else:
                print("El Error ocurrira en el hilo de grupo 2  \t")
                error_map1 = False
                error_map2 = True

        else:
            error_map1 = False
            error_map2 = False
        
        #-> Inducir Error Nodo Combiner?
        state_combiner = input('Inducir un error al nodo combiner? S/N: ').upper()
        
        if state_combiner == 'S':
            #Elegir al azar el nodo a parar
            combiner_random = random.randint(0, nodes_combiner)
            
            #Bandera de parar nodo
            if combiner_random==0:
                print("El Error ocurrira en el hilo de grupo 1  \t")
                error_comb1 = True
                error_comb2 = False
            else:
                print("El Error ocurrira en el hilo de grupo 2  \t")
                error_comb1 = False
                error_comb2 = True

        else:
            error_comb1 = False
            error_comb2 = False
            
        #-> Inducir Error Nodo Reducer?
        state_reducer = input('Inducir un error al nodo reducer? S/N: ').upper()
        
        if state_reducer == 'S':
            #Elegir al azar el nodo a parar
            reduce_random = random.randint(0, nodes_reducer)
            
            #Bandera de parar nodo
            if reduce_random==0:
                print("El Error ocurrira en el hilo de grupo 1  \t")
                error_red1 = True
                error_red2 = False
            else:
                print("El Error ocurrira en el hilo de grupo 2  \t")
                error_red1 = False
                error_red2 = True

        else:
            error_red1 = False
            error_red2 = False
        
        
        print("Inicio Paralelismo \t")

        #Inicio Nodos 
        hilo_grupo1 = threading.Thread(target=ejecutar_hilos, args=(chunks_list[(int(chunks_list_size/2)):],error_map1,error_comb1,error_red1,"Parte1"))
        hilo_grupo2 = threading.Thread(target=ejecutar_hilos, args=(chunks_list[:(int(chunks_list_size/2))],error_map2,error_comb2,error_red2,"Parte2"))
        
        hilo_grupo1.start()
        hilo_grupo2.start()
        
        hilo_grupo1.join()
        hilo_grupo2.join()
        
        print("Todos los hilos han finalizado.")
        
        #Empezar union
        #Crear archivo final      
        merge_files("ficheros/reduced_chunks/reduced_chunks_Parte1.txt", "ficheros/reduced_chunks/reduced_chunks_Parte2.txt", "ficheros/final/TextoFinal.txt")
        
        print("Finalizo con Exito  \t")
        
    else:
        print('ERROR! SE NECESITA EJECUTAR DE NUEVO EL PROGRAMA')