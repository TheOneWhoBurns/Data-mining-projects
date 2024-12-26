from threading import current_thread
import time

#Funcion Map
def map_execution(List_map_chunks,list_chunk_name,lock,error):
    #Sacar el nombre del hilo que se esta ejecutando
    id = current_thread().name
    
    if error==True:
            print(f"Reiniciando Hilo {id} Dañado")
            time.sleep(15)
            print(f"Hilo {id} Reparado, Iniciando Procesos")
    chunk_name=""
    while len(list_chunk_name)>0:
        
        with lock:
            chunk_name=list_chunk_name.pop(0)
            List_map_chunks.append("mapped_"+chunk_name)
        
    
        #Inicia Proceso
        print(f"MAP Nodo {id} inicio el proceso del Chunk: {chunk_name}")
        
        #Lectura del archivo chunk
        with open(f"ficheros/chunks/{chunk_name}", "r", encoding='utf-8') as reader:
            with open(f"ficheros/mapped_chunks/mapped_{chunk_name}", "a+", encoding='utf-8') as writer:
                line = reader.readline()
                while line:
                    if isinstance(line, str):
                        words = line.strip().split(" ")
                        for word in words:
                            writer.write(f"<{word}, 1>\n")
                    line = reader.readline()

        #Finaliza Proceso
        print(f"MAP Nodo {id} finalizo su proceso de: {chunk_name}")

    return List_map_chunks

