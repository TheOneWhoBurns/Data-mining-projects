from threading import current_thread
from collections import defaultdict
import os, time, json, re

def combiner(List_combiner_chunks,list_map_name,lock,error):
    """
    Combine the output of the map phase to reduce the volume of data.
    writes the combined result back to a file in the same folder with a 'combined_' prefix.
    """
    
    id = current_thread().name

    if error==True:
            print(f"Reiniciando Hilo {id} Dañado")
            time.sleep(15)
            print(f"Hilo {id} Reparado, Iniciando Procesos")
    chunk_name=""
    while len(list_map_name)>0:
        
        with lock:
            chunk_name=list_map_name.pop(0)
        
        List_combiner_chunks.append("combined_"+chunk_name)
        #Inicia Proceso
        dict = {}
        print(f"Combiner Nodo {id} inicio el proceso del Chunk: {chunk_name}")
        
        #Lectura del archivo chunk
        with open(f"ficheros/mapped_chunks/{chunk_name}", "r", encoding='utf-8') as reader:
            KVCount = reader.read().split("\n")

            for KV in KVCount:
    
                if KV == "":
                    continue
    
                KV_clean = re.sub(r'^<|>$', '', KV)
                KV_split = KV_clean.split(', ')
                key = KV_split[0]
                value = int(KV[KV.rfind(",") + 1: KV.rfind(">")])
    
                if key in dict.keys():
                    dict[key] = dict[key] + value
                else:
                    dict[key] = value
    
        with open(f"ficheros/map_output_folder/combined_{chunk_name}", "a+") as writer:
            for k, v in dict.items():
                writer.write(f"<{k}, {v}>\n")
            #Finaliza Proceso
            print(f"Combiner Nodo {id} finalizo su proceso de: {chunk_name}")

    return List_combiner_chunks
    
    
def reducer(chunk_name,lock,error,name_part):
    
    id = current_thread().name
    
    if error==True:
            print(f"Chunk Name  {id} Dañado")
            print(f"Reiniciando Hilo {chunk_name} ")
            time.sleep(15)
            print(f"Hilo {id} Reparado, Iniciando Procesos")

    #Inicia Proceso
    dict = {}

    with open(f"ficheros/sorted_chunks/{chunk_name}.txt", "r") as reader:
        KVCount = reader.read().split("\n")
        for KV in KVCount:
    
            if KV == "":
                continue
    
            KV_clean = re.sub(r'^<|>$', '', KV)
            KV_split = KV_clean.split(',[')
    
            key = KV_split[0]
    
            value = re.search(r'\[(.*?)\]', KV).group()
    
            try:
                value = json.loads(value)
                sum_value = sum(value)
            except:
                print("ERROR IN SUM", KV_clean)
    
            if key in dict.keys():
                dict[key] = dict[key] + sum_value
            else:
                dict[key] = sum_value
    
    with open(f"ficheros/{name_part}.txt", "a+") as writer:
        for k, v in dict.items():
            writer.write(f"<{k},{v}>\n")
        #Finaliza Proceso
        print(f"Reducer Nodo {id} finalizo su proceso de: {chunk_name}")

    return None
    
  