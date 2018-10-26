#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 15 20:09:53 2018

@author: dfischer
"""

import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import multiprocessing as mp
import re
import os

server = create_engine('mysql+mysqlconnector://dfischer:Danielfm123@danielfm123.dlinkddns.com/lab',pool_size=32)
juegos = pd.read_sql("""select mc_indice.*
                     from mc_indice left join mc_detail
                     on mc_indice.id = mc_detail.id
                     where mc_detail.id is null
                     and mc_indice.metascore <> 'tbd'
                     #and mc_indice.id = 237""",server)

juegos = [juegos.iloc[i] for i in range(len(juegos))]
#juego = juegos[0]

def get_detail(juego):
    if (not str(juego["id"]) in os.listdir("detalles")):
        return(False)

    path = "detalles/" + str(juego["id"])
    print(path)
    soup = BeautifulSoup(open(path), 'html.parser')
    
    #try:
    source = [x.text for x in soup.findAll("div", {'class' : 'source'})]
    source = [re.sub("[^A-Za-z0-9\s]+","",x) for x in source]
    #except:
    #    source = [None]
        
    #try:
    value = [re.sub("\n","",x.text) for x in soup.findAll("div", {'class' : 'review_grade'})]
    value = [value[x] for x in range(len(source))]
    #except:
    #    value = [None]
        
    
    registro = pd.DataFrame({
            "id" : juego["id"],
            "detail_metascore_value" : value,
            "detail_metascore_source" : source
            })
    query_borrar= "delete from mc_detail where id ={}".format(juego["id"])
    server.execute(query_borrar)
    registro.to_sql("mc_detail",server,if_exists='append',index=False)
    #print(registro)
    return(True)



for juego in juegos:
    get_detail(juego)


p = mp.Pool(4)
result = p.map(get_detail,juegos)    
p.close()

#juegos = pd.read_sql("select * from mc_indice where id = 10827",server)
#juego = juegos.iloc[0]
#get_juego(juego)
    
    
    
