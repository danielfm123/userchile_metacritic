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
                     from mc_indice left join mc_juegos
                     on mc_indice.id = mc_juegos.id
                     where mc_juegos.id is null
                     and mc_indice.metascore <> 'tbd'
                     #and mc_indice.id = 491""",server)

juegos = [juegos.iloc[i] for i in range(len(juegos))]
#juego = juegos[0]

def get_juego(juego):
    if (not str(juego["id"]) in os.listdir("juegos")):
        return(False)

    url = juego["url"]
    path = "juegos/" + str(juego["id"])
    print(path)
    soup = BeautifulSoup(open(path), 'html.parser')
    
    try:
        publisher = [re.sub("[\s]+"," ",re.sub("\n|\t",'',soup.find("li", {'class' : 'summary_detail publisher'}).find("span",{"class":"data"}).text)).strip()]
    except:
        publisher = [None]
        
    try:
        devel = [re.sub("[\s]+"," ",re.sub("\n|\t",'',soup.find(None, {'class' : 'summary_detail developer'}).find("span",{"class":"data"}).text)).strip()]
    except:
        devel = [None]
        
    try:
        release_date = [soup.find("li", {"class":"summary_detail release_data"}).find("span",{"class":"data"}).text]
    except:
        release_date = [None]
        
    try:
        generes = [x.text for x in soup.find("li",{"class":"summary_detail product_genre"}).findAll("span",{"class":"data"})]
        generes = [";".join(generes)]
    except:
        generes = [None]
        
    try:
        userscore = [soup.find('div', {'class' : 'userscore_wrap feature_userscore'}).find('a',{"class":"metascore_anchor"}).find("div").text]
    except:
        userscore = [None]
    
    try:
        metascore = [soup.find(None, {'itemprop' : 'ratingValue'}).text]
    except:
        metascore = [None]
    
    registro = pd.DataFrame({
            "id" : [juego["id"]],
            "nombre" : [juego["nombre"]],
            "publisher" : publisher,
            "devel" : devel,
            "metascore" : metascore,
            "userscrore" : userscore,
            "metascore_url" : [url + "/critic-reviews"],
            "userscore_url" : [url + "/user-reviews" ],       
            "game_url" : [url],
            "sistema": [juego["sistema"]],
            "release_date": release_date,
            "generes": generes
            })
    query_borrar= "delete from mc_juegos where id ={}".format(juego["id"])
    server.execute(query_borrar)
    registro.to_sql("mc_juegos",server,if_exists='append',index=False)
    #print(registro)
    return(True)



for juego in juegos:
    get_juego(juego)


p = mp.Pool(4)
result = p.map(get_juego,juegos)    
p.close()

#juegos = pd.read_sql("select * from mc_indice where id = 10827",server)
#juego = juegos.iloc[0]
#get_juego(juego)
    
    
    
