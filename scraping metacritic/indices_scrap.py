#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 15 14:44:54 2018

@author: dfischer
"""

import pandas as pd
from sqlalchemy import create_engine
from selenium import webdriver
import multiprocessing as mp

#pip3 install MySQL-connector-python 
server = create_engine('mysql+mysqlconnector://dfischer:Danielfm123@danielfm123.dlinkddns.com/lab',pool_size=8)

url_generica = "http://www.metacritic.com/browse/games/release-date/available/{}/date?page={}"
sistemas = ["ps4","xboxone","switch","wii-u","3ds","vita","ps3","ps","xbox","ds","n64","psp","dreamcast","gba","gamecube","wii","xbox360","ps2","pc"]
sistemas = ["ps","xbox","ds","n64","psp","dreamcast","gba","gamecube","wii","xbox360","ps2","pc"]

def get_pagina(parametro):
    
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")  
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(20)
    
    sistema = parametro["sistema"]
    pagina =  parametro["pagina"]
    print(str(pagina))
    url = url_generica.format(sistema,pagina)

    try:
        driver.get(url)
    except:
        print("reintentando pagina {}".format(str(pagina)))
        driver.quit()
        return(get_pagina(pagina))
    #print("sacando indices")
    objects = driver.find_elements_by_xpath("//div[@class='basic_stat product_title']/a")
    urls = [o.get_attribute("href") for o in objects]
    nombres = [o.text for o in objects]
    meta = [x.text for x in driver.find_elements_by_xpath("//div[@class='basic_stat product_score brief_metascore']")]
    user = [x.text for x in driver.find_elements_by_xpath("//li[@class='stat product_avguserscore']/span[2]")]
    
    driver.quit()
    
    tabla = pd.DataFrame({"sistema":sistema, "nombre":nombres, "url": urls, "pagina" : pagina, "metascore": meta, "userscore":user} )    
    #al sql
    #print("al SQL")
    query_borrar= "delete from mc_indice where pagina={} and sistema ='{}'".format(str(pagina),sistema)
    server.execute(query_borrar)
    tabla.to_sql("mc_indice",server,if_exists='append',index=False)
    return(1)

for sistema in sistemas:

    print(sistema)
    
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")  
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(20)

    driver.get(url_generica.format(sistema,"0"))
    try:
        paginas = int(driver.find_element_by_xpath("//li[@class='page last_page']/a").text)
    except:
        paginas = 1
    print("Son {} paginas".format(str(paginas)))
    driver.quit()
    #del driver
    
    parametros = [{"sistema": sistema,"pagina" : p} for p in range(paginas)]
    p = mp.Pool(4)
    try:
        p.map(get_pagina,parametros)
    except:
        pass
    p.close()

