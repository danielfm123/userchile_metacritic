#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 15 20:09:53 2018

@author: dfischer
"""

import pandas as pd
from sqlalchemy import create_engine
from selenium import webdriver
from webdriverplus import WebDriver
import multiprocessing as mp
import random
import os

server = create_engine('mysql+mysqlconnector://dfischer:Danielfm123@danielfm123.dlinkddns.com/lab',pool_size=5)
juegos = pd.read_sql("""select * from mc_indice
                     where metascore <> 'tbd'""",server)
juegos = [juegos.iloc[i] for i in range(len(juegos))]

    
def get_detail(juego):
    if (str(juego["id"]) in os.listdir("detalles")):
        return(1)
    url = juego["url"] + "/critic-reviews"
    print(str(juego["id"]) + ' ' + url)
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    driver = WebDriver("chrome", reuse_browser=True,options=chrome_options)
    driver.set_page_load_timeout(random.randrange(180, 240,1))
    try:
        driver.get(url)
    except:
        print("reintentando id {} ,url {}".format(str(juego["id"]).url))
        driver.quit()
        return(get_detail(juego))

    Html_file= open("detalles/" + str(juego["id"]),"w")
    Html_file.write(driver.page_source)
    Html_file.close()
    driver.quit()
    return(1)


if "p" in globals():
    pass
else:
    p = mp.Pool(16)
result = p.map(get_detail,juegos)    
    
    
    
    