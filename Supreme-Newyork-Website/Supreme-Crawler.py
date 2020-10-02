import requests
import sqlite3
import json 
import pandas as pd 
from pandas.io.json import json_normalize
import os


response = requests.get("http://www.supremenewyork.com/mobile_stock.json")
response_list = json.loads(response.text)

date = response_list["release_date"]
release_week = response_list["release_week"]

Temp_Frame = json_normalize(response_list["products_and_categories"])
Column_Names = Temp_Frame.columns

Main_Frame = json_normalize(Temp_Frame[Column_Names[0]][0])

for names in Column_Names[1:]:
    print(names)
    Frame = json_normalize(Temp_Frame[names][0])
    Main_Frame = Main_Frame.append(Frame)

Main_Frame = Main_Frame.drop(["image_url","image_url_hi"],axis=1)

SUB_MASTER_DATAFRAME = []

All_IDS = Main_Frame["id"].unique()

response = requests.get("https://www.supremenewyork.com/shop/"+str(All_IDS[0])+".json")
response_list = json.loads(response.text)

Temp_Frame = json_normalize(response_list)
Second_Temp_Frame = json_normalize(Temp_Frame["styles"][0])

Third_Temp_Frame = json_normalize(Second_Temp_Frame["sizes"][0])
Third_Temp_Frame.columns = ["name","main_id","stock_level"]

Fourth_Data_Frame = pd.DataFrame(index=range(len(Third_Temp_Frame)),columns=range(len(Second_Temp_Frame.iloc[0])-2))

for row_number in range(len(Second_Temp_Frame.iloc[0])-2):
    Fourth_Data_Frame[row_number] = Second_Temp_Frame.iloc[0][row_number]

Fourth_Data_Frame.columns = list(Second_Temp_Frame.columns)[:-2]

ALL_DATA_FRAME = pd.concat([Third_Temp_Frame, Fourth_Data_Frame], axis=1, sort=False)

for size in range(1,len(Second_Temp_Frame["sizes"])):
    
    Third_Temp_Frame = json_normalize(Second_Temp_Frame["sizes"][size])
    
    Third_Temp_Frame.columns = ["name","main_id","stock_level"]
    
    Fourth_Data_Frame = pd.DataFrame(index=range(len(Third_Temp_Frame)),columns=range(len(Second_Temp_Frame.iloc[size])-2))

    for row_number in range(len(Second_Temp_Frame.iloc[size])-2):
        Fourth_Data_Frame[row_number] = Second_Temp_Frame.iloc[size][row_number]
    
    Fourth_Data_Frame.columns = list(Second_Temp_Frame.columns)[:-2]

    Fourth_Data_Frame = pd.concat([Third_Temp_Frame, Fourth_Data_Frame], axis=1, sort=False)
    
    ALL_DATA_FRAME = ALL_DATA_FRAME.append(Fourth_Data_Frame)
    
ALL_DATA_FRAME = ALL_DATA_FRAME.drop(["description","image_url","swatch_url","mobile_zoomed_url","mobile_zoomed_url_hi","bigger_zoomed_url"],axis=1)

for names in list(Temp_Frame.columns)[1:]:
    ALL_DATA_FRAME[names] = Temp_Frame[names][0]
    
ALL_DATA_FRAME = ALL_DATA_FRAME.reset_index(drop=True)  
ALL_DATA_FRAME["product_id"] = All_IDS[0]
                        
SUB_MASTER_DATAFRAME = ALL_DATA_FRAME


##########################################################
                        
for product_scrape_id in All_IDS[1:]:
    
    try:
        
        response = requests.get("https://www.supremenewyork.com/shop/"+str(product_scrape_id)+".json")
        response_list = json.loads(response.text)
        Temp_Frame = json_normalize(response_list)
        Second_Temp_Frame = json_normalize(Temp_Frame["styles"][0])
        Third_Temp_Frame = json_normalize(Second_Temp_Frame["sizes"][0])
        Third_Temp_Frame.columns = ["name","main_id","stock_level"]
        Fourth_Data_Frame = pd.DataFrame(index=range(len(Third_Temp_Frame)),columns=range(len(Second_Temp_Frame.iloc[0])-2))


        for row_number in range(len(Second_Temp_Frame.iloc[0])-2):
            Fourth_Data_Frame[row_number] = Second_Temp_Frame.iloc[0][row_number]

        Fourth_Data_Frame.columns = list(Second_Temp_Frame.columns)[:-2]

        ALL_DATA_FRAME = pd.concat([Third_Temp_Frame, Fourth_Data_Frame], axis=1, sort=False)


        for size in range(1,len(Second_Temp_Frame["sizes"])):

            Third_Temp_Frame = json_normalize(Second_Temp_Frame["sizes"][size])

            Third_Temp_Frame.columns = ["name","main_id","stock_level"]

            Fourth_Data_Frame = pd.DataFrame(index=range(len(Third_Temp_Frame)),columns=range(len(Second_Temp_Frame.iloc[size])-2))

            for row_number in range(len(Second_Temp_Frame.iloc[size])-2):
                Fourth_Data_Frame[row_number] = Second_Temp_Frame.iloc[size][row_number]

            Fourth_Data_Frame.columns = list(Second_Temp_Frame.columns)[:-2]

            Fourth_Data_Frame = pd.concat([Third_Temp_Frame, Fourth_Data_Frame], axis=1, sort=False)

            ALL_DATA_FRAME = ALL_DATA_FRAME.append(Fourth_Data_Frame)

        ALL_DATA_FRAME = ALL_DATA_FRAME.drop(["description","image_url","swatch_url","mobile_zoomed_url","mobile_zoomed_url_hi","bigger_zoomed_url"],axis=1)

        for names in list(Temp_Frame.columns)[1:]:
            ALL_DATA_FRAME[names] = Temp_Frame[names][0]

        ALL_DATA_FRAME = ALL_DATA_FRAME.reset_index(drop=True)
        ALL_DATA_FRAME["product_id"] = product_scrape_id


        SUB_MASTER_DATAFRAME = SUB_MASTER_DATAFRAME.append(ALL_DATA_FRAME)
        
        print("Success at " + str(product_scrape_id))
        
    except:
        print("Failed at "+str(product_scrape_id))


                        
SUB_MASTER_DATAFRAME.columns = ["size","product_id","stock_level","main_id","color","currency",
                               'image_url_hi', 'swatch_url_hi', 'description', 'can_add_styles',
                               'can_buy_multiple', 'ino', 'cod_blocked', 'canada_blocked',
                               'purchasable_qty', 'new_item', 'apparel', 'handling',
                               'no_free_shipping', 'can_buy_multiple_with_limit',"id"]



Final_Data_Frame = pd.merge(Main_Frame,SUB_MASTER_DATAFRAME, on ="id")

Final_Data_Frame["date"] = date
Final_Data_Frame["release_week"] = release_week



Final_Data_Frame = Final_Data_Frame[[
"date",
"release_week",
"name",
"currency",	
"price",	
"sale_price",
"color",
"size",
"category_name",
"description",	
"new_item_x",
"stock_level",	
"purchasable_qty",	
"new_item_y",	
"id",	
"product_id",
"main_id",		
"image_url_hi",	
"swatch_url_hi",	
"can_add_styles",	
"can_buy_multiple",	
"ino",
"cod_blocked",	
"canada_blocked",	
"apparel",	
"handling",	
"no_free_shipping",
"can_buy_multiple_with_limit",	
"position"

]]


Final_Data_Frame.drop_duplicates(
    subset =list(Final_Data_Frame.columns), 
    keep = "first", inplace = True) 


from datetime import date
today = date.today()

todays_date = today.strftime("%m-%d-%y")

file_name = "Records/Supreme_Products_"+str(todays_date)+".xlsx"

writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
Final_Data_Frame.to_excel(writer, sheet_name='Sheet1',index=False)

writer.save()
