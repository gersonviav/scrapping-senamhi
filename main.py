import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
import re
def stations():
    link = "https://www.senamhi.gob.pe/mapas/mapa-estaciones-2/"
    response = requests.get(link)
    stn_senamhi = BeautifulSoup(response.text, 'html.parser')

    stn_senamhi2 = re.split(r'nom', str(stn_senamhi))[1:]
    stn = []
    cat = []
    lat = []
    lon = []
    ico = []
    cod = []
    cod_old = []
    estado = []
    data_stn = []
# estado:
    for i in range(len(stn_senamhi2)):
        x = stn_senamhi2[i].replace('\"', '').replace(': ', ":").replace(',\n', "").replace('\\}\\{', "")
        data_estaciones = x.split(",")
        print("debug",data_estaciones,len(data_estaciones))
        stn.append(data_estaciones[0].replace(":", ""))
        cat.append(data_estaciones[1].replace("cate:", ""))
        lat.append(data_estaciones[2].replace("lat:", ""))
        lon.append(data_estaciones[3].replace("lon:", ""))
        ico.append(data_estaciones[4].replace(" ico:", ""))
        
        cod.append(data_estaciones[5].replace(" cod:", "") if data_estaciones[5][:5] == " cod:" else None)
        
        cod_old.append(data_estaciones[6].replace("cod_old:", "") if data_estaciones[6][:8] == "cod_old:" else None)
        #[':SAN BORJA', ' cate:EMA', ' lat:-12.10859', ' lon:-77.00769', ' ico:M', ' cod:112193', ' estado:AUTOMATICA}{']
        estado_value = data_estaciones[7] if len(data_estaciones) > 7 else data_estaciones[6]
        estado.append( estado_value.replace("}{","").replace(" estado:", "") if estado_value[:8] == " estado:" else None)

        data_stn.append(pd.DataFrame({
            'estacion': stn[-1],
            'categoria': cat[-1],
            'lat': lat[-1],
            'lon': lon[-1],
            'ico': ico[-1],
            'cod': cod[-1],
            'cod_old': cod_old[-1],
            'estado': estado[-1]
        }, index=[0]))

    df_stns = pd.concat(data_stn, ignore_index=True)
    return df_stns

def senamhiws_ger(x, stations, from_date=None, to_date=None):
    # print("CODIGO",x)
    if not x or not all(isinstance(code, str) for code in x):
        print("Codigo no definido")
        return None

    cod_stn = []
    df_history_senamhi = []

    if from_date is None and to_date is None:
        from_date = datetime(2016, 1, 1)
        to_date = datetime(2023, 12, 31)
    elif from_date is None and to_date is not None:
        from_date = datetime(2016, 1, 1)
    elif from_date is not None and to_date is None:
        to_date = datetime(2023, 12, 31)

    for code in x:
        cod_stn.append(code)
        idx_cod = stations.index[stations['cod'] == code]
        df_idx_stn = stations.loc[idx_cod].reset_index(drop=True)

        ts_date = pd.date_range(from_date, to_date, freq='MS')
        tsw_date = ts_date.strftime('%Y%m')

        for j, date in enumerate(ts_date):
            if pd.isna(df_idx_stn['cod'][0]):
                link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={df_idx_stn['cod'].iloc[0]}&CBOFiltro={tsw_date[j]}&t_e={df_idx_stn['ico'].iloc[0]}&estado={df_idx_stn['estado'].iloc[0]}&cod_old={df_idx_stn['cod_old'].iloc[0]}"
            else:
                link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={df_idx_stn['cod'].iloc[0]}&CBOFiltro={tsw_date[j]}&t_e={df_idx_stn['ico'].iloc[0]}&estado={df_idx_stn['estado'].iloc[0]}"
            
            try:    
                    print(link)
                    data_stn_senamhi = pd.read_html(link)
            # Tu código para manejar los datos después de leerlos correctamente
            except ValueError as e:
                    print(f"Error al leer HTML desde el enlace: {e}")
                    print(f"El enlace que falló es: {link}")

            data_df_history_senamhi = data_stn_senamhi[1]
            data_df_history_senamhi.columns = data_df_history_senamhi.iloc[0]
            data_df_history_senamhi = data_df_history_senamhi[1:]

            df_history_senamhi.append(data_df_history_senamhi)

            output_filename = f"{cod_stn[-1]}_{df_idx_stn['estacion'].iloc[0]}.xlsx"
            data_df_history_senamhi.to_excel(output_filename, index=False)

    return df_history_senamhi

def senamhiws_info(x, stations, from_date=None, to_date=None,lat=None,lon=None,stc=None):
    # print("CODIGO",x)
    print("inputs",lat,lon,stc)
    if not x or not all(isinstance(code, str) for code in x):
        print("Codigo no definido")
        return None

    cod_stn = []
    df_history_senamhi = []

    if from_date is None and to_date is None:
        from_date = datetime(2016, 1, 1)
        to_date = datetime(2023, 12, 31)
    elif from_date is None and to_date is not None:
        from_date = datetime(2016, 1, 1)
    elif from_date is not None and to_date is None:
        to_date = datetime(2023, 12, 31)

    for code in x:
        cod_stn.append(code)
        idx_cod = stations.index[stations['cod'] == code]
        df_idx_stn = stations.loc[idx_cod].reset_index(drop=True)

        ts_date = pd.date_range(from_date, to_date, freq='MS')
        tsw_date = ts_date.strftime('%Y%m')

        for j, date in enumerate(ts_date):
            if pd.isna(df_idx_stn['cod'][0]):
                link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={df_idx_stn['cod'].iloc[0]}&CBOFiltro={tsw_date[j]}&t_e={df_idx_stn['ico'].iloc[0]}&estado={df_idx_stn['estado'].iloc[0]}&cod_old={df_idx_stn['cod_old'].iloc[0]}"
            else:
                link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={df_idx_stn['cod'].iloc[0]}&CBOFiltro={tsw_date[j]}&t_e={df_idx_stn['ico'].iloc[0]}&estado={df_idx_stn['estado'].iloc[0]}"
            
            try:    
                    print(link)
                    data_stn_senamhi = pd.read_html(link)
            # Tu código para manejar los datos después de leerlos correctamente
            except ValueError as e:
                    print(f"Error al leer HTML desde el enlace: {e}")
                    print(f"El enlace que falló es: {link}")

            data_df_history_senamhi = data_stn_senamhi[1]
            data_df_history_senamhi.columns = data_df_history_senamhi.iloc[0]
            data_df_history_senamhi = data_df_history_senamhi[1:]

            df_history_senamhi.append(data_df_history_senamhi)
            data_df_history_senamhi["estacion"] =stc
            data_df_history_senamhi["lat"] =lat
            data_df_history_senamhi["lon"] =lon
            # df_history_senamhi ["ico"] =ico
            output_filename = f"{cod_stn[-1]}_{df_idx_stn['estacion'].iloc[0]}.xlsx"
            data_df_history_senamhi.to_excel(output_filename, index=False)

    return df_history_senamhi

# Uso del código
stations_data = stations()
print(stations_data[stations_data["estacion"]=='CHOSICA'])
#print(stations_data["cod"])
# Suponiendo que "stations_data" es tu DataFrame
#lista_cod = stations_data["cod"].unique().tolist()
#print(lista_cod)
stations_data  = stations_data[stations_data["cod"]== '47278214']
print("aaaaa",stations_data.head())
# Assuming stations_data is your DataFrame
stations_data['lat'] = pd.to_numeric(stations_data['lat'], errors='coerce')
stations_data['lon'] = pd.to_numeric(stations_data['lon'], errors='coerce')

lat =   stations_data['lat'].tolist()[0]
lon =   stations_data['lon'].tolist()[0]
#ico =   stations_data['ico']
stc = stations_data['estacion'].tolist()[0]
codigos = stations_data['cod'].tolist()
print('codigos',codigos)

print("values",lat,lon,stc)
#codigos = lista_cod
try   :
        #resultados = senamhiws_ger(codigos, stations_data,'2024-01-01','2024-02-16')
        resultados =  senamhiws_info(codigos, stations_data,'2024-01-01','2024-02-16',lat,lon,stc)
        #print(resultados)
except Exception as e:
        print(f"An error occurred: {e}")
#https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones=47278214&CBOFiltro=201601&t_e=H&estado=AUTOMATICA"
#https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones=47278214&CBOFiltro=202310&t_e=H&estado=None