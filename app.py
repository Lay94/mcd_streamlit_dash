import pandas as pd
import numpy as np

import streamlit as st
import folium
from streamlit_folium import st_folium

import pymysql
from sqlalchemy import create_engine

from shapely import wkt
import geopandas as gpd

myUser = 'usuario1'
myPass = 'Xlk28td31'
myEndpoint = 'dataflow-db.cfkblpqnn1el.us-east-1.rds.amazonaws.com'
myPort = 3304
myDb = 'dataflow'

APP_TITLE = 'Delitos en el estado de Sonora'
link1 = "https://www.inegi.org.mx/"
link2 = "https://www.gob.mx/sesnsp"
APP_SUB_TITLE = f" Fuentes de datos: [INEGI]({link1}), [SESNSP]({link2}) "

def get_query(myUser, myPass, myEndpoint, myPort, myDb, query):
    """Función que realiza una consulta en la base de datos y la devuelve en un dataframe"""       
    try:
        connection = pymysql.connect(
                        user=myUser,
                        password=myPass,
                        host=myEndpoint,
                        port=myPort,
                        database=myDb)
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)            
                
                # Con fetchall traemos todas las filas
                results = cursor.fetchall()
                
                #Guardando los resultados en un dataframe
                df= pd.read_sql_query(query, connection)
        
        finally:
            connection.close()
    except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
        print("Ocurrió un error al conectar: ", e)
    
    return df

@st.cache(allow_output_mutation=True)
def get_data(myUser, myPass, myEndpoint, myPort, myDb):
    df_crimes = get_query(myUser, myPass, myEndpoint, myPort, myDb, "SELECT * FROM delitos_2;")
    df_shape = get_query(myUser, myPass, myEndpoint, myPort, myDb, "SELECT * FROM shapes_son;") 
    df_pop = get_query(myUser, myPass, myEndpoint, myPort, myDb, "SELECT * FROM pob_2;")
    
    return df_crimes, df_shape, df_pop
    

def display_mun_filter(df_shapes, mun_name):
    state_list = [''] + list(df_shapes.NOMBRE.unique())
    state_list.sort()
    state_index = state_list.index(mun_name) if mun_name in state_list else 0
    return st.sidebar.selectbox('Municipios', state_list, state_index)

def display_report_type_filter(dictionary):
    
    selected_item = st.sidebar.selectbox('Bien jurídico afectado', ['Todos los delitos',
                                                           'La vida y la Integridad corporal',
                                                           'Libertad personal',
                                                           'La libertad y la seguridad sexual',
                                                           'El patrimonio',
                                                           'La familia', 
                                                           'La sociedad', 
                                                           'Otros bienes jurídicos afectados'])
    
    
    return dictionary[selected_item]

def display_time_filters(df):
    year_list = list(df['Year'].unique())
    
    year = st.sidebar.slider('Seleccione el año', min_value=int(min(year_list)), max_value=int(max(year_list)), step=1)
    
    return year

def display_facts(df, df_shape, df_pop, year, report_type, mun_name, title, string_format='{:,}', is_perK=False):
    df = df[(df['Year'] == year)]
    
    df_shape_indexed = df_shape.set_index('NOMBRE')
    
    if mun_name:
        state_id = df_shape_indexed['ID'][mun_name]
        df = df[df['ID'] == int(state_id)]
        df_pop = df_pop[df_pop.index == int(state_id)]
    df.drop_duplicates(inplace=True)
    if is_perK:
         total = df[report_type].sum() / df_pop.sum().iloc[0]*100000 if len(df) else 0
                                                                                  
    else:
        total = df[report_type].sum()
    st.metric(title, string_format.format(round(total)))

def to_polygon(str_shapely):
    """Función que convierte str a elemento elegible para geolocalización en formato geojson"""      
    input_string = str_shapely    
    try:
        # Crea un objeto shapely del string de entrada
        shapely_object = wkt.loads(input_string)
    except Exception as e:
        shapely_object = np.nan

    return shapely_object

def clean_geodata(df):
    # Creando nueva columna con el obketo de tipo POLYGON
    df['geometry_2'] = df['geometry'].apply(to_polygon)
    
    # Eliminando columnas innecesarias y valores perdidos
    columns_to_drop = ['geometry']
    df2 = df.drop(columns=columns_to_drop)
    df2 = df2.dropna()
    
    df2.columns = ['ID', 'NOMBRE', 'geometry']
    
    # Convirtiendo dataframe a GeoDataFrame
    gdf = gpd.GeoDataFrame(df2, geometry='geometry', crs="EPSG:4326")

    # Convirtiendo GeoDataFrame a GeoJSON
    #geojson = gdf.to_json()
    
    return gdf

def get_dict_index(value, dict_report_type):
    """Función que busca el índice de un diccionario dado el elemento"""
    for key, val in dict_report_type.items():
        if val == value:
            return key
 
def find_max_smaller(lst, target):
    smaller_values = [x for x in lst if int(x) < target]
    if smaller_values:
        return max(smaller_values)
    else:
        return min(lst)

def row_operation(row, report_type, year):
    return row[report_type] / row[str(year)]*100000
        
def display_map(df, df_shape, df_pop_indexed, year, report_type):
    df = df[(df['Year'] == year)]
    
    map = folium.Map(location=[29.0892, -110.9615], zoom_start=6, scrollWheelZoom=False, tiles='CartoDB positron')
    
    df_indexed = df_shape.set_index('ID')
    df_data_indexed = df.set_index('ID')
    
    
    # Formando datos para visualizar
    #df_data_indexed['NOMBRE'] = df_data_indexed.reset_index()['ID'].apply(lambda x: df_indexed.loc[str(x),]['NOMBRE']).values
    df_new = df_data_indexed.groupby('ID')[[report_type]].sum().reset_index()
    df_new = pd.merge(df_new, df_pop_indexed.reset_index(), on='ID')
    df_new['per_100k'] = df_new.apply(lambda row: row_operation(row, report_type, df_pop_indexed.columns[0]), axis=1)
    
    choropleth = folium.Choropleth(
        geo_data = df_shape.to_json(),
        data=df_new,
        columns=('ID', 'per_100k'),
        key_on='feature.properties.ID',
        #fill_color='YlOrRd',
        fill_opacity=0.7,
        line_opacity=0.7,
        highlight=True,
        legend_name = 'Delitos por cada 100K habitantes'
    )
    choropleth.add_to(map)

    
    for feature in choropleth.geojson.data['features']:
        state_id = feature['properties']['ID']
        feature['properties']['name'] = df_indexed['NOMBRE'][state_id]
        feature['properties']['per_100k'] = 'Delitos c/100K habitantes: ' + str(round(df_data_indexed[report_type][int(state_id)].sum()/df_pop_indexed.loc[int(state_id)].iloc[0]*100000
                                                                                    )) if state_id in list(df_indexed.index) else ''
        feature['properties']['population'] = 'Población: ' + '{:,}'.format(df_pop_indexed.loc[int(state_id)].iloc[0]) if state_id in list(df_indexed.index) else ''
        
    choropleth.geojson.add_child(
        folium.features.GeoJsonTooltip(['name', 'population', 'per_100k'], labels=False)
    )

    st_map = st_folium(map, width=700, height=450)

    state_name = ''
    if st_map['last_active_drawing']:
        state_name = st_map['last_active_drawing']['properties']['name']
    return state_name

def main():
    st.set_page_config(APP_TITLE)
    st.title(APP_TITLE)
    st.caption(APP_SUB_TITLE)
    
    a1, a2, a3 = st.sidebar.columns([20, 40, 20])
    with a1:
        a1.image('mcd_icon.png')
    with a2:
        a2.image('unison_icon.png')
    
    st.sidebar.header('Parámetros')
       
    #### Cargando datos
    df_crimes, df_shape, df_pop = get_data(myUser, myPass, myEndpoint, myPort, myDb)
    
    df_shape = clean_geodata(df_shape)
    df_crimes['TOT']=df_crimes[['DE_PA','DE_FA', 'DE_LS', 'DE_SO', 'DE_VI',	'DE_LP', 'DE_BJ']].sum(axis=1)
    
    # Adicionando columna year
    df_crimes['Year'] = df_crimes['FECHA'].apply(lambda row: row.year) 
   
    dict_report_type = {'Todos los delitos': 'TOT',
              'La vida y la Integridad corporal': 'DE_VI',
              'Libertad personal': 'DE_LP',
              'La libertad y la seguridad sexual': 'DE_LS',
              'El patrimonio': 'DE_PA',
              'La familia': 'DE_FA',
              'La sociedad': 'DE_SO',
              'Otros bienes jurídicos afectados': 'DE_BJ'}
    
    #Visualizando filtros y mapas
    year = display_time_filters(df_crimes)
    df_pop_indexed = df_pop.set_index('ID')    
    df_pop_indexed = df_pop_indexed[[str(year)]] if str(year) in df_pop_indexed.columns else df_pop_indexed[[find_max_smaller(df_pop_indexed.columns[1:].values, year)]]
    
    report_type = display_report_type_filter(dict_report_type)
    state_name = display_map(df_crimes, df_shape, df_pop_indexed, year, report_type)
    state_name = display_mun_filter(df_shape, state_name)
    
    description_report_type = get_dict_index(report_type, dict_report_type)
    
    #Visualizando métricas
    st.subheader(f'KPI: {state_name}   {description_report_type} ')

    col1, col2 = st.columns(2)
    with col1:
        display_facts(df_crimes, df_shape, df_pop_indexed, year, report_type, state_name, 'Total de Delitos')
    with col2:
        display_facts(df_crimes, df_shape, df_pop_indexed, year, report_type, state_name, 'Total de Delitos c/100K habitantes', is_perK=True )

    st.sidebar.markdown("")
    
    st.sidebar.markdown('''
                        ---
                        💻 [Código en github](https://github.com/Lay94/mcd_streamlit_dash/).
                        ''') 

if __name__ == "__main__":
    main()