import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import dash
import dash_table
from pandas import DataFrame
import plotly           #(version 4.5.4) pip install plotly==4.5.4
import plotly.express as px
import pyodbc
import sqlalchemy
import base64
import datetime
import io
import datetime as dt 
import numpy as np
import dash_table as dt
import dash_core_components as dcc
import plotly.graph_objs as go
import pandas as pd
from dash_bootstrap_templates import load_figure_template
import plotly.express as px
from dash.exceptions import PreventUpdate
                    
import dash
from dash_table import DataTable



# url = "https://github.com/plotly/datasets/raw/master/" "26k-consumer-complaints.csv"
# df = pd.read_csv(url)

df_valGral = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query1_Tab1_filt.xlsx", sheet_name='Sheet_name_1')
df_valEnf = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query2_Tab1_filt3.xlsx", sheet_name='Sheet_name_1')
df_valPlat = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query3_Tab1_filt.xlsx", sheet_name='Sheet_name_1')
df_valCom = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query4_Tab1_filt.xlsx", sheet_name='Sheet_name_1')
df_valBono = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query5_Tab1_filt.xlsx", sheet_name='Sheet_name_1')


###################################################################################

dddf = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\sessionsUid_4validation2.xlsx", sheet_name='Sheet_name_1')


def get_engine():
    connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'xxxxxx',
                        'koice',
                        'ko_ice',
                        1433)
    res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
    return res
    
def get_query(query):
    engine = get_engine()
    res = pd.read_sql(query, con=engine)
    return res


query3 = """ 

select distinct tt.MarketSegment, tt.Segment, tt.SessionUid, sl.Question, sl.Response, sl.IsProcessed, sl.EvidenceImageURL 
from reportes.sessionsTT_4validation tt 
       join dbo.SurveyList sl on sl.SessionUId = tt.SessionUid 
where sl.Question = '¿La tienda tiene libre acceso a Clientes?'

"""

dddf = pd.DataFrame(get_query(query3))

#df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/solar.csv 47')
available_indicators = dddf['SessionUid'].unique()


###################################################################################################

def get_engine():
    connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'xxxxxx',
                        'koice',
                        'ko_ice',
                        1433)
    res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
    return res
    
def get_query(query):
    engine = get_engine()
    res = pd.read_sql(query, con=engine)
    return res


query = """ 
select distinct tt.MarketSegment, tt.Segment, tt.SessionUid, sl.Question, sl.Response, sl.IsProcessed, sl.EvidenceImageURL 
from reportes.sessionsTT_4validation tt 
       join dbo.SurveyList sl on sl.SessionUId = tt.SessionUid 
where sl.EvidenceImageURL is not NULL and sl.Question = '¿La tienda tiene libre acceso a Clientes?'
"""

xxf = pd.DataFrame(get_query(query))

###################################################################################################


def get_engine():
    connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'xxxxxx',
                        'koice',
                        'ko_ice',
                        1433)
    res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
    return res
    
def get_query(query):
    engine = get_engine()
    res = pd.read_sql(query, con=engine)
    return res


query = """ 
select distinct tt.SessionUid, tt.ice_points, srl.UserName, srl.OutletCode, srl.Status, srl.ClientCode, srl.UserRole, srl.LocationClusterName, srl.VisitStartDateTime, srl.VisitEndDateTime, YEAR(srl.VisitStartDateTime) as year, MONTH(srl.VisitStartDateTime) as month, DAY(srl.VisitStartDateTime) as day, ll.LocationId, ll.Code, ll.Name, ll.BottlerCode, ll.BottlerId, ll.BottlerName, ll.SalesOrganizationName, ll.SalesOfficeName, ll.SalesGroupName, ll.MarketSegment, ll.Segment, ll.LocationLocalTradeChannel 
from dbo.a_tt_10_CMP_r_traditional_data_01 tt 
       join dbo.SceneReportList srl on srl.SessionUid = tt.SessionUid 
       join dbo.LocationList ll on ll.LocationId = tt.LocationId
where YEAR(srl.VisitStartDateTime) = '2021' and srl.LocationClusterName = 'Traditional' and srl.UserRole = 'Auditor' 
"""

yf = pd.DataFrame(get_query(query))
#df = df.sort_values(by="VisitStartDateTime",ascending=False)
#df  

yf['dateInt']=yf['year'].astype(str) + yf['month'].astype(str).str.zfill(2)+ yf['day'].astype(str).str.zfill(2)
yf['fecha'] = pd.to_datetime(yf['dateInt'], format='%Y%m%d')

yf=yf.sort_values(['fecha'])

yf['fecha'] = pd.to_datetime(yf.fecha,errors='coerce')
yf.index = yf['fecha']
#del df['fecha']

validators = ['Oscar García', 'Leslie Reséndiz', 'Noé Velázquez', 'Edgar Retana', 'Areli Carreón', 'Andrea Salazar']

yf['Review']= pd.np.tile(validators, len(yf) // len(validators)).tolist() + validators[:len(yf)%len(validators)]

yf['randNumCol'] = np.random.randint(0,1, size=len(yf))
#kf['random_sample'] = kf.index.isin(kf.sample(frac=0.5).index)
#kf['random_sample'] = False
#kf.loc[kf.sample(frac=0.5).index, 'random_sample'] = True

#kff = kf.sample(frac = 0.2)
#kff.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\df_selected1_2.xlsx", sheet_name='Sheet_name_1', index=False)


###################################################################################################


def get_engine():
    connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'xxxxxx',
                        'koice',
                        'ko_ice',
                        1433)
    res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
    return res
    
def get_query(query):
    engine = get_engine()
    res = pd.read_sql(query, con=engine)
    return res


query2 = """ 

SELECT * FROM a_outlet_sample_validation_session

"""

yf2 = pd.DataFrame(get_query(query2))
yf2=yf2.rename(columns={"SessionUId": "SessionUid"})
yf2['Focus'] = 1

yf2=yf2.drop_duplicates(subset=['SessionUid'])
yf.insert(1, 'Focus', yf['SessionUid'].map(yf2.set_index('SessionUid')['Focus']))
print (yf['Focus'].unique())
print (yf['Focus'].value_counts())


###################################################################################################
jf=yf
####################################################################################################

def get_engine():
    connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'xxxxxx',
                        'koice',
                        'ko_ice',
                        1433)
    res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
    return res
    
def get_query(query):
    engine = get_engine()
    res = pd.read_sql(query, con=engine)
    return res


query = """ 
select distinct tt.SessionUid, tt.ice_points, srl.UserName, srl.OutletCode, srl.Status, srl.ClientCode, srl.UserRole, srl.LocationClusterName, srl.VisitStartDateTime, srl.VisitEndDateTime, YEAR(srl.VisitStartDateTime) as year, MONTH(srl.VisitStartDateTime) as month, DAY(srl.VisitStartDateTime) as day, ll.LocationId, ll.Code, ll.Name, ll.BottlerCode, ll.BottlerId, ll.BottlerName, ll.SalesOrganizationName, ll.SalesOfficeName, ll.SalesGroupName, ll.MarketSegment, ll.Segment, ll.LocationLocalTradeChannel 
from dbo.a_tt_10_CMP_r_traditional_data_01 tt 
       join dbo.SceneReportList srl on srl.SessionUid = tt.SessionUid 
       join dbo.LocationList ll on ll.LocationId = tt.LocationId
where YEAR(srl.VisitStartDateTime) = '2021' and srl.LocationClusterName = 'Traditional' and srl.UserRole = 'Auditor' 
"""

kf = pd.DataFrame(get_query(query))

####################################################################################################

df = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\df_selected1.xlsx", sheet_name='Sheet_name_1')

sessionuid=df['SessionUid']
df['sessionuid']=sessionuid

df['id'] = df['SessionUid']
df.set_index('id', inplace=True, drop=False)

df['Focus'] = df['Focus'].fillna('No')

dff = df.groupby(['Review', 'VisitStartDateTime', 'month', 'day', 'Focus', 'UserName', 'OutletCode', 'UserRole', 'LocationClusterName', 'MarketSegment', 'Segment', 'BottlerName', 'LocationId', 'sessionuid'], as_index=False)[['SessionUid']].count()
dff1 = dff[['Review', 'VisitStartDateTime', 'month', 'day', 'Focus', 'UserName', 'OutletCode', 'UserRole', 'LocationClusterName', 'MarketSegment', 'Segment', 'BottlerName', 'LocationId', 'sessionuid']]
#dff = df.groupby(['Review', 'Focus', 'UserName', 'OutletCode', 'UserRole', 'LocationClusterName', 'MarketSegment', 'Segment', 'BottlerName', 'LocationId', 'SessionUid'], as_index=False)[['ice_points']].sum()
df2 = df.groupby(['MarketSegment', 'Segment', 'LocationId', 'SessionUid'], as_index=False)[['ice_points']].sum()


####################################################################################################

def get_engine():
    connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'xxxxxx',
                        'koice',
                        'ko_ice',
                        1433)
    res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
    return res
    
def get_query(query):
    engine = get_engine()
    res = pd.read_sql(query, con=engine)
    return res


query = """ 
SELECT distinct * FROM [reportes].[sessionsTT_4validation]
"""

zf = pd.DataFrame(get_query(query))
####################################################################################################

def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )

####################################################################################################


def get_engine():
    connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'xxxxxx',
                        'koice',
                        'ko_ice',
                        1433)
    res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
    return res
    
def get_query(query):
    engine = get_engine()
    res = pd.read_sql(query, con=engine)
    return res


query2 = """ 
select distinct tt.MarketSegment, tt.Segment, tt.LocationId, tt.sessionuid, srl.UserName, srl.OutletCode, srl.ImageCaptureTime, srl.SubSceneType, srl.Scene, srl.Status, srl.ImageCount, srl.SessionUid, srl.SceneUId, srl.SceneId, srl.PhotoUrl, srl.VisitStartDate, srl.LocationId, srl.UserRole, srl.LocationClusterName 
from reportes.list_chosen_sessionsUid_tt2 tt 
       join dbo.SceneReportList srl on srl.SessionUid = tt.sessionuid 
where YEAR(srl.VisitStartDateTime) = '2021' and srl.LocationClusterName = 'Traditional' and srl.UserRole = 'Auditor'
"""

pf2 = pd.DataFrame(get_query(query2))

####################################################################################################

def get_engine():
    connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'xxxxxx',
                        'koice',
                        'ko_ice',
                        1433)
    res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
    return res
    
def get_query(query):
    engine = get_engine()
    res = pd.read_sql(query, con=engine)
    return res


query3 = """ 
select distinct tt.MarketSegment, tt.Segment, tt.sessionuid, sl.SurveyName, sl.Question, sl.Response, sl.SurveyQuestionId, sl.IsProcessed, sl.EvidenceImageURL 
from reportes.list_chosen_sessionsUid_tt2 tt 
       join dbo.SurveyList sl on sl.SessionUId = tt.sessionuid 
where sl.EvidenceImageURL is not NULL
"""

pf3 = pd.DataFrame(get_query(query3))


####################################################################################################

PAGE_SIZE = 20


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUMEN])
load_figure_template("bootstrap")

available_users = kf['UserName'].unique()


image_filename = (r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Imágenes\nueva_4.png") # replace with your own image
encoded_image = base64.b64encode(open(image_filename, 'rb').read())


image_filename2 = (r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Imágenes\INICIO.png") # replace with your own image
encoded_image2 = base64.b64encode(open(image_filename2, 'rb').read())


image_filename3 = (r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Imágenes\GENERAL.png") # replace with your own image
encoded_image3 = base64.b64encode(open(image_filename3, 'rb').read())


image_filename4 = (r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Imágenes\ENFRIADOR.png") # replace with your own image
encoded_image4 = base64.b64encode(open(image_filename4, 'rb').read())


image_filename5 = (r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Imágenes\PLATAFORMA.png") # replace with your own image
encoded_image5 = base64.b64encode(open(image_filename5, 'rb').read())


image_filename6 = (r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Imágenes\COMUNICACION.png") # replace with your own image
encoded_image6 = base64.b64encode(open(image_filename6, 'rb').read())


image_filename7 = (r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Imágenes\BONO.png") # replace with your own image
encoded_image7 = base64.b64encode(open(image_filename7, 'rb').read())


image_filename8 = (r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Imágenes\CIERRE.png") # replace with your own image
encoded_image8 = base64.b64encode(open(image_filename8, 'rb').read())


columns = [
    {"id": 0, "name": "SessionUid"},
    {"id": 1, "name": "Focus"},
    {"id": 2, "name": "ice_points"},
    {"id": 3, "name": "UserName"},
    {"id": 4, "name": "OutletCode"},
    {"id": 5, "name": "Status"},
    {"id": 6, "name": "ClientCode"},
    {"id": 7, "name": "UserRole"},
    {"id": 8, "name": "LocationClusterName"},
    {"id": 9, "name": "VisitStartDateTime"},
    {"id": 10, "name": "VisitEndDateTime"},
    {"id": 11, "name": "year"},
    {"id": 12, "name": "month"},
    {"id": 13, "name": "day"},
    {"id": 14, "name": "LocationId"},
    {"id": 15, "name": "Code"},
    {"id": 16, "name": "Name"},
    {"id": 17, "name": "BottlerCode"},
    {"id": 18, "name": "BottlerId"},
    {"id": 19, "name": "BottlerName"},
    {"id": 20, "name": "SalesOrganizationName"},
    {"id": 21, "name": "SalesOfficeName"},
    {"id": 22, "name": "SalesGroupName"},
    {"id": 23, "name": "MarketSegment"},
    {"id": 24, "name": "Segment"},
    {"id": 25, "name": "LocationLocalTradeChannel"},
    {"id": 26, "name": "dateInt"},
    {"id": 27, "name": "fecha"},
    {"id": 28, "name": "Review"},
    {"id": 29, "name": "randNumCol"},
]

app.scripts.config.serve_locally = True

columns_valGral = [
    {"id": 0, "name": "MarketSegment"},
    {"id": 1, "name": "Segment"},
    {"id": 2, "name": "SessionUid"},
    {"id": 3, "name": "Question"},
    {"id": 4, "name": "Response"},
    {"id": 5, "name": "IsProcessed"},
    {"id": 6, "name": "EvidenceImageURL"},
    
]

columns_valEnf = [
    {"id": 0, "name": "SessionUid"},
    {"id": 1, "name": "Question"},
    {"id": 2, "name": "Response"},
    {"id": 3, "name": "IsProcessed"},
    {"id": 6, "name": "EvidenceImageURL"},
    
]


columns_valPlat = [
    {"id": 0, "name": "SessionUid"},
    {"id": 1, "name": "Question"},
    {"id": 2, "name": "Response"},
    {"id": 3, "name": "IsProcessed"},
    {"id": 6, "name": "EvidenceImageURL"},
    
]


columns_valCom = [
    {"id": 0, "name": "SessionUid"},
    {"id": 1, "name": "Question"},
    {"id": 2, "name": "Response"},
    {"id": 3, "name": "IsProcessed"},
    {"id": 6, "name": "EvidenceImageURL"},
    
]


columns_valBono = [
    {"id": 0, "name": "SessionUid"},
    {"id": 1, "name": "Question"},
    {"id": 2, "name": "Response"},
    {"id": 3, "name": "IsProcessed"},
    {"id": 6, "name": "EvidenceImageURL"},
    
]




####################################################################################################

app.layout =  dbc.Container(
    [
        html.H1("Storecheck", className="bg-primary text-white p-3"),
        html.H3("Bienvenido al nuevo sistema de validación ICE: Canal Tradicional", className="bg-primary text-white p-1"),
        html.Div(
    [
        dbc.Tabs(
            [
                dbc.Tab(
                    label="INICIO", tab_id="tab-1",
                    children = [
                    html.Div(
                        id = 'pestanya_datatable3',
                        children = [
                            #html.H3("Bienvenido al nuevo sistema de validación ..."),
                             html.Div([
    html.Img(src='data:image/png;base64,{}'.format(encoded_image.decode()))
]),
    html.Div(id='output-data-upload2'), 
                          ]
                    )
                ]
                    ),
                dbc.Tab(
                    label="BITÁCORA", 
                    tab_id="tab-1-5",
                    
                  #  ),
                   # dcc.Tab(
                id = 'datatable_bit',
                #value = 'datatable',
                #label = 'BITÁCORA',
                children = [
                    html.Div(
                        id = 'pestanya_datatable_bit',
                        children = [
                            #html.H3("Día a validar: 06/03/2021"),
#                             html.Div([
#     dcc.DatePickerRange(
#     id='my-date-picker-range',
# 	min_date_allowed=dt(2021, 1, 1),
#     max_date_allowed=dt(2021, 10, 22),
#     initial_visible_month=dt(2021, 1, 1),
#     end_date=dt(2021, 10, 22)
# ),
# dash_table.DataTable(
#     id='table',
#     columns=[
#         {"name": i, "id": i, "deletable": False, "selectable": False, "hideable": True} for i in kf.columns
#     ],
#     data=kf.to_dict("rows"),
# #    filter_action="native",
#     page_action="native",
#     style_table = {
#                                     'overflowX':'auto'
#                                 },
#              style_data_conditional = [
#                                     {
#                                         'if': {'row_index': 'odd'},
#                                         'backgroundColor': 'rgb(248, 248, 248)'
#                                     },
#                                     {
#                                         'if': {
#                                             'filter_query': '{ice_points} <= 30.0'
#                                         },
#                                         'backgroundColor':'#388587',
#                                         'fontWeight':'italics'
#                                     },
#                                     {
#                                         'if': {
#                                             'filter_query' : '{Focus} = "Yes"'
#                                         },
#                                         'backgroundColor':'#FB9A91'
#                                     },
#                                                                     ],                   
#              style_cell={
#                                 'textAlign' : 'center',
#                                     'width': '80px',
#                                     'minWidth': '160px',
#                                     'maxWidth': '160px',
#                                     'overflow': 'hidden',
#                                     'color': 'black',
#              },
#              style_data = {'backgroundColor':'lavender'},
#                                 style_header = {
#                                     'fontWeight':'bold',
#                                     'overflowX':'scroll',
#                                     'backgroundColor':'#3498DB'
#                                 },
#              fixed_rows={ 'headers': True, 'data': 0 },
#              virtualization=False,
            
#     )
# ]),
                            html.Br(),
                            html.H5("Favor de presionar y leer el manual general de uso del nuevo sistema de validación"),
                        
                            html.Div([
    html.Img(src='data:image/png;base64,{}'.format(encoded_image2.decode())),
    html.Br(),
    html.Br(),

]),
                            html.Div(children=[
                                html.Br(),

                            html.H5(children='Selecciona el usuario para cargar las sesiones a validar:'),
                            dcc.Dropdown(id='dropdown_bit', options=[
                                {'label': i, 'value': i} for i in jf.Review.unique()
                                    ], multi=True, placeholder='Filtrar por usuario'),
                                        html.Div(id='table-container_bit'),
                                        html.Br(),

                                        #html.H5("La siguiente liga te lleva al portal de validación donde podrás capturar y cargar tus resultados:"),
                                        #html.A("Liga al sitio de validación", href='http://127.0.0.1:8077/', target="_blank"),

                                ]

                            )
                        ]
                    )
                ]
            ),    
        dbc.Tab(
                    label="FILTROS", tab_id="tab-2-5",
                    children = [
                    
##################################
html.Div([

    
    html.Div([
        html.Div([
            dcc.Dropdown(id='linedropdown_filt2',
        value='BottlerName', 
        options=[{'value': x, 'label': x} 
                 for x in ['Focus', 'BottlerName', 'MarketSegment', 'Segment']],
        clearable=False
            ),
        ],className='', style={'padding-left':'4%', 'padding-right':'5%','width': '48%', 'display': 'inline-block'}),

        html.Div([
        dcc.Dropdown(id='piedropdown_filt2',
        value='SessionUid', 
        options=[{'value': x, 'label': x} 
                 for x in ['SessionUid']],
        clearable=False

        ),
        ],className='', style={'padding-left':'4%', 'padding-right':'5%','width': '48%', 'display': 'inline-block'}),
        html.H3("Conteo y segmentación de sesiones por validar"),
        dcc.Graph(id="pie-chart_filt2", style={'padding-left':'4%', 'padding-right':'5%', 'marginBottom': 15, 'marginTop': 15}),

        html.Div(
                        id = 'pestanya_estadistica',
                        children = [
                            
                            html.Div(id='row1', children=[
                                
                            ], className = 'row'),
                            html.Div(id = 'row3', children = [
                                html.Div(
                                    id = 'div-rangeslider',
                                    children = [
                                    
                    html.Div([
                    html.Label('Rango fechas'),
                    dcc.DatePickerRange(id='selector_fecha_filt2',start_date=kf["VisitStartDateTime"].min(),end_date=kf["VisitEndDateTime"].max())
                    ],style={'padding-left':'3%', 'padding-right':'3%', 'width': '35%', 'float': 'right', 'display': 'inline-block'}),

                                       html.Div([
                    html.Label('Embotelladora'),
                    dcc.Dropdown(id='selector_filt2',
                        options=[{'label': i, 'value': i} for i in kf['BottlerName'].unique()],
                        value='Coca-Cola FEMSA S.A.B. de C.V.'
                    )],style={'padding-left':'3%', 'padding-right':'3%', 'width': '35%', 'display': 'inline-block'}),

                    html.H3("Metricas ICE canal Tradicional"),

                    html.Div([
                    dcc.Graph(id='barplot_ventas_seg_filt2')
                    ],style={'padding-right':'1%', 'width': '50%', 'float': 'left', 'display': 'inline-block'}),

                    html.Div([
                    dcc.Graph(id='barplot_beneficio_cat_filt2')
                    ],style={'width': '50%', 'float': 'center', 'display': 'inline-block'}),
                    
                    html.Div([
    html.Div([
    dcc.Graph(id='clientside-graph_filt2')
    ],style={'width': '100%', 'float': 'center', 'display': 'inline-block'}),
    dcc.Store(
        id='clientside-figure-store_filt2',
        data=[{
            'x': kf[kf['ice_points'] == '']['VisitStartDateTime'],
            'y': kf[kf['ice_points'] == '']['ice_points']
        }]
    ),
    'Studio Indicator',
    html.Div([
    dcc.Dropdown(
        id='clientside-graph-indicator_filt2',
        options=[
            {'label': 'ICE_TT', 'value': 'ice_points'}
        ],
        value='ice_points'
    )],style={'padding-left':'3%', 'padding-right':'3%', 'width': '35%', 'display': 'inline-block'}),
    'Username',
    html.Div([
    dcc.Dropdown(
        id='clientside-graph-country_filt2',
        options=[
            {'label': UserName, 'value': UserName}
            for UserName in available_users
        ],
        value=''
    )],style={'padding-left':'3%', 'padding-right':'3%', 'width': '35%', 'display': 'inline-block'}),
            
]) 
               ], className = 'twelve columns', style = {'padding-left':'4%', 'padding-right':'5%'}
                                ),
                            ], className = 'row'),
                        ]
                    ),

    ]),
    html.Br(),
    html.Button(
        ['Update'],
        id='btn_filt2'
    ),
    dash_table.DataTable(
        id='table_filt2',
        data=[]
    ),
    html.Br(),
    html.Button(
        ['Filtering'],
        id='btn2_filt2'
    ),
    html.Div(id='datatable-paging_filt2'),
    dash_table.DataTable(
        id='table2_filt2',
        data=[],
        #id='datatable_id',
            #data=dff.to_dict('records'),
            columns=columns,
            editable=False,
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            row_selectable="multi",
            row_deletable=False,
            selected_rows=[],
            page_action="native",
            #page_current= 0,
            #page_size= 6,
             #page_action='none',
             style_table = {
                                    'overflowX':'auto'
                                },
             style_data_conditional = [
                                    {
                                        'if': {'row_index': 'odd'},
                                        'backgroundColor': 'rgb(248, 248, 248)'
                                    },
                                    {
                                        'if': {
                                            'filter_query': '{ice_points} <= 30.0'
                                        },
                                        'backgroundColor':'#388587',
                                        'fontWeight':'italics'
                                    },
                                    {
                                        'if': {
                                            'filter_query' : '{Focus} = "Yes"'
                                        },
                                        'backgroundColor':'#FB9A91'
                                    },
                                                                    ],                   
             style_cell={
                                'textAlign' : 'center',
                                    'width': '80px',
                                    'minWidth': '160px',
                                    'maxWidth': '160px',
                                    'overflow': 'hidden',
                                    'color': 'black',
             },
             style_data = {'backgroundColor':'lavender', 'width': '150px', 'minWidth': '150px', 'maxWidth': '150px',
        'overflow': 'hidden',
        'textOverflow': 'ellipsis',},
                                style_header = {
                                    'fontWeight':'bold',
                                    'overflowX':'scroll',
                                    'backgroundColor':'#3498DB'
                                },
             fixed_rows={ 'headers': True, 'data': 0 },
             virtualization=False,
            style_cell_conditional=[
                {'if': {'column_id': 'SessionUid'},
                 'width': '40%', 'textAlign': 'left'},
                {'if': {'column_id': 'ice_points'},
                 'width': '30%', 'textAlign': 'left'},
            ],
    ),
    
])

##################################

                ]
                    ),

#                 dbc.Tab(
#                     label="VALIDACIÓN GENERAL", tab_id="tab-0", 
#                     children = [
#                         html.Div([

#     html.Div([
#     dcc.Upload(
#         id='upload-data',
#         children=html.Div([
#             'Drag and Drop or ',
#             html.A('Select Files')
#         ]),
#         style={
#             'width': '100%',
#             'height': '60px',
#             'lineHeight': '60px',
#             'borderWidth': '1px',
#             'borderStyle': 'dashed',
#             'borderRadius': '5px',
#             'textAlign': 'center',
#             'margin': '10px'
#         },
#         # Allow multiple files to be uploaded
#         multiple=True
#     ),
#     html.Div(id='output-data-upload'),
    
#     #html.Button("Custom export", id="export_table"),
# ]),                        

#    #########
#    #########
#    #########

# ])
#                     ]
#                     ),
                    dbc.Tab(
                    label="VALIDACIÓN GENERAL", tab_id="tab-05", 
                    children = [
                        html.Div([

    html.Div([
    dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Espacio para cargar el archivo con las sesiones seleccionadas',
           # html.A('Select Files')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        # Allow multiple files to be uploaded
        multiple=True
    ),
    html.Div(id='output-data-upload'),
    
    #html.Button("Custom export", id="export_table"),
]),

    html.H4('Presiona los elementos disponibles para solicitar información'),

    html.Div([
    html.Button(
        ['Exterior de la tienda'],
        id='btn_valGral'
    ),
    DataTable(
        id='table_valGral',
        data=[]
    )
]),
html.Br(),
html.Img(src='data:image/png;base64,{}'.format(encoded_image3.decode())),
    html.Br(),
   
])
                    ]
                    ),
             dbc.Tab(
                    label="ENFRIADOR", tab_id="tab-4",
                    children = [
                  html.Div([

    html.Div([
    dcc.Upload(
        id='upload-data-enfriador',
        children=html.Div([
            'Espacio para cargar y validar las sesiones seleccionadas',
           # html.A('Select Files')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        # Allow multiple files to be uploaded
        multiple=True
    ),
    html.Div(id='output-data-upload-enfriador'),
    
    #html.Button("Custom export", id="export_table"),
]),

    html.H4('Presiona los elementos disponibles para solicitar información'),

    html.Div([
    html.Button(
        ['Lllenado al 75%'],
        id='btn_valEnf'
    ),
    DataTable(
        id='table_valEf',
        data=[]
    )
]),
html.Br(),
   html.Img(src='data:image/png;base64,{}'.format(encoded_image4.decode())),
])
                  
                ]
                    ),    
            dbc.Tab(
                    label="PLATAFORMA", tab_id="tab-5",
                    children = [

                         html.Div([
    html.Div([
    dcc.Upload(
        id='upload-data-plataforma',
        children=html.Div([
            'Espacio para cargar y validar las sesiones seleccionadas',
           # html.A('Select Files')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        # Allow multiple files to be uploaded
        multiple=True
    ),
    html.Div(id='output-data-upload-plataforma'),
        #html.Button("Custom export", id="export_table"),
]),
    html.H4('Presiona los elementos disponibles para solicitar información'),

    html.Div([
    html.Button(
        ['Tipo de plataforma'],
        id='btn_valPlat'
    ),
    DataTable(
        id='table_valPlat',
        data=[]
    )
]),
html.Br(),
   html.Img(src='data:image/png;base64,{}'.format(encoded_image5.decode())),
])  

                ]
                    ),
            dbc.Tab(
                    label="COMUNICACIÓN", tab_id="tab-6",
                    children = [
                  
                      html.Div([

    html.Div([
    dcc.Upload(
        id='upload-data-com',
        children=html.Div([
            'Espacio para cargar el archivo con las sesiones seleccionadas',
           # html.A('Select Files')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        # Allow multiple files to be uploaded
        multiple=True
    ),
    html.Div(id='output-data-upload-com'),
    
    #html.Button("Custom export", id="export_table"),
]),

    html.H4('Presiona los elementos disponibles para solicitar información'),

    html.Div([
    html.Button(
        ['Comunicación'],
        id='btn_valCom'
    ),
    DataTable(
        id='table_valCom',
        data=[]
    )
]),
html.Br(),
   html.Img(src='data:image/png;base64,{}'.format(encoded_image6.decode())),
])  

                ]
                    ),
            dbc.Tab(
                    label="BONO", tab_id="tab-7",
                    children = [
                   
                        html.Div([

    html.Div([
    dcc.Upload(
        id='upload-data-bono',
        children=html.Div([
            'Espacio para cargar el archivo con las sesiones seleccionadas',
           # html.A('Select Files')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        # Allow multiple files to be uploaded
        multiple=True
    ),
    html.Div(id='output-data-upload-bono'),
    
    #html.Button("Custom export", id="export_table"),
]),

    html.H4('Presiona los elementos disponibles para solicitar información'),

    html.Div([
    html.Button(
        ['Exterior de la tienda'],
        id='btn_valBono'
    ),
    DataTable(
        id='table_valBono',
        data=[]
    )
]),
html.Br(),
   html.Img(src='data:image/png;base64,{}'.format(encoded_image7.decode()))
])  


                ]
                    ),
            dbc.Tab(
                    label="CARGA DE DATOS Y CIERRE DE SESIÓN", tab_id="tab-8",
                    children = [

                        html.Div([
        html.Br(),
        dcc.Upload(
            id='upload_fin',
            children=html.Button('Ingresar los resultados de la Validación General'),
              ),
        html.Div(id="output_data_fin"),

html.Br(),

        dcc.Upload(
            id='upload_fin1',
            children=html.Button('Ingresar los resultados del Enfriador'),
              ),
        html.Div(id="output_data_fin1"),


html.Br(),

        dcc.Upload(
            id='upload_fin2',
            children=html.Button('Ingresar los resultados de Plataforma'),
              ),
        html.Div(id="output_data_fin2"),


html.Br(),
        dcc.Upload(
            id='upload_fin3',
            children=html.Button('Ingresar los resultados de Comunicación'),
              ),
        html.Div(id="output_data_fin3"),


html.Br(),
        dcc.Upload(
            id='upload_fin4',
            children=html.Button('Ingresar los resultados de Bono'),
              ),
        html.Div(id="output_data_fin4"),


html.Br(),

        ]),
                    html.Br(),
                    html.Img(src='data:image/png;base64,{}'.format(encoded_image8.decode())),

                    ]
                    ),
            ],
            id="tabs",
            active_tab="tab-1",
        ),
        html.Div(id="content"),
    ]
),
],
    fluid=True,
)

###################################################################################################
############################################################################################

def parse_contents(contents, filename, date):
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            # Assume that the user uploaded a CSV file
            df = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded))
    except Exception as e:
        print(e)
        return html.Div([
            'There was an error processing this file.'
        ])

    return html.Div([
        #html.H5(filename),
        html.H6(datetime.datetime.fromtimestamp(date)),

        dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns],
            editable=True, 
            row_deletable=True,
            export_format="xlsx",
            export_headers="display",
            fixed_rows = {'headers':True},
                                style_table = {
                                    'overflowX':'auto'
                                },
                                style_cell={
                                'textAlign' : 'center',
                                    'width': '80px',
                                    'minWidth': '160px',
                                    'maxWidth': '160px',
                                    'overflow': 'hidden',
                                    'color': 'black',
             }, 
             style_data = {'backgroundColor':'lavender'},
                                style_header = {
                                    'fontWeight':'bold',
                                    'overflowX':'scroll',
                                    'backgroundColor':'#3498DB'
                                },




        ),
        df.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\ValoresGrals2.xlsx", sheet_name='Sheet_name_1', index=False),

        html.Hr(),  # horizontal line

        
    ])

####################################################################################################
####################################################################################################
# validación General

def parse_data0(contents, filename):
    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)
    try:
        if "csv" in filename: #csv file
            df = pd.read_csv(io.StringIO(decoded.decode("utf-8")), delimiter=',', decimal=','),
            print(type(df)),
        elif "xls" in filename: #xls file
            df = pd.read_excel(io.BytesIO(decoded))
    except Exception as e:
        print(e)
        return html.Div(["There was an error processing this file."])
    print(df)

    #####

    print("flags-tt - Borrando los datos de la tabla Val. Gral.")
    query_clear_tt = """ 
    
    DELETE FROM reportes.ValidacionGeneral_2021

        """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()
#get_query(query_clear_Exh_as_cc_comp)
#####

    print("flags-as - Llenando la tabla de Vla. Gral.")

    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res


    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    
    engine = get_engine()
    df.to_sql('ValidacionGeneral_2021', con=engine, schema='reportes', if_exists='append', index=False)
        #print(df_line)



    return df

####################################################################################################
# Enfriador

def parse_data1(contents, filename):
    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)
    try:
        if "csv" in filename: #csv file
            df1 = pd.read_csv(io.StringIO(decoded.decode("utf-8")), delimiter=',', decimal=','),
            print(type(df1)),
        elif "xls" in filename: #xls file
            df1 = pd.read_excel(io.BytesIO(decoded))
    except Exception as e:
        print(e)
        return html.Div(["There was an error processing this file."])
    print(df1)

#####

    print("flags-tt - Borrando los datos de la tabla Enfriador")
    query_clear_tt = """ 
    
    DELETE FROM reportes.EnfriadorVal_2021

        """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()
#get_query(query_clear_Exh_as_cc_comp)
#####

    print("flags-as - Llenando la tabla Enfriador")

    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res


    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    
    engine = get_engine()
    df1.to_sql('EnfriadorVal_2021', con=engine, schema='reportes', if_exists='append', index=False)
        #print(df_line)



    return df1

####################################################################################################
# Plataforma

def parse_data2(contents, filename):
    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)
    try:
        if "csv" in filename: #csv file
            df2 = pd.read_csv(io.StringIO(decoded.decode("utf-8")), delimiter=',', decimal=','),
            print(type(df2)),
        elif "xls" in filename: #xls file
            df2 = pd.read_excel(io.BytesIO(decoded))
    except Exception as e:
        print(e)
        return html.Div(["There was an error processing this file."])
    print(df2)

#####

    print("flags-tt - Borrando los datos de la tabla Plataforma")
    query_clear_tt = """ 
    
    DELETE FROM reportes.PlataformaVal_2021

        """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()
#get_query(query_clear_Exh_as_cc_comp)
#####

    print("flags-as - Llenando la tabla Plataforma")

    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res


    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    
    engine = get_engine()
    df2.to_sql('PlataformaVal_2021', con=engine, schema='reportes', if_exists='append', index=False)
        #print(df_line)



    return df2

####################################################################################################
# Comunicación

def parse_data3(contents, filename):
    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)
    try:
        if "csv" in filename: #csv file
            df3 = pd.read_csv(io.StringIO(decoded.decode("utf-8")), delimiter=',', decimal=','),
            print(type(df3)),
        elif "xls" in filename: #xls file
            df3 = pd.read_excel(io.BytesIO(decoded))
    except Exception as e:
        print(e)
        return html.Div(["There was an error processing this file."])
    print(df3)

#####

    print("flags-tt - Borrando los datos de la tabla Comunicación")
    query_clear_tt = """ 
    
    DELETE FROM reportes.ComunicaciónVal_2021

        """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()
#get_query(query_clear_Exh_as_cc_comp)
#####

    print("flags-as - Llenando la tabla Comunicación")

    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res


    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    
    engine = get_engine()
    df3.to_sql('ComunicaciónVal_2021', con=engine, schema='reportes', if_exists='append', index=False)
        #print(df_line)



    return df3


####################################################################################################
####################################################################################################
# Bono

def parse_data4(contents, filename):
    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)
    try:
        if "csv" in filename: #csv file
            df4 = pd.read_csv(io.StringIO(decoded.decode("utf-8")), delimiter=',', decimal=','),
            print(type(df4)),
        elif "xls" in filename: #xls file
            df4 = pd.read_excel(io.BytesIO(decoded))
    except Exception as e:
        print(e)
        return html.Div(["There was an error processing this file."])
    print(df4)

#####

    print("flags-tt - Borrando los datos de la tabla Bono")
    query_clear_tt = """ 
    
    DELETE FROM reportes.BonoVal_2021

        """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()
#get_query(query_clear_Exh_as_cc_comp)
#####

    print("flags-as - Llenando la tabla de Bono")

    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res


    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    
    engine = get_engine()
    df4.to_sql('BonoVal_2021', con=engine, schema='reportes', if_exists='append', index=False)
        #print(df_line)

    return df4

####################################################################################################
############################################################################################

@app.callback(
     Output("output_data_fin", "children"),
     Input("upload_fin", "contents"),
     Input("upload_fin", "filename"),
)
def update_table(contents, filename):
    # table = html.Div()
    if contents:
        df=parse_data0(contents,filename)
        table = html.Div(
            [
                #html.H4(filename),
                dash_table.DataTable(
                    columns=[{"name": i, "id": i} for i in df.columns],
                    data=df.to_dict("records"),
                    editable=True

                ),
                html.Hr(),
                html.Div("Raw Content"),
                html.Pre(
                    contents[0:200] + "...",
                    style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"},
                ),
            ]
        )
        return table

            

@app.callback(
     Output("output_data_fin1", "children"),
     Input("upload_fin1", "contents"),
     Input("upload_fin1", "filename"),
)
def update_table(contents, filename):
    # table = html.Div()
    if contents:
        df1=parse_data1(contents,filename)
        table = html.Div(
            [
                #html.H4(filename),
                dash_table.DataTable(
                    columns=[{"name": i, "id": i} for i in df1.columns],
                    data=df1.to_dict("records"),
                    editable=True

                ),
                html.Hr(),
                html.Div("Raw Content"),
                html.Pre(
                    contents[0:200] + "...",
                    style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"},
                ),
            ]
        )
        return table


@app.callback(
     Output("output_data_fin2", "children"),
     Input("upload_fin2", "contents"),
     Input("upload_fin2", "filename"),
)
def update_table(contents, filename):
    # table = html.Div()
    if contents:
        df2=parse_data2(contents,filename)
        table = html.Div(
            [
                #html.H4(filename),
                dash_table.DataTable(
                    columns=[{"name": i, "id": i} for i in df2.columns],
                    data=df2.to_dict("records"),
                    editable=True

                ),
                html.Hr(),
                html.Div("Raw Content"),
                html.Pre(
                    contents[0:200] + "...",
                    style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"},
                ),
            ]
        )
        return table



@app.callback(
     Output("output_data_fin3", "children"),
     Input("upload_fin3", "contents"),
     Input("upload_fin3", "filename"),
)
def update_table(contents, filename):
    # table = html.Div()
    if contents:
        df3=parse_data3(contents,filename)
        table = html.Div(
            [
                #html.H4(filename),
                dash_table.DataTable(
                    columns=[{"name": i, "id": i} for i in df3.columns],
                    data=df3.to_dict("records"),
                    editable=True

                ),
                html.Hr(),
                html.Div("Raw Content"),
                html.Pre(
                    contents[0:200] + "...",
                    style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"},
                ),
            ]
        )
        return table



@app.callback(
     Output("output_data_fin4", "children"),
     Input("upload_fin4", "contents"),
     Input("upload_fin4", "filename"),
)
def update_table(contents, filename):
    # table = html.Div()
    if contents:
        df4=parse_data4(contents,filename)
        table = html.Div(
            [
                #html.H4(filename),
                dash_table.DataTable(
                    columns=[{"name": i, "id": i} for i in df4.columns],
                    data=df4.to_dict("records"),
                    editable=True

                ),
                html.Hr(),
                html.Div("Raw Content"),
                html.Pre(
                    contents[0:200] + "...",
                    style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"},
                ),
            ]
        )
        return table       

####################################################################################################

@app.callback(
    [Output("table_valGral", "data"), Output('table_valGral', 'columns')],
    [Input("btn_valGral", "n_clicks")]
)
def updateTable(n_clicks):
    df_valGral2 = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query1_Tab1_filt.xlsx", sheet_name='Sheet_name_1')

    if n_clicks is None:
        return df_valGral.values[0:1], columns_valGral

    return df_valGral2.values, columns_valGral


@app.callback(
    [Output("table_valEf", "data"), Output('table_valEf', 'columns')],
    [Input("btn_valEnf", "n_clicks")]
)
def updateTable(n_clicks):
    df_valEnf2 = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query2_Tab1_filt3.xlsx", sheet_name='Sheet_name_1')

    if n_clicks is None:
        return df_valEnf.values[0:1], columns_valEnf

    return df_valEnf2.values, columns_valEnf



@app.callback(
    [Output("table_valPlat", "data"), Output('table_valPlat', 'columns')],
    [Input("btn_valCom", "n_clicks")]
)
def updateTable(n_clicks):
    df_valPlat2 = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query3_Tab1_filt.xlsx", sheet_name='Sheet_name_1')

    if n_clicks is None:
        return df_valPlat.values[0:1], columns_valPlat

    return df_valPlat2.values, columns_valPlat



@app.callback(
    [Output("table_valCom", "data"), Output('table_valCom', 'columns')],
    [Input("btn_valGral", "n_clicks")]
)
def updateTable(n_clicks):
    df_valCom2 = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query4_Tab1_filt.xlsx", sheet_name='Sheet_name_1')

    if n_clicks is None:
        return df_valCom.values[0:1], columns_valCom

    return df_valCom2.values, columns_valCom



@app.callback(
    [Output("table_valBono", "data"), Output('table_valBono', 'columns')],
    [Input("btn_valBono", "n_clicks")]
)
def updateTable(n_clicks):
    df_valBono2 = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query5_Tab1_filt.xlsx", sheet_name='Sheet_name_1')

    if n_clicks is None:
        return df_valBono.values[0:1], columns_valBono

    return df_valBono2.values, columns_valBono

app.clientside_callback(
    """
    function(n_clicks) {
        if (n_clicks > 0)
            document.querySelector("#table_to_export button.export").click()
        return ""
    }
    """,
    Output("export_table", "data-dummy"),
    [Input("export_table", "n_clicks")]
)


@app.callback(Output('output-data-upload', 'children'),
              Input('upload-data', 'contents'),
              State('upload-data', 'filename'),
              State('upload-data', 'last_modified'))
def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = [
            parse_contents(c, n, d) for c, n, d in
            zip(list_of_contents, list_of_names, list_of_dates)]
        return children


@app.callback(Output('output-data-upload-enfriador', 'children'),
              Input('upload-data-enfriador', 'contents'),
              State('upload-data-enfriador', 'filename'),
              State('upload-data-enfriador', 'last_modified'))
def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = [
            parse_contents(c, n, d) for c, n, d in
            zip(list_of_contents, list_of_names, list_of_dates)]
        return children



@app.callback(Output('output-data-upload-plataforma', 'children'),
              Input('upload-data-plataforma', 'contents'),
              State('upload-data-plataforma', 'filename'),
              State('upload-data-plataforma', 'last_modified'))
def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = [
            parse_contents(c, n, d) for c, n, d in
            zip(list_of_contents, list_of_names, list_of_dates)]
        return children



@app.callback(Output('output-data-upload-com', 'children'),
              Input('upload-data-com', 'contents'),
              State('upload-data-com', 'filename'),
              State('upload-data-com', 'last_modified'))
def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = [
            parse_contents(c, n, d) for c, n, d in
            zip(list_of_contents, list_of_names, list_of_dates)]
        return children




@app.callback(Output('output-data-upload-bono', 'children'),
              Input('upload-data-bono', 'contents'),
              State('upload-data-bono', 'filename'),
              State('upload-data-bono', 'last_modified'))
def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = [
            parse_contents(c, n, d) for c, n, d in
            zip(list_of_contents, list_of_names, list_of_dates)]
        return children
###############################################################################################
# graficos tabfiltros


@app.callback(
    Output('clientside-figure-store_filt2', 'data'),
    Input('clientside-graph-indicator_filt2', 'value'),
    Input('clientside-graph-country_filt2', 'value')
)
def update_store_data(indicator, UserName):
    dff = kf[kf['UserName'] == UserName]
    return [{
        'x': dff['VisitStartDateTime'],
        'y': dff[indicator],
        'mode': 'markers',
           }]


app.clientside_callback(
    """
    function(data, scale) {
        return {
            'data': data,
            'layout': {
                 'yaxis': {'type': scale}
             }
        }
    }
    """,
    Output('clientside-graph_filt2', 'figure'),
    Input('clientside-figure-store_filt2', 'data')
)



# FASE 4: Callback para actualizar gráfico de Segmento en función del dropdown de País y según selector de fechas
@app.callback(Output('barplot_ventas_seg_filt2', 'figure'),
              [Input('selector_fecha_filt2', 'start_date'), Input('selector_fecha_filt2', 'end_date'), Input('selector_filt2', 'value')])
def actualizar_graph_seg(fecha_min, fecha_max, seleccion):
    filtered_df = kf[(kf["VisitStartDateTime"]>=fecha_min) & (kf["VisitStartDateTime"]<=fecha_max) & (kf["BottlerName"]==seleccion)]

# FASE 2: CREACIÓN DE GRÁFICOS Y GROUPBY
    df_agrupado = filtered_df.groupby("UserName")["ice_points"].agg("mean").to_frame(name = "ice_points").reset_index()

    return{
        'data': [go.Bar(x=df_agrupado["UserName"],
                            y=df_agrupado["ice_points"] 
                            )],
        'layout': go.Layout(
            title="Puntos ICE por usuario",
            xaxis={'title': ""},
            yaxis={'title': "ice_points_mean"},
            hovermode='closest',
            xaxis_tickangle=45
        )}


# FASE 4: Callback para actualizar gráfico de beneficio de categorías en función del dropdown de País y según selector de fechas
@app.callback(Output('barplot_beneficio_cat_filt2', 'figure'),
              [Input('selector_fecha_filt2', 'start_date'),Input('selector_fecha_filt2', 'end_date'),Input('selector_filt2', 'value'),Input('barplot_ventas_seg_filt2', 'hoverData')])
def actualizar_graph_cat(fecha_min, fecha_max, seleccion,hoverData):
# FASE 5: Interactividad inter-gráfico hoverData
    v_index = hoverData['points'][0]['x']
    filtered_df = kf[(kf["VisitStartDateTime"]>=fecha_min) & (kf["VisitStartDateTime"]<=fecha_max) & (kf["BottlerName"]==seleccion) & (kf["UserName"]==v_index)]

# FASE 2: CREACIÓN DE GRÁFICOS Y GROUPBY
    df_agrupado = filtered_df.groupby("MarketSegment")["ice_points"].agg("mean").to_frame(name = "Bottler").reset_index()

    return{
        'data': [go.Bar(x=df_agrupado["MarketSegment"],
                            y=df_agrupado["Bottler"]
                            )],
        'layout': go.Layout(
            title="Tipo de tienda",
            xaxis={'title': ""},
            yaxis={'title': "ice_points_mean"},
            hovermode='closest'
                        )}



@app.callback(
    Output("pie-chart_filt2", "figure"), 
    [Input("linedropdown_filt2", "value"), 
     Input("piedropdown_filt2", "value")])
def generate_chart(linedropdown, piedropdown):
    fig = px.pie(dff, values=piedropdown, names=linedropdown)
    return fig

##############################################################################################
# Primeros elementos

@app.callback(
    [Output("table_filt2", "data"), Output('table_filt2', 'columns')],
    [Input("btn_filt2", "n_clicks")]
)

def updateTable(n_clicks):
    dff = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\df_selected1.xlsx", sheet_name='Sheet_name_1')

    if n_clicks is None:
        return df.values[0:1], columns[28:29]

    return dff.values[0:1], columns[28:29]



@app.callback(
    [Output("table2_filt2", "data"), Output('table2_filt2', 'columns')],
    [Input("btn2_filt2", "n_clicks")]
)

def updateTable(n_clicks):
    yf = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\df_selected1.xlsx", sheet_name='Sheet_name_1')

    if n_clicks is None:
        return #df.values[0:2], columns

    return yf.values, columns


# print("flags-as - Llenando la tabla")

    
# def get_engine():
#         connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
#                         'jrivas',
#                         'XXXXX',
#                         'koice',
#                         'ko_ice',
#                         1433)
#         res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
#         return res


# def get_query(query):
#         engine = get_engine()
#         res = pd.read_sql(query, con=engine)
#         return res
        
# dff = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\df_selected1.xlsx", sheet_name='Sheet_name_1')

    
# engine = get_engine()
# dff.to_sql('Chosen_sessionsUid_User', con=engine, schema='reportes', if_exists='append', index=False)
#         #print(df_line)


@app.callback(
    [Output('datatable-paging_filt2', 'figure')],
    [Input('table2_filt2', 'selected_rows'),
     Input('piedropdown_filt2', 'value'),
     Input('linedropdown_filt2', 'value')]
)
   
def update_data(chosen_rows,piedropval,linedropval):
    zf = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\df_selected1.xlsx", sheet_name='Sheet_name_1')
    # sessionuid=zf['SessionUid']
    # zf['sessionuid']=sessionuid

    # zf['id'] = zf['SessionUid']
    # zf.set_index('id', inplace=True, drop=False)

    # zf['Focus'] = zf['Focus'].fillna('No')


    if len(chosen_rows)==0:
        df_filterd = zf[zf['SessionUid'].isin([''])]
        print(df_filterd)
    else:
        print(chosen_rows)
        df_filterd = zf[zf.index.isin(chosen_rows)]


    #extract list of chosen countries
    list_chosen_countries=df_filterd['SessionUid'].tolist()
    #filter original df according to chosen countries
    #because original df has all the complete dates
    df_line = zf[zf['SessionUid'].isin(list_chosen_countries)]


    dfff = DataFrame (list_chosen_countries ,columns=['SessionUid'])
    # print('aquí')

    # dfff.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\chosen_sessionsUids_4validation.xlsx", sheet_name='Sheet_name_1', index=False)  


    #print(list_chosen_countries)
    print('aquí')
    print(dfff)

    df_f=pd.merge(zf,dfff,how='inner',on='SessionUid')

#######################################################################################
#######################################################################################

    df_f = df_f [['Review','ice_points','MarketSegment','month','day','Segment','LocationId','UserRole','SessionUid']]

    print(df_f)

    #####################
    # Validación general
    #####################

    # df_filterd=df_filterd.drop(columns=['SessionUid'])
    # df_filterd=df_filterd[['Review', 'Focus', 'MarketSegment', 'Segment', 'LocationId', 'sessionuid']]
    df_f['¿Hay error?']=''
    df_f['Tipo de error 1']=''
    df_f['¿Otro tipo de error? 2']=''
    df_f['¿Llegó Aclaración?']=''   

    # print(df_filterd.dtypes)

    #dfff.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\sessionsUid_4validation.xlsx", sheet_name='Sheet_name_1', index=False)  
    df_f.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\sessionsUid_4validation2.xlsx", sheet_name='Sheet_name_1', index=False)  


#######################################################################################

    #####################
    # Enfriador
    #####################


    df_f_Enfriador = df_f [['Review','ice_points','MarketSegment','month','day','Segment','LocationId','UserRole','SessionUid']]
    df_f_Enfriador['Capacidad_Fria']=''
    df_f_Enfriador['Respeto']=''
    df_f_Enfriador['Lleno_73pct']=''
    df_f_Enfriador['Precio']=''
    df_f_Enfriador['1a_Posicion']=''
    df_f_Enfriador['Cattman_Portafolio']=''
    df_f_Enfriador['Cattman_Frentes']=''
    df_f_Enfriador['Cattman_Mix']=''
    df_f_Enfriador['Cattman Bloques']=''
    df_f_Enfriador.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Local_IP\sessionsUid_4validate_enfriador.xlsx", sheet_name='Sheet_name_1', index=False)  


#######################################################################################


    #####################
    # Plataforma
    #####################


    df_f_Plataforma = df_f [['Review','ice_points','MarketSegment','month','day','Segment','LocationId','UserRole','SessionUid']]
    df_f_Plataforma['Portafolio_Comidas']=''
    df_f_Plataforma['Respeto_Comidas']=''
    df_f_Plataforma['Comunicacion_Comidas']=''
    df_f_Plataforma['Portafolio_Hidratacion']=''
    df_f_Plataforma['Respeto_Hidratacion']=''
    df_f_Plataforma['Comunicacion_Hidratacion']=''
    df_f_Plataforma['Portafolio_Nutricion']=''
    df_f_Plataforma['Respeto_Nutricion']=''
    df_f_Plataforma['Comunicacion_Nutrición']=''
    df_f_Plataforma['Portafolio_Ahorrack']=''
    df_f_Plataforma['Respeto_Ahorrack']=''
    df_f_Plataforma['Comunicacion_Ahorrack']=''
    df_f_Plataforma.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Local_IP\sessionsUid_4validate_plataforma.xlsx", sheet_name='Sheet_name_1', index=False)  


#######################################################################################


    #####################
    # Comunicación
    #####################


    df_f_Comunicacion = df_f [['Review','ice_points','MarketSegment','month','day','Segment','LocationId','UserRole','SessionUid']]
    df_f_Comunicacion['Capacidad_Fria']=''
    df_f_Comunicacion['Respeto']=''
    df_f_Comunicacion['Lleno_73pct']=''
    df_f_Comunicacion.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Local_IP\sessionsUid_4validate_comunicacion.xlsx", sheet_name='Sheet_name_1', index=False)  



#######################################################################################


    #####################
    # Bono
    #####################


    df_f_Bono = df_f [['Review','ice_points','MarketSegment','month','day','Segment','LocationId','UserRole','SessionUid']]
    df_f_Bono['Sombra']=''
    df_f_Bono['Comunicacion']=''
    df_f_Bono.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Local_IP\sessionsUid_4validate_bono.xlsx", sheet_name='Sheet_name_1', index=False)  



#######################################################################################
# Validación general
#######################################################################################


    print("flags-tt - Borrando los datos de la tabla")
    query_clear_tt = """ 
    
    DELETE FROM reportes.sessionsTT_4validation

        """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()
#get_query(query_clear_Exh_as_cc_comp)
#####

    print("flags-as - Llenando la tabla")

    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res


    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    
    engine = get_engine()
    df_f.to_sql('sessionsTT_4validation', con=engine, schema='reportes', if_exists='append', index=False)
        #print(df_line)

#    print("flags-as - Armando query1_tab1")


    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res


    query3 = """ 

        select distinct tt.MarketSegment, tt.Segment, tt.SessionUid, sl.Question, sl.Response, sl.IsProcessed, sl.EvidenceImageURL 
        from reportes.sessionsTT_4validation tt 
            join dbo.SurveyList sl on sl.SessionUId = tt.SessionUid 
        where sl.Question = '¿La tienda tiene libre acceso a Clientes?'

                """

    df = pd.DataFrame(get_query(query3))
    df.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query1_Tab1_filt.xlsx", sheet_name='Sheet_name_1', index=False)

#######################################################################################
# Enfriador
#######################################################################################


    print("flags-tt - Borrando los datos de la tabla Enfriador")
    query_clear_tt = """ 
    
    DELETE FROM reportes.sessionsTT_4validation_Enfriador

        """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()
#get_query(query_clear_Exh_as_cc_comp)
#####

    print("flags-as - Llenando la tabla Enfriador")

    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res


    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    
    engine = get_engine()
    df_f_Enfriador.to_sql('sessionsTT_4validation_Enfriador', con=engine, schema='reportes', if_exists='append', index=False)
        #print(df_line)

#    print("flags-as - Armando query1_tab1")


    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res


    query2 = """ 

        select distinct tt.SessionUid, sl.Question, sl.Response, sl.IsProcessed, sl.EvidenceImageURL 
from reportes.sessionsTT_4validation_Enfriador tt 
       join dbo.SurveyList sl on sl.SessionUId = tt.SessionUid 
where sl.Question = '¿El enfriador se encuentra lleno arriba del 75%?'


                """

    tf = pd.DataFrame(get_query(query2))
    tf.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query2_Tab1_filt3.xlsx", sheet_name='Sheet_name_1', index=False)

#######################################################################################
# Plataforma
#######################################################################################

    print("flags-tt - Borrando los datos de la tabla Plataforma")
    query_clear_tt = """ 
    
    DELETE FROM reportes.sessionsTT_4validation_Plataforma

        """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()
#get_query(query_clear_Exh_as_cc_comp)
#####

    print("flags-as - Llenando la tabla Plataforma")

    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res


    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    
    engine = get_engine()
    df_f_Plataforma.to_sql('sessionsTT_4validation_Plataforma', con=engine, schema='reportes', if_exists='append', index=False)
        #print(df_line)

#    print("flags-as - Armando query1_tab1")


    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res


    query4 = """ 

        select distinct tt.SessionUid, sl.Question, sl.Response, sl.IsProcessed, sl.EvidenceImageURL 
        from reportes.sessionsTT_4validation_Plataforma tt 
            join dbo.SurveyList sl on sl.SessionUId = tt.SessionUid 
        where sl.Question = '¿Qué tipo de Plataforma es?'


                """

    trf = pd.DataFrame(get_query(query4))
    trf.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query3_Tab1_filt.xlsx", sheet_name='Sheet_name_1', index=False)


#######################################################################################
# Comunicación
#######################################################################################


    print("flags-tt - Borrando los datos de la tabla Comunicación")
    query_clear_tt = """ 
    
    DELETE FROM reportes.sessionsTT_4validation_Comunicacion

        """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()
#get_query(query_clear_Exh_as_cc_comp)
#####

    print("flags-as - Llenando la tabla Comunicación")

    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res


    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    
    engine = get_engine()
    df_f_Comunicacion.to_sql('sessionsTT_4validation_Comunicacion', con=engine, schema='reportes', if_exists='append', index=False)
        #print(df_line)

#    print("flags-as - Armando query1_tab1")


    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res


    query5 = """ 

        select distinct tt.SessionUid, sl.Question, sl.Response, sl.IsProcessed, sl.EvidenceImageURL 
from reportes.sessionsTT_4validation_Comunicacion tt 
       join dbo.SurveyList sl on sl.SessionUId = tt.SessionUid 
where sl.Question = '¿Qué material exterior FIJO de Coca-Cola hay y cuántos?'


                """

    trrf = pd.DataFrame(get_query(query5))
    trrf.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query4_Tab1_filt.xlsx", sheet_name='Sheet_name_1', index=False)
#######################################################################################
# Bono
#######################################################################################


    print("flags-tt - Borrando los datos de la tabla Bono")
    query_clear_tt = """ 
    
    DELETE FROM reportes.sessionsTT_4validation_Bono

        """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()
#get_query(query_clear_Exh_as_cc_comp)
#####

    print("flags-as - Llenando la tabla Bono")

    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res


    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    
    engine = get_engine()
    df_f_Bono.to_sql('sessionsTT_4validation_Bono', con=engine, schema='reportes', if_exists='append', index=False)
        #print(df_line)

#    print("flags-as - Armando query1_tab1")


    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res


    query6 = """ 

        select distinct tt.SessionUid, sl.Question, sl.Response, sl.IsProcessed, sl.EvidenceImageURL 
from reportes.sessionsTT_4validation_Bono tt 
       join dbo.SurveyList sl on sl.SessionUId = tt.SessionUid 
where sl.Question = '¿Qué materiales de comunicación existen y cuántos?'

                """

    trrrf = pd.DataFrame(get_query(query6))
    trrrf.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\Query5_Tab1_filt.xlsx", sheet_name='Sheet_name_1', index=False)

#######################################################################################
#######################################################################################


@app.callback(
    dash.dependencies.Output('table-container_bit', 'children'),
    [dash.dependencies.Input('dropdown_bit', 'value')])
def display_table(dropdown_value):
    if dropdown_value is None:
        return generate_table()

    dff = jf[jf.Review.str.contains(''.join(dropdown_value))]

    #dff['random_sample'] = False
    #dff.loc[dff.sample(frac=0.2, random_state=1).index, 'random_sample'] = True

    dff.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\df_selected1.xlsx", sheet_name='Sheet_name_1', index=False)
    
    
    print("flags-tt - Borrando los datos de la tabla")
    query_clear_tt = """ 
    
    DELETE FROM list_chosen_user_tt

        """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()
#get_query(query_clear_Exh_as_cc_comp)


    print("flags-as - Llenando la tabla")

    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res


    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    
    engine = get_engine()
    dff.to_sql('list_chosen_user_tt', con=engine, schema='reportes', if_exists='append', index=False)

    print(dff)

    return dash_table.DataTable(
        id='datatable-row-ids',
        columns=[
            {'name': i, 'id': i, 'deletable': True} for i in dff.columns
            # omit the id column
            if i != 'id'
        ],
        data=dff.to_dict('records'),
        editable=True,
        sort_mode='multi',
        fixed_rows = {'headers':True},
                                style_table = {
                                    'overflowX':'auto'
                                },
                                style_cell = {
                                    'textAlign' : 'center',
                                    'width': '100px',
                                    'minWidth': '100px',
                                    'maxWidth': '100px',
                                    'overflow': 'hidden',
                                    'color': 'black',
                                },
                                style_data = {'backgroundColor':'lavender'},
                                style_header = {
                                    'fontWeight':'bold',
                                    'overflowX':'scroll',
                                    'backgroundColor':'#43A6E8'
                                },
                                style_data_conditional = [
                                    {
                                        'if': {
                                            'filter_query': '{ice_points} <= 30.0'
                                        },
                                        'backgroundColor':'#5DADE2',
                                        'fontWeight':'italics'
                                    },
                                                                        ],
            )



@app.callback(
        Output("output-1_enf","children"),
        [Input("save-button_enf","n_clicks")],
        [State("table_enf","data")]
        )

def selected_data_to_xlsx(nclicks,table1_enf): 
    if nclicks == 0:
        raise PreventUpdate
    else:
        z2_enf = pd.DataFrame(table1_enf)
        #.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\table_test_saveChanges.xlsx", sheet_name='Sheet_name_1', index=False)
        #return "Data Submitted"

#z2 = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\table_test_saveChanges.xlsx", sheet_name='Sheet_name_1')

    print("carga - Borrando los datos de la tabla")
    query_clear_tt_enf = """ 
    
    DELETE FROM Validacion_Enfriador
       """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt_enf)
    conn.close()

    print("cargando - Llenando la tabla")
    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res

    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res
    
    engine = get_engine()
    z2_enf.to_sql('Validacion_Enfriador2', con=engine, schema='reportes', if_exists='append', index=False)



@app.callback(
    Output('clientside-figure-store', 'data'),
    Input('clientside-graph-indicator', 'value'),
    Input('clientside-graph-country', 'value')
)
def update_store_data(indicator, UserName):
    dff = kf[kf['UserName'] == UserName]
    return [{
        'x': dff['VisitStartDateTime'],
        'y': dff[indicator],
        'mode': 'markers',
           }]


app.clientside_callback(
    """
    function(data, scale) {
        return {
            'data': data,
            'layout': {
                 'yaxis': {'type': scale}
             }
        }
    }
    """,
    Output('clientside-graph', 'figure'),
    Input('clientside-figure-store', 'data')
)


@app.callback(
        Output("output-1","children"),
        [Input("save-button","n_clicks")],
        [State("table","data")]
        )

def selected_data_to_xlsx(nclicks,table1): 
    if nclicks == 0:
        raise PreventUpdate
    else:
        z2 = pd.DataFrame(table1)
        #.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\table_test_saveChanges.xlsx", sheet_name='Sheet_name_1', index=False)
        #return "Data Submitted"

#z2 = pd.read_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\table_test_saveChanges.xlsx", sheet_name='Sheet_name_1')

    print("carga - Borrando los datos de la tabla")
    query_clear_tt = """ 
    
    DELETE FROM Validacion_General
       """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()

    print("cargando - Llenando la tabla")
    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res

    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res
    
    engine = get_engine()
    z2.to_sql('Validacion_General', con=engine, schema='reportes', if_exists='append', index=False)



# FASE 4: Callback para actualizar gráfico de Segmento en función del dropdown de País y según selector de fechas
@app.callback(Output('barplot_ventas_seg', 'figure'),
              [Input('selector_fecha', 'start_date'), Input('selector_fecha', 'end_date'), Input('selector', 'value')])
def actualizar_graph_seg(fecha_min, fecha_max, seleccion):
    filtered_df = kf[(kf["VisitStartDateTime"]>=fecha_min) & (kf["VisitStartDateTime"]<=fecha_max) & (kf["BottlerName"]==seleccion)]

# FASE 2: CREACIÓN DE GRÁFICOS Y GROUPBY
    df_agrupado = filtered_df.groupby("UserName")["ice_points"].agg("mean").to_frame(name = "ice_points").reset_index()

    return{
        'data': [go.Bar(x=df_agrupado["UserName"],
                            y=df_agrupado["ice_points"] 
                            )],
        'layout': go.Layout(
            title="Puntos ICE por usuario",
            xaxis={'title': ""},
            yaxis={'title': "ice_points_mean"},
            hovermode='closest',
            xaxis_tickangle=45
        )}


# FASE 4: Callback para actualizar gráfico de beneficio de categorías en función del dropdown de País y según selector de fechas
@app.callback(Output('barplot_beneficio_cat', 'figure'),
              [Input('selector_fecha', 'start_date'),Input('selector_fecha', 'end_date'),Input('selector', 'value'),Input('barplot_ventas_seg', 'hoverData')])
def actualizar_graph_cat(fecha_min, fecha_max, seleccion,hoverData):
# FASE 5: Interactividad inter-gráfico hoverData
    v_index = hoverData['points'][0]['x']
    filtered_df = kf[(kf["VisitStartDateTime"]>=fecha_min) & (kf["VisitStartDateTime"]<=fecha_max) & (kf["BottlerName"]==seleccion) & (kf["UserName"]==v_index)]

# FASE 2: CREACIÓN DE GRÁFICOS Y GROUPBY
    df_agrupado = filtered_df.groupby("MarketSegment")["ice_points"].agg("mean").to_frame(name = "Bottler").reset_index()

    return{
        'data': [go.Bar(x=df_agrupado["MarketSegment"],
                            y=df_agrupado["Bottler"]
                            )],
        'layout': go.Layout(
            title="Tipo de tienda",
            xaxis={'title': ""},
            yaxis={'title': "ice_points_mean"},
            hovermode='closest'
                        )}



@app.callback(Output("content", "children"), [Input("tabs", "active_tab")])
def switch_tab(at):
    if at == "tab-1":
        return "Data Science Division"
    elif at == "tab-1-5":
        return "Data Science Division"
    elif at == "tab-2":
        return "Data Science Division"
    elif at == "tab-2-5":
        return "Data Science Division"  
    elif at == "tab-0":
        return "Data Science Division"  
    elif at == "tab-05":
        return "Data Science Division"
    elif at == "tab-3":
        return "Data Science Division"
    elif at == "tab-4":
        return "Data Science Division"
    elif at == "tab-5":
        return "Data Science Division"
    elif at == "tab-6":
        return "Data Science Division"
    elif at == "tab-7":
        return "Data Science Division"
    elif at == "tab-8":
        return "Data Science Division"
    return html.P("This shouldn't ever be displayed...")


@app.callback(
    Output("pie-chart", "figure"), 
    [Input("linedropdown", "value"), 
     Input("piedropdown", "value")])
def generate_chart(linedropdown, piedropdown):
    fig = px.pie(dff, values=piedropdown, names=linedropdown)
    return fig



@app.callback(
    [Output('datatable-paging', 'figure')],
    [Input('datatable_id', 'selected_rows'),
     Input('piedropdown', 'value'),
     Input('linedropdown', 'value')]
)
def update_data(chosen_rows,piedropval,linedropval):
    if len(chosen_rows)==0:
        df_filterd = dff[dff['sessionuid'].isin([''])]
        print(df_filterd)
    else:
        print(chosen_rows)
        df_filterd = dff[dff.index.isin(chosen_rows)]


    #extract list of chosen countries
    list_chosen_countries=df_filterd['sessionuid'].tolist()
    #filter original df according to chosen countries
    #because original df has all the complete dates
    df_line = df[df['sessionuid'].isin(list_chosen_countries)]
    
    dfff = DataFrame (list_chosen_countries ,columns=['SessionUid'])


    #print(list_chosen_countries)
    print('aquí')
    print(dfff)

    df_filterd=df_filterd.drop(columns=['SessionUid'])
    df_filterd=df_filterd[['Review', 'Focus', 'MarketSegment', 'Segment', 'LocationId', 'sessionuid']]
    df_filterd['¿Hay error?']=''
    df_filterd['Tipo de error 1']=''
    df_filterd['¿Otro tipo de error? 2']=''
    df_filterd['¿Llegó Aclaración?']=''   

    print(df_filterd.dtypes)

    df_filterd.to_excel(r"C:\Users\Lenovo\OneDrive - Storecheck S.A. de C.V\Documentos\Dashboard_visor_capturador\dataframes\list_chosen_sessionsUid.xlsx", sheet_name='Sheet_name_1', index=False)  
######
    print("flags-tt - Borrando los datos de la tabla")
    query_clear_tt = """ 
    
    DELETE FROM list_chosen_sessionsUid_tt2

        """

    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res
    
    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    eng = get_engine()
    conn = eng.connect()
    conn.execute(query_clear_tt)
    conn.close()
#get_query(query_clear_Exh_as_cc_comp)
#####

    print("flags-as - Llenando la tabla")

    
    def get_engine():
        connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
        res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
        return res


    def get_query(query):
        engine = get_engine()
        res = pd.read_sql(query, con=engine)
        return res

    
    engine = get_engine()
    df_filterd.to_sql('list_chosen_sessionsUid_tt2', con=engine, schema='reportes', if_exists='append', index=False)
        #print(df_line)



    
if __name__ == "__main__":
    app.run_server(port=8071)
