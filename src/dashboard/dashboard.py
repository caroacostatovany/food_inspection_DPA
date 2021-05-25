# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd

from src.utils.general import get_db_conn_psycopg, read_pkl_from_s3
from src.utils.constants import CREDENCIALES, S3, BUCKET_NAME

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

conn = get_db_conn_psycopg(CREDENCIALES)

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options


# Distribución de scores del modelo seleccionado en el último punto en el tiempo  con el que fue validado

query_2 = """ select * 
              from results.validation;
          """

dashboard_historial = pd.read_sql(query_2, conn)

fig_2 = px.histogram(dashboard_historial, x="predicted_score_1", marginal="rug",
                   hover_data=dashboard_historial.columns)

# Distribución obtenida para las últimas predicciones (último consecutivo).


query = """
            select * 
            from monitoring.scores;
        """

dashboard = pd.read_sql(query, conn)

fig = px.histogram(dashboard, x="predicted_score_1", color="predicted_labels", marginal="rug",
                   hover_data=dashboard.columns)


app.layout = html.Div(children=[
    html.H1(children='Food inspection dashboard'),

    html.Div(children='''
        Distribución de scores de predicciones
    '''),

    dcc.Graph(
        id='nueva-prediccion',
        figure=fig
    ),

    html.Div(children='''
        Distribución de scores de historial
    '''),

    dcc.Graph(
        id='distribucion-validada',
        figure=fig_2
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
