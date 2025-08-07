import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import os

# Variáveis de ambiente
DB_URL= "postgresql+psycopg2://postgres:postgres@db:5432/dataflow"
TABLE= "weather_forecast_raw"

engine = create_engine(DB_URL)

app = dash.Dash(__name__)
app.title = "Dashboard Climático"

app.layout = html.Div([
    html.H1("Clima - Atualização em tempo real"),
    dcc.Interval(id='interval-component', interval=1*1000, n_intervals=0),
    dcc.Graph(id='line-graph'),
    dcc.Graph(id='pie-description'),
    dcc.Graph(id='pie-condition')
])

@app.callback(
    [Output('line-graph', 'figure'),
     Output('pie-description', 'figure'),
     Output('pie-condition', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_graph(n):
    query = f"SELECT * FROM {TABLE} ORDER BY inserted_at LIMIT {n+1}"
    df = pd.read_sql(query, engine)

    if df.empty:
        return {}, {}, {}

    df["temperature"] = df["temp_range"].str.extract(r'(\d+)').astype(float)
    df["wind"] = df["wind_speedy"].str.extract(r'([\d\.]+)').astype(float)

    fig_line = px.bar(df, x="full_date", y="temperature", title="Temperatura ao longo do tempo")

    fig_desc = px.pie(df, names="description", title="Distribuição de descrição")
    fig_cond = px.pie(df, names="condition", title="Distribuição de condição")

    return fig_line, fig_desc, fig_cond

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050)