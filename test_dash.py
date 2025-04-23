import dash
from dash import html, dcc
import plotly.express as px
import pandas as pd

app = dash.Dash(__name__)
df = px.data.iris()

fig = px.scatter(df, x='sepal_width', y='sepal_length', color='species')

app.layout = html.Div(children=[
    html.H1('Iris Dataset Dashboard'),
    dcc.Graph(figure=fig)
])

if __name__ == '__main__':
    app.run(debug=True)