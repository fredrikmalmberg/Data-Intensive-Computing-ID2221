import pandas as pd
import plotly.express as px
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from cassandra.cluster import Cluster
import numpy as np
import time
from ml_model import build_model, process_data, train

# Setting up connection to Cassandra
cluster = Cluster(["127.0.0.1"])
session = cluster.connect('tweets_space')
def to_df(col_names, rows):
    return pd.DataFrame(rows, columns=col_names)
session.row_factory = to_df
session.default_fetch_size = None
query = "SELECT * FROM tweets"
model = None

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.scripts.config.serve_locally = False

padding_style = {'height' : '1px'} # for some reason bootstrap remove empty columns with 0 height

server = app.server
app.layout = html.Div(
    html.Div([
        html.Div([
            html.Div([
                html.Div('', style= padding_style)],
                className = "columns two"),
            html.Div([
                html.H1('Tweet Dashboard')],
                className = "columns five")
            
            ], className = "row"),
        html.Div(id='live-update-accuracy', className = 'row'),
        html.Div(id='live-update-metrics', className = 'row'),

        html.Div([
            html.H3('Number of tweets coming in',style = {'padding-left' : '20%'}, className="row"),
            dcc.Dropdown(
                id='dropdown', className="row",
                style = {'padding-left': '20%', 'fontSize': '20px'},
                options=[
                    {'label': 'Tweets sent over time', 'value': 'tweets'},
                    {'label': 'Reach by tweet send time', 'value': 'retweets'},
                ],
                value='tweets'
            ),
            dcc.Graph(id='live-update-graph', style = {'width' : '80%', 'float' : 'right'}),
            ], className = "columns eight"),
        dcc.Interval(
            id='interval-component',
            interval=1*5000,
            n_intervals=0
        ),
        dcc.Interval(
            id='interval-component2',
            interval=1 * 1000,
            n_intervals=0
        ),
        dcc.Interval(
            id='interval-component3',
            interval=1*5000,
            n_intervals=0
        )
    ], className = 'row'), className = 'Container'
)


# For updating the numbers
@app.callback(Output('live-update-accuracy', 'children'),
              [Input('interval-component3', 'n_intervals')])
def update_accuracy(n):
    global model
    rslt = session.execute(query, timeout=None)
    df = rslt._current_rows
    overall_acc = str(0)
    x_predict, x_train, y_train, x_test, y_test = process_data(df)
    if len(x_test) > 0:
        if model is None:
            overall_acc = '0'
        else:
            overall_acc = "{:.2f}".format(model.evaluate(x_test, y_test)[1])
    h_style = {}
    return [
        # Creating the headlines

        html.Div([
            html.Div([
                html.Div('', style=padding_style)],
                className="columns two"),
            html.Div([html.H4('Overall MSE: ' + overall_acc, style=h_style)],
                className="columns six")
        ], className="row")
    ]
# For updating the numbers
@app.callback(Output('live-update-metrics', 'children'),
              [Input('interval-component', 'n_intervals')])

def update_metrics(n):
    global model
    rslt = session.execute(query, timeout=None)
    df = rslt._current_rows
    x_predict, x_train, y_train, x_test, y_test = process_data(df)
    if len(x_train) > 0:
        if model is None:
            # To be implemented
            model = build_model()
        else:
            pass
        history = train(model, x_train, y_train)

    # Some totals to show
    tweet_count = str(df.count()['id'])
    df_clean = df[df['input'].map(lambda x: x != None)]
    retweet_count = str(df_clean['input'].map(lambda x: x[0][0]).sum())

    # Split this into one where we have tweets 5 min old

    # Picking out data where we only have input
    df_five_min = df[df['target'].map(lambda x: x == None)]
    # Where we have recieved at least one lookup
    df_five_min = df_five_min[df_five_min["input"].notna() & (df_five_min["input"].dropna().map(len) == 5)]

    # Where the input is valid
    df_five_min = df_five_min[df_five_min['input'].map(lambda x: x[0][0] != None)]
    # only 10 min old tweets are shown here
    df_five_min = df_five_min[df_five_min['timestamp'].map(lambda x: (int(time.time()-x)/60) < 10)] # Turned this of for non-live

    # Picking out data where we have a target
    df_thirty_min = df[df['target'].map(lambda x: x != None)]
    df_thirty_min = df_thirty_min[df_thirty_min["input"].notna() & (df_thirty_min["input"].dropna().map(len) == 5)]

    df = df_five_min
    top_tweet_idx = df['input'].map(lambda x: x[0][0]).idxmax()
    top_tweet = str(df['id'][top_tweet_idx])
    top_tweet_current_retweets = str(df['input'][top_tweet_idx][0][0])
    prediction = str(int(model.predict(np.array(df_five_min['input'][top_tweet_idx]).reshape(1,20)[0:])[0,0]))
    top_tweet_prediction_retweets = prediction
    top_tweet_content = str(df['content'][top_tweet_idx])

    # Show the ones where we have final values
    df = df_thirty_min

    #
    has_no_values = False
    if df['id'].count() == 0:
        has_no_values = True
    else:
        # making sure we have inputs
        df = df[df['input'].map(lambda x: x != None)]
        # Where the input is valid
        df = df[df['input'].map(lambda x: x[0] != None)]
        df = df[df['input'].map(lambda x: x[0][0] != None)]
        if df['id'].count() == 0:
            print("No values with targets and valid input")
            has_no_values = True

    if has_no_values:
        top_tweet_idx = 'NA'
        top_tweet_30 = 'NA'
        top_tweet_content_30 = 'NA'
        top_tweet_retweets_30 = 'NA'
        top_tweet_predicted_retweets_30 = 'NA'
    else:
        top_tweet_idx = df['target'].map(lambda x: x[0]).idxmax()
        top_tweet_30 = str(df['id'][top_tweet_idx])
        top_tweet_content_30 = str(df['content'][top_tweet_idx])
        top_tweet_retweets_30 = str(df['target'][top_tweet_idx][0])
        prediction = str(int(model.predict(np.array(df['input'][top_tweet_idx]).reshape(1, 20)[0:])[0, 0]))
        top_tweet_predicted_retweets_30 = prediction


    
    h_style = {}
    style ={'fontSize': '16px'}
    id_style = {'fontSize': '16px', 'color': 'darkblue', 'text-decoration': 'underline'}

    
    return [
        
        # Creating the headlines
        html.Div([
            html.Div([
                html.Div('', style= padding_style)],
                className = "columns two"),
            html.Div([
                html.H5('Number of tweets monitored: ' + tweet_count, style=h_style)],
                className = "columns three"),
            html.Div([
                html.H5('Total reach through retweets: ' + retweet_count, style=h_style)],
                className = "columns three")#,
            #html.Div([
            #    html.H5('Overall Accuracy: ' + overall_acc, style=h_style)],
            #    className = "columns two")
            ], className = "row"),
        
        # Creating the table
        html.Div([
            html.Div(' ',
                className = "columns two ", style = padding_style),
            html.Div([
                html.H3('Tweets seen for 5 min', style=h_style, className="row"),
                html.H5('Current top tweet', style=h_style, className="row"),
                html.P(['ID: ',html.P(top_tweet, style = id_style ), html.Span(top_tweet_content, className = 'extra')], style=style),
                html.P('Current number of retweets: ' + top_tweet_current_retweets, style=style),
                html.P('30 min prediction: ' + top_tweet_prediction_retweets, style=style)],
                className = "columns three"),
            html.Div([
                html.H3('Outcome after 30 min', style=h_style, className="row"),
                html.H5('Top tweet', style=h_style, className="row"),
                html.P(['ID: ',html.P(top_tweet_30, style = id_style ), html.Span(top_tweet_content_30, className = 'extra')], style=style),
                html.P('Current number of retweets: ' + top_tweet_retweets_30, style=style), # todo
                html.P('Predicted number of retweets: ' + top_tweet_predicted_retweets_30, style=style)],
                className = "columns three")
            ],
            className = "row")
        ]

# For updating the graph
@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component2', 'n_intervals')],
              [Input('dropdown', 'value')])
def update_graph_live(n, value):

    rslt = session.execute(query, timeout=None)
    df = rslt._current_rows
    # Where we have recieved at least one lookup

    df['timestamp'] = df['timestamp'].map(lambda x: pd.Timestamp(x, unit='s'))

    # Grouping and counting by hour
    df['timestamp'] = df['timestamp'].map(lambda x: str(x.hour) + ":" + (str(x.minute) if x.minute > 9 else '0' + str(x.minute)   ))
    if value == 'tweets':
        df_grouped = df.groupby(df['timestamp'])['id'].count()
    else:
        # We need to have done one lookup to show something here
        df = df[df['input'].map(lambda x: x != None)]
        df['input'] = df['input'].map(lambda x: x[0][0])
        df_grouped = df.groupby(df['timestamp'])['input'].sum()

    fig = px.line(df_grouped)
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)