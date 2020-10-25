import pandas as pd
import plotly.express as px
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from cassandra.cluster import Cluster
from ast import literal_eval
from ml_model import main_task

real_data = True
if real_data:
    # Setting up connection to Cassandra
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect('tweets_space')
    def to_df(col_names, rows):
        return pd.DataFrame(rows, columns=col_names)
    session.row_factory = to_df
    session.default_fetch_size = None
    query = "SELECT * FROM tweets"

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
            interval=1*500,
            n_intervals=0
        )
    ], className = 'row'), className = 'Container'
)

# For updating the numbers
@app.callback(Output('live-update-metrics', 'children'),
              [Input('interval-component', 'n_intervals')])

def update_metrics(n):
    if real_data:
        rslt = session.execute(query, timeout=None)
        df = rslt._current_rows
    else:
        df = pd.read_csv('tweets_2.csv', header = None)  
        df.columns = ['id','content', 'input','rules','target', 'timestamp']
        df['input'] = df['input'].fillna("[[None,None,None,None]]")
        # Needs literal her
    # Some totals to show
    tweet_count = str(df.count()['id'])
    df_clean = df[df['input'].map(lambda x: x != None)]
    retweet_count = str(df_clean['input'].map(lambda x: x[0][3]).sum())

    # Split this into one where we have tweets 5 min old

    # Picking out data where we only have input
    df_five_min = df[df['target'].map(lambda x: x == None)]
    # Where we have recieved at least one lookup
    df_five_min = df_five_min[df_five_min['input'].map(lambda x: x != None)]
    # Where the input is valid
    df_five_min = df_five_min[df_five_min['input'].map(lambda x: x[0][0] != None)]

    # Picking out data where we have a target
    df_thirty_min = df[df['target'].map(lambda x: x != None)]



    df = df_five_min
    top_tweet_idx = df['input'].map(lambda x: x[0][3]).idxmax()
    top_tweet = str(df['id'][top_tweet_idx])
    top_tweet_current_retweets = str(df['input'][top_tweet_idx][0][0])
    top_tweet_prediction_retweets = '0'  # todo
    top_tweet_content = str(df['content'][top_tweet_idx])
    top_predicted_tweet_idx = df['input'].map(lambda x: x[0][3]).idxmax()  # todo
    top_predicted_tweet = str(df['id'][top_predicted_tweet_idx])
    top_predicted_tweet_current_retweets = str(df['input'][top_predicted_tweet_idx][0][0])
    top_predicted_tweet_prediction = '0'  # todo
    top_predicted_tweet_content = str(df['content'][top_predicted_tweet_idx])

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
            has_no_values = True

    if has_no_values:
        top_tweet_idx = 'NA'
        top_tweet_30 = 'NA'
        top_tweet_content_30 = 'NA'
        top_tweet_retweets_30 = 'NA'
        top_tweet_predicted_retweets_30 = 'NA'
    else:
        top_tweet_idx = df['input'].map(lambda x: x[0][3]).idxmax()
        top_tweet_30 = str(df['id'][top_tweet_idx])
        top_tweet_content_30 = str(df['content'][top_tweet_idx])
        top_tweet_retweets_30 = str(df['input'][top_tweet_idx][0][0])
        top_tweet_predicted_retweets_30 = '0'

    # To be implemented
    overall_acc = str(0) # todo
    
    h_style = {} #{'padding': '10px', 'fontSize': '16px', 'font-family': 'Arial, Helvetica, sans-serif'}
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
                className = "columns two"),
            html.Div([
                html.H5('Total reach through retweets: ' + retweet_count, style=h_style)],
                className = "columns two"),
            html.Div([
                html.H5('Overall Accuracy: ' + overall_acc, style=h_style)],
                className = "columns two")
            ], className = "row"),
        
        # Creating the tables
        html.Div([
            html.Div(' ',
                className = "columns two ", style = padding_style),
            html.Div([
                html.H3('Tweets seen for 5 min', style=h_style, className="row"),
                html.H5('Current top tweet', style=h_style, className="row"),
                html.P(['ID: ',html.P(top_tweet, style = id_style ), html.Span(top_tweet_content, className = 'extra')], style=style),
                html.P('Current number of retweets: ' + top_tweet_current_retweets, style=style),
                html.P('30 min prediction: ' + top_tweet_prediction_retweets, style=style),
                html.P('------------------', className = "row"),
                html.H5('Predicted top tweet', style=h_style, className="row"),
                html.P(['ID: ',html.P(top_predicted_tweet, style = id_style ), html.Span(top_predicted_tweet_content, className='extra')],
                       style=style),
                html.P('Current number of retweets: ' + top_predicted_tweet_current_retweets, style=style),
                html.P('30 min prediction: ' + top_predicted_tweet_prediction, style=style)],
                className = "columns four"),
            html.Div([
                html.H3('Outcome after 30 min', style=h_style, className="row"),
                html.H5('Predicted top tweet', style=h_style, className="row"),
                html.P(['ID: ',html.P(top_tweet_30, style = id_style ), html.Span(top_tweet_content_30, className = 'extra')], style=style),
                html.P('Current number of retweets: ' + top_tweet_retweets_30, style=style),
                html.P('Predicted number of retweets: ' + top_tweet_predicted_retweets_30, style=style)],
                className = "columns four")
            ],
            className = "row")
        ]

# For updating the graph
@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals')],
              [Input('dropdown', 'value')])
def update_graph_live(n, value):

    if real_data:
        rslt = session.execute(query, timeout=None)
        df = rslt._current_rows
    else:
        df = pd.read_csv('tweets_2.csv', header=None)
        df.columns = ['id', 'content', 'input', 'rules', 'target', 'timestamp']
        df = df.fillna("[[0,0,0,0]]")
        df['input'] = df['input'].apply(literal_eval)
    # Where we have recieved at least one lookup

    df['timestamp'] = df['timestamp'].map(lambda x: pd.Timestamp(x, unit='s'))

    # Grouping and counting by hour
    #df['ts'] = df['ts'].map(lambda x: x[13:-13]) todo leaving these as they depend on format from cassandra
    #df['ts'] = pd.to_datetime(df['ts'], infer_datetime_format=True)  # " format = '%m %Y')

    df['timestamp'] = df['timestamp'].map(lambda x: str(x.hour) + ":" + str(x.minute))

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