import sys,os
from flask import Flask, flash, render_template, request, redirect, url_for, abort, send_from_directory
from werkzeug.utils import secure_filename
import pandas as pd
import numpy as np
import datetime
from threading import Thread
from bokeh.models.widgets import Slider
from bokeh.models import Select, ColumnDataSource, DataTable, TableColumn, Div, ResetTool
from tornado.ioloop import IOLoop
from bokeh.server.server import Server
from bokeh.embed import server_document, components
from bokeh.plotting import figure
from bokeh.layouts import column,row,layout
from bokeh.io import curdoc
from bokeh.events import Reset
import dateutil.parser
import numbers
import requests

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 150 * 1024 * 1024        # Setting the limit for uploading file
app.config['UPLOAD_EXTENSIONS'] = ['.csv', '.xlsx']         # Allowed file
app.config['UPLOAD_PATH'] = 'uploads'                       # Folder name to save the uploads

if not os.path.exists(app.config['UPLOAD_PATH']):
    os.mkdir(app.config['UPLOAD_PATH'])

###Visualizer###
type_visualize = ['Line', 'Scatter']
selection_flag = False

def plot_plot(plot_type,data):
    colors = ["blue","red","green","cyan"]
    plot = figure(x_axis_type="datetime", tools="pan,box_select,wheel_zoom,hover,reset,save", title="Your Plot")
    for i in range(len(data)):
        keys_list = list(data[i].data)
        if x_axis_plot not in keys_list:
            data[i].add(time_axis[x_axis_plot],name=x_axis_plot)
    if plot_type == 'Line':
        for i in range(len(list_df)):
            for y in range(len(y_axis_plot)):
                if len(data) > 1:
                    legend_name = selected_filtered_data[i]+'-'+y_axis_plot[y]
                else:
                    legend_name = y_axis_plot[y]
                plot.line(x_axis_plot, y_axis_plot[y], source=data[i], legend_label=legend_name, color=colors[i+y], name=legend_name)
    elif plot_type == 'Scatter':
        for i in range(len(list_df)):
            for y in range(len(y_axis_plot)):
                if len(data) > 1:
                    legend_name = selected_filtered_data[i]+'-'+y_axis_plot[y]
                else:
                    legend_name = y_axis_plot[y]
                plot.circle(x_axis_plot, y_axis_plot[y], source=data[i], legend_label=legend_name, color=colors[i+y], name=legend_name)
    plot.legend.location = "top_left"
    plot.legend.click_policy="hide"
    return plot

def bkapp(doc):
    global time_axis
    global selected
    global layout_1
    global data_table_list
    time_axis = pd.DataFrame({x_axis_plot: pd.Series([], dtype='datetime64[ns]')})
    temp_list = []
    for i in range(len(list_df)):
        for col in list_df[i].columns:
            if col in [x_axis_plot]:
                time_axis[col] = pd.Series(data=np.array(list_df[i][col].values))
            if col not in y_axis_plot:
                del list_df[i][col]

    for i in range(len(list_df)):
        temp_list.append(ColumnDataSource(list_df[i]))

    def moving_avg(attr, old, new):
        global layout_1
        if new == 0:
            for i in range(len(list_df)):
                data = list_df[i]
                temp_list[i].data = ColumnDataSource.from_df(data)
        else:
            for i in range(len(list_df)):
                data = list_df[i].rolling(new).mean()
                temp_list[i].data = ColumnDataSource.from_df(data)
        layout_1.children.pop()
        layout_1 = layout([[data_table_list,column(plot_plot(select_type.value,temp_list))]])
        doc.add_root(layout_1)

    def visual_type(attr, old, new):
        global layout_1
        layout_1.children.pop()
        layout_1 = layout([[data_table_list,column(plot_plot(select_type.value,temp_list))]])
        doc.add_root(layout_1)

    slider = Slider(start=0, end=30, value=0, step=1, title="Smoothing by N units")
    slider.on_change('value', moving_avg)
    select_type = Select(title="Type of Visualization:", value="Scatter", options=type_visualize)
    select_type.on_change('value', visual_type)

    def data_table_selection():
        global data_table_list
        data_table_list = []
        for i in range(len(temp_list)):
            source_summary = ColumnDataSource(selected[i])
            if len(temp_list) != 1:
                heading = "Summary - " + selected_filtered_data[i]
            else:
                heading = "Summary - "
            data_table_columns = []
            for i in list(source_summary.data):
                if i == "level_0":
                    data_table_columns.append(TableColumn(field=i, title='',),)
                else:
                    data_table_columns.append(TableColumn(field=i, title=i,),)
            data_table = DataTable(source=source_summary, columns=data_table_columns, index_header='', index_position=None, width=600, height=280)
            data_table_heading = Div(text=heading,width=600, height=20)
            data_table_list.append(data_table_heading)
            data_table_list.append(data_table)

    def selection_change(attrname, old, new):
        global data_table_list
        global selected
        global layout_1
        selected = []
        selection_flag = True
        for i in range(len(temp_list)):
            selected_indices = temp_list[i].selected.indices
            selected_df = temp_list[i].to_df()
            selected_df.set_index('index')
            selected.append(selected_df.iloc[selected_indices,:].describe())
        data_table_selection()   
        layout_1.children.pop()
        layout_1 = layout([[data_table_list,column(plot_plot(select_type.value,temp_list))]])
        doc.add_root(layout_1)

    for i in range(len(temp_list)):
        temp_list[i].selected.on_change('indices', selection_change)
    
    selected = []
    for i in range(len(temp_list)):
        selected_df = temp_list[i].to_df()
        selected_df.set_index('index')
        selected.append(selected_df.describe())

    data_table_selection()
    layout_1 = layout([[data_table_list,column(plot_plot(select_type.value,temp_list))]])
    doc.add_root(layout([slider, select_type]))
    doc.add_root(layout_1)

@app.route('/visualizer', methods=['GET'])
def visualizer():
    script = server_document('http://localhost:5006/bkapp')
    return render_template("visualizer.html", script=script, template="Flask")

def bk_worker():
    server = Server({'/bkapp': bkapp}, io_loop=IOLoop(), allow_websocket_origin=["localhost:8000"])
    server.start()
    server.io_loop.start()

Thread(target=bk_worker).start()
##############

###Analyser###
list_time_data_type = ['Datetime','Others (Numerical data)']
filter_trigger = False
@app.route('/analyser')
def analyser():
    global df
    global time_recommendations
    global file_ext
    time_recommendations = []
    if file_ext == '.csv':
        df = pd.read_csv(os.path.join(app.config['UPLOAD_PATH'], filename))
    elif file_ext == '.xlsx':
        df = pd.read_excel(os.path.join(app.config['UPLOAD_PATH'], filename), engine='openpyxl')
    return render_template('analyser.html',  tables=[df.head(5).to_html(classes='data', header="true")], tables1=[df.describe().to_html(classes='data', header="true")], xcolumns=time_recommendations, columns=df.columns.to_list(), datatype_list=list_time_data_type, many_to_one=0)

@app.route('/analyser', methods=['POST'])
def plot_parameters_validation():
    global list_df
    global filter_trigger
    global filtered_column
    global x_axis_plot
    global y_axis_plot
    global selected_filtered_data
    list_df = []
    x_axis_plot = request.form.get('x_axis')
    y_axis_plot = request.form.getlist('y_axis')
    x_datatype = request.form.get('x_axis_datatype')
    y_filter = request.form.get('y_axis_filter')
    try:
        if x_datatype == 'Datetime':
            if isinstance(df[x_axis_plot][0], numbers.Number):
                return render_template('analyser.html',  tables=[df.head(5).to_html(classes='data', header="true")], tables1=[df.describe().to_html(classes='data', header="true")], xcolumns=time_recommendations, columns=df.columns.to_list(), x_axis_datatype_error="Time axis data type is invalid (Should have been Numerical)", datatype_list=list_time_data_type)     #Invalid X axis data type error (Should have been Datetime)
            df[x_axis_plot] = pd.to_datetime(df[x_axis_plot])
        elif x_datatype == 'Others (Numerical data)':
            if not isinstance(df[x_axis_plot][0], numbers.Number):
                return render_template('analyser.html',  tables=[df.head(5).to_html(classes='data', header="true")], tables1=[df.describe().to_html(classes='data', header="true")], xcolumns=time_recommendations, columns=df.columns.to_list(), x_axis_datatype_error="Time axis data type is invalid (Should have been Datetime)", datatype_list=list_time_data_type)     #Invalid X axis data type error (Should have been Numerical)
            df[x_axis_plot] = pd.to_datetime(df[x_axis_plot], format='%S')
    except dateutil.parser._parser.ParserError:
        return render_template('analyser.html',  tables=[df.head(5).to_html(classes='data', header="true")], tables1=[df.describe().to_html(classes='data', header="true")], xcolumns=time_recommendations, columns=df.columns.to_list(), x_error="Time axis data is invalid", datatype_list=list_time_data_type)     #Invalid X axis data error (Time axis can't be parsed)
    if any(item == 'O' for item in df[y_axis_plot].dtypes.to_list()):
        return render_template('analyser.html',  tables=[df.head(5).to_html(classes='data', header="true")], tables1=[df.describe().to_html(classes='data', header="true")], xcolumns=time_recommendations, columns=df.columns.to_list(), y_error="One or more selected y axis data is object type and not plottable!",  datatype_list=list_time_data_type)     #Invalid Y axis data error
    if y_filter != 'None' and not filter_trigger:
        if df[y_filter].dtype == 'O':
            filter_trigger = True
            filtered_column = y_filter
            filtered_data_list = list(df[y_filter].unique())
            return render_template('analyser.html',  tables=[df.head(5).to_html(classes='data', header="true")], tables1=[df.describe().to_html(classes='data', header="true")], xcolumns=time_recommendations, columns=df.columns.to_list(),  datatype_list=list_time_data_type, many_to_one=1, data_filter=filtered_data_list)
        else:
            return render_template('analyser.html',  tables=[df.head(5).to_html(classes='data', header="true")], tables1=[df.describe().to_html(classes='data', header="true")], xcolumns=time_recommendations, columns=df.columns.to_list(), filter_error="Filter by option is not a string!!", datatype_list=list_time_data_type)
    if filter_trigger:
        selected_filtered_data = request.form.getlist('data_filter')
        for i in selected_filtered_data:
            list_df.append(df[df[filtered_column] == i])
    else:
        list_df = [df]
    return redirect(url_for('visualizer'))
##############

###Uploader###
@app.errorhandler(413)
def too_large(e):
    return "File is too large", 413

@app.route('/')
def uploader():
    return render_template('uploader.html')

def get_data_from_URL(URL):
    url = URL
    html = requests.get(url).content
    try:
        df_list = pd.read_html(html)
    except:
        return "Cannot be parsed. Please provide a different URL"
    df = df_list[-1]
    df.to_csv('./'+app.config['UPLOAD_PATH']+'/data.csv')
    return "Okay"

def get_data_from_Drive(Drive):
    import gdown
    output = app.config['UPLOAD_PATH']+'/data.csv'
    gdown.download(Drive,output, quiet=False) 

def upload_url(link):
    return get_data_from_URL(link)

def upload_file(uploaded_file):
    global file_ext
    uploaded_file.seek(0,2)
    size = uploaded_file.tell()
    uploaded_file.seek(0)
    if size > 50*1024*1024:                                                   
        return "Uploaded file is greater than 50 MB"                           #Upload file size limit error
    if filename != '':
        file_ext = os.path.splitext(filename)[1]
        if file_ext not in app.config['UPLOAD_EXTENSIONS']:
            return "Invalid file!"                                             #Invalid file type error
        uploaded_file.save(os.path.join(app.config['UPLOAD_PATH'], filename))
    return "Okay"

@app.route('/', methods=['POST'])
def upload():
    global filename
    global file_ext
    uploaded_file = request.files['file']
    url_link = request.form.get('url_text')
    filename = secure_filename(uploaded_file.filename)
    if filename == '' and url_link =='':
        return render_template('uploader.html', no_file_error="No Input was given!!! Please select a file or give a url.")
    if filename != '' and url_link !='':
        return render_template('uploader.html', no_file_error="Both inputs were selected!!! Please select a file or give a url.")
    if filename != '':
        msg = upload_file(uploaded_file)
        if msg != 'Okay':
            return render_template('uploader.html', no_file_error=msg)
    elif url_link != '':
        msg = upload_url(url_link)
        if msg == 'Okay':
            file_ext = '.csv'
            filename = 'data.csv'
        else:
            return render_template('uploader.html', no_file_error=msg)
    return redirect(url_for('analyser'))


##############

if __name__ == '__main__':
    app.secret_key = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'
    app.run(port=8000)