import ast

from flask import Flask, jsonify, request
from flask import render_template

app = Flask(__name__)
hashtags = []
values = []


@app.route('/')
def get_chart_page():
    global hashtags, values
    hashtags = []
    values = []
    return render_template('chart.html', values=values, hashtags=hashtags)


@app.route('/refereshData')
def refresh_graph_data():
    global hashtags, values
    print("Hashtags now: ".format(hashtags))
    print("Values now: ".format(values))
    return jsonify(sHashtags=hashtags, sData=values)


@app.route("/updateData", methods=['POST'])
def update_data():
    global hashtags, values
    if not request.form or 'data' not in request.form:
        return 'error', 400
    hashtags = ast.literal_eval(request.form['hashtag'])
    values = ast.literal_eval(request.form['data'])
    print("Hashtags received: ".format(hashtags))
    print("Values now: ".format(values))

    return "Success", 201

if __name__ == '__main__':
    app.run(host='localhost', port=5001)
