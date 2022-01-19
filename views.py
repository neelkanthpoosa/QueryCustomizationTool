from flask import Flask, request, jsonify, render_template
from utils import  connect_mysql,connect_redshift,connect_mongodb
#,connect_mongodb,connect_mongo
import json
app = Flask(__name__)


@app.route('/mysql', methods=['GET'])
def query_mysql():
    query = request.args.get('query', 'show tables;')
    database = request.args.get('database')
    connection = connect_mysql(host='database-1.c5ov2h84nuch.us-east-2.rds.amazonaws.com', user='admin',
                               password='Awsdbs10!', db=database)
    try:
        col_name, content, query_time = connection.run_query(query)
        result = {'col_name': col_name, 'result': content, 'query_time': query_time}
    except Exception as e:
        # print(e._str_())
        return e.__str__().split('"')[1], 404
    connection.disconnect()
    print(result['result'])
    return jsonify(result)


@app.route('/redshift', methods=['GET'])
def query_redshift():
    query = request.args.get('query', 'show tables;')
    print("query is " + query)
    database = request.args.get('database')
    print("database is " + database)
    try:
        connection = connect_redshift(host='redshift-cluster-1.ckay2097thwx.us-east-2.redshift.amazonaws.com', user='admin',
                                    password='Awsdbs10!',
                                    database=database)
        col_name, content, query_time = connection.run_query(query)
        result = {'col_name': col_name, 'result': content, 'query_time': query_time}
    except Exception as e:
        # print(e._str_())
        return e.__str__(), 404
    
    connection.disconnect()
    print(result['result'])
    return jsonify(result)


@app.route('/mongodb', methods=['GET'])
def query_mongo():
    query = request.args.get('query', 'show tables;')
    print(query)
    database = request.args.get('database')
    print(database)
    # driver='{MongoDB Unicode ODBC 1.4.2}',server='18.219.52.254',database=database,port='3307',user='neel',password='mongo123'
    print("connecting")
    try:
        connection = connect_mongodb(database)
        print("connected")
        col_name, content, query_time = connection.run_query(query)
        print(col_name)
        result = {'col_name': col_name, 'result': content, 'query_time': query_time}
    except Exception as e:
        return e.__str__().split('"')[1], 404
    
    connection.disconnect()
    print(result['result'])
    return jsonify(result)



@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

 

