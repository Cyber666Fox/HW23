import os
import readline
from typing import Optional, Union, Tuple

from flask import Flask, request, jsonify, Response
from werkzeug.exceptions import BadRequest

from utils import get_result, get_cmd

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def do_query(params):
    with open(os.path.join(DATA_DIR, params["file_name"])) as f:
        file_data = f.readline()

    if params["cmd1"] == "filter":
        result = filter(lambda record:params["value1"] in record, file_data)
    elif params["cmd1"] == "map":
        col_num = int(params["value1"])
        result = map(lambda record:record.split()[col_num], file_data)
    elif params["cmd1"] == "unique":
        result = set(file_data)
    elif params["cmd1"] == "sort":
        reverse = params["value1"] == "desc"
        result = sorted(file_data, reverse=reverse)
    return result

@app.route("/perform_query", methods=['POST'])
def perform_query() -> Union[Response, Tuple[str, int]]:
    query: Optional[dict] = request.json

    try:
        file_name: str = query["file_name"]
        cmd1: str = query["cmd1"]
        value1: str = query["value1"]
        cmd2: str = query["cmd2"]
        value2: str = query["value2"]
    except KeyError:
        return " ", 400

    if not os.path.exists(os.path.join(DATA_DIR, file_name)):
        raise BadRequest

    chunk = get_cmd(query)

    result = get_result(DATA_DIR, file_name, chunk)
    return jsonify(result)


if __name__ == '__main__':
    app.run()

