import os
from typing import Optional, Union, Tuple

from flask import Flask, request, jsonify, Response
from werkzeug.exceptions import BadRequest

from utils import get_result, get_cmd

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


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

    chunk = get_cmd(query)

    result = get_result(DATA_DIR, file_name, chunk)
    return jsonify(result)


if __name__ == '__main__':
    app.run()

