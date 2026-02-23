from flask import Flask, jsonify, request
import random

app = Flask(__name__)


@app.route("/random", methods=["GET"])
def random_number():
    min_val = int(request.args.get("min", 0))
    max_val = int(request.args.get("max", 100))
    value = random.randint(min_val, max_val)
    return jsonify({"random_number": value})


@app.route("/", methods=["GET"])
def index():
    return "Data Service running on datatailr!\n"


@app.route("/__health_check__.html", methods=["GET"])
def health_check():
    return "OK\n"


@app.route("/greet", methods=["GET"])
def greet():
    name = request.args.get("name", "World")
    return jsonify({"greeting": f"Hello, {name} from Data Service!"})


def main(port):
    app.run("0.0.0.0", port=int(port), debug=False)


if __name__ == "__main__":
    main(1024)
