from flask import Flask, render_template, Response
from datetime import datetime
from pm25 import get_data_from_mysql, write_data_to_mysql, get_avg_pm25_from_mysql
import json

books = {
    1: {
        "name": "Python book",
        "price": 299,
        "image_url": "https://im2.book.com.tw/image/getImage?i=https://www.books.com.tw/img/CN1/136/11/CN11361197.jpg&v=58096f9ck&w=348&h=348",
    },
    2: {
        "name": "Java book",
        "price": 399,
        "image_url": "https://im1.book.com.tw/image/getImage?i=https://www.books.com.tw/img/001/087/31/0010873110.jpg&v=5f7c475bk&w=348&h=348",
    },
    3: {
        "name": "C# book",
        "price": 499,
        "image_url": "https://im1.book.com.tw/image/getImage?i=https://www.books.com.tw/img/001/036/04/0010360466.jpg&v=62d695bak&w=348&h=348",
    },
}

app = Flask(__name__)


@app.route("/avg-pm25")
def get_avg_pm25():
    result = get_avg_pm25_from_mysql()
    count = [r[0] for r in result]
    pm25 = [float(r[1]) for r in result]

    return Response(
        json.dumps({"count": count, "pm25": pm25}, ensure_ascii=False),
        mimetype="application/json",
    )


@app.route("/update-db")
def update_db():
    result = write_data_to_mysql()

    return json.dumps(result, ensure_ascii=False)


@app.route("/pm25")
def get_pm25():
    values = get_data_from_mysql()
    print(values)
    columns = ["站點名稱", "縣市", "PM2.5", "更新時間", "單位"]
    return render_template("pm25.html", values=values, columns=columns)


@app.route("/")
def index():
    time = now_time()
    return render_template("index.html", time=time, name="Roger2022")


@app.route("/nowtime")
def now_time():
    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(time)
    return time


@app.route("/bmi/height=<h>&weight=<w>")
def get_bmi(h, w):
    bmi = round(eval(w) / (eval(h) / 100) ** 2, 2)

    return f"<h1>身高:{h}cm 體重:{w}kg <br>BMI:{bmi}</h1>"


@app.route("/books")
@app.route("/books/id=<int:id>")
def get_books(id=None):
    try:
        if id == None:
            return render_template("books.html", books=books)

        return render_template("books.html", books=books[id])
    except Exception as e:
        return f"書籍編號錯誤:{e}"


app.run(debug=True)
