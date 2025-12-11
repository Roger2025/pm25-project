import pymysql
import requests
import os
from dotenv import load_dotenv

load_dotenv()

table_str = """
create table if not exists pm25(
id int auto_increment primary key,
site varchar(25),
count varchar(50),
pm25 int,
datacreationdate datetime,
itemunit varchar(20),
unique key site_time (site,datacreationdate)
)
"""

url = "https://data.moenv.gov.tw/api/v2/aqx_p_02?api_key=4c89a32a-a214-461b-bf29-30ff32a61a8a&limit=1000&sort=datacreationdate%20desc&format=JSON"

sqlstr = "insert ignore into pm25 (site,count,pm25,datacreationdate,itemunit)\
    values(%s,%s,%s,%s,%s)"

conn, cursor = None, None


import os
import pymysql


def open_db():
    global conn, cursor
    try:
        conn = pymysql.connect(
            host=os.environ["MYSQL_HOST"],
            user=os.environ["MYSQL_USER"],
            password=os.environ["MYSQL_PASSWORD"],
            port=int(os.environ["MYSQL_PORT"]),
            database=os.environ["MYSQL_DB"],
            charset="utf8mb4",
            cursorclass=pymysql.cursors.Cursor,
            ssl={"ssl": {}},  # Aiven 需要 SSL
        )

        cursor = conn.cursor()
        cursor.execute(table_str)
        conn.commit()
        print("資料庫開啟成功!")

    except Exception as e:
        print("資料庫開啟失敗：", e)


def close_db():
    if conn is not None:
        conn.close()
        print("資料庫關閉成功!")


def get_open_data():
    res = requests.get(url, verify=False)
    datas = res.json()["records"]
    values = [list(data.values()) for data in datas if list(data.values())[2] != ""]
    return values


def write_to_sql():
    try:
        values = get_open_data()
        if len(values) == 0:
            print("目前無資料")
            return

        size = cursor.executemany(sqlstr, values)
        conn.commit()
        print(f"寫入{size}筆資料成功!")
        return size
    except Exception as e:
        print(e)

    return 0


def write_data_to_mysql():
    try:
        open_db()
        size = write_to_sql()
        return {"結果": "success", "寫入筆數": size}
    except Exception as e:
        print(e)

        return {"結果": "failure", "message": str(e)}
    finally:
        close_db()


def get_data_from_mysql():
    try:
        open_db()
        # sqlstr = "select max(datacreationdate) from pm25;"
        # cursor.execute(sqlstr)
        # max_date = cursor.fetchone()
        # print(max_date)

        sqlstr = (
            "select site,count,pm25,datacreationdate,itemunit "
            "from pm25 "
            "where datacreationdate=(select max(datacreationdate) from pm25);"
        )
        cursor.execute(sqlstr)
        datas = cursor.fetchall()
        # 去掉ID
        # datas = [data[1:] for data in datas]

        # 取得不重複縣市名稱
        sqlstr = "select distinct count from pm25;"
        cursor.execute(sqlstr)
        counts = [count[0] for count in cursor.fetchall()]

        return datas, counts
    except Exception as e:
        print(e)
    finally:
        close_db()

    return None


def get_avg_pm25_from_mysql():
    try:
        open_db()
        sqlstr = """
        select count,round(avg(pm25),2) from pm25 group by count;
        """
        cursor.execute(sqlstr)
        datas = cursor.fetchall()

        return datas
    except Exception as e:
        print(e)
    finally:
        close_db()

    return None


def get_pm25_by_count(count):
    try:
        open_db()
        sqlstr = """
        select site,pm25,datacreationdate from pm25 
        where count=%s and datacreationdate=(select max(datacreationdate) from pm25)
        """
        cursor.execute(sqlstr, (count,))
        datas = cursor.fetchall()

        return datas
    except Exception as e:
        print(e)
    finally:
        close_db()

    return None


if __name__ == "__main__":
    # write_data_to_mysql()
    # print(get_avg_pm25_from_mysql())
    # print(get_pm25_by_count("臺中市"))
    print(get_data_from_mysql())
