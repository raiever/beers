import pymysql

connection = pymysql.connect(host="localhost",
                             user="root",
                             password="",
                             db="beer",
                             charset='UTF8',
                             )

try:
    with connection.cursor() as cursor:
        sql = "INSERT INTO style (style_name, description) VALUES (%s, %s)"
        style_sample = 'American Amber / Red Ale'
        desc_sample = 'Primarily a catch all for any beer less than a'
        cursor.execute(sql, (style_sample, desc_sample))
    connection.commit()
finally:
    pass


if __name__ == '__main__':
    print(connection)