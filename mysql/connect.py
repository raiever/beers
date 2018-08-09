import pymysql


class MySQLUtil(object):
    def __init__(self, host, user, passwd, db, port=3306, charset='UTF8', auto_commit=True, auto_connect=True):
        self.init_command = None
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = int(port)
        self.charset = charset
        self.auto_commit = auto_commit
        self.conn = None
        self.cursor = None
        if auto_connect:
            self.connect()

    def connect(self):
        if not self.cursor or not self.cursor.connection:
            print("connect()...")
            self.init_command = 'SET NAMES %s' % self.charset
            self.conn = pymysql.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, port=self.port,
                                        charset=self.charset, init_command=self.init_command, autocommit=True)  # FIXME: TEST
            # self.cursor = self.conn.cursor(pymysql.cursors.SSDictCursor)
            self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
            # self.conn.autocommit(self.auto_commit)
            print('connect() OK.')

    def execute(self, query, *args, **kwargs):
        try:
            self.cursor.execute(query, *args, **kwargs)
        except:  # pymysql.OperationalError:
            self.connect()  # reconnect
            self.cursor.execute(query, *args, **kwargs)
        return

    def select(self, query, *args, **kwargs):
        try:
            self.cursor.execute(query, *args, **kwargs)
            while True:
                row = self.cursor.fetchone()
                if not row:
                    break
                yield row
        except:  # pymysql.OperationalError:
            self.connect()  # reconnect
            self.cursor.execute(query, *args, **kwargs)
            while True:
                row = self.cursor.fetchone()
                if not row:
                    break
                yield row
        # except:
        #     traceback.print_exc()
        return

# connection = pymysql.connect(host="localhost",
#                              user="root",
#                              password="",
#                              db="beer",
#                              charset='UTF8',
#                              )

# try:
#     with connection.cursor() as cursor:
#         sql = "INSERT INTO style (style_name, description) VALUES (%s, %s)"
#         style_sample = 'American Amber / Red Ale'
#         desc_sample = 'Primarily a catch all for any beer less than a'
#         cursor.execute(sql, (style_sample, desc_sample))
#     connection.commit()
# finally:
#     pass


if __name__ == '__main__':
    input_collection = MySQLUtil(host='localhost', user='root', passwd='', db='beer', auto_connect=False)
    for row in input_collection.connect():