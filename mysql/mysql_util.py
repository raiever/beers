import traceback
import time
import pymysql

# noinspection PyShadowingNames
from crawlers.config import MYSQL


class MySQLUtil(object):
    def __init__(self, host, user, passwd, db, port=3306, charset='UTF8', auto_commit=True, auto_connect=True):
        self.init_command = None
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = int(port)
        self.charset = charset.replace('-', '').upper()
        self.auto_commit = auto_commit
        self.conn = None
        self.cursor = None
        if auto_connect:
            self.connect()

    def connect(self):
        if not self.cursor or not self.cursor.connection:
            # print('connect()...')
            self.init_command = 'SET NAMES %s' % self.charset
            self.conn = pymysql.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, port=self.port,
                                        charset=self.charset, init_command=self.init_command, autocommit=True)  # FIXME: TEST
            # self.cursor = self.conn.cursor(pymysql.cursors.SSDictCursor)
            self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
            # self.conn.autocommit(self.auto_commit)
            # print('connect() OK.')

    def __repr__(self):
        return '%s@%s:%s/%s' % (self.user, self.host, self.port, self.db)

    def __del__(self):
        if self.cursor:
            self.conn.close()

    # def __check_connection(self):
    #     try:
    #         self.cursor.execute("SELECT 1")
    #     except (AttributeError, pymysql.OperationalError):
    #         self.connect()

    def affected_rows(self):
        # when using 'insert .. on duplicate key update ..'
        # 0 if an existing row is set to its current values
        # 1 if the row is inserted as a new row
        # 2 if an existing row is updated
        return self.conn.affected_rows()

    @property
    def rowcount(self):
        return self.cursor.rowcount

    @staticmethod
    def addslashes(field):
        return field.replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"')

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


if __name__ == '__main__':
    field = ''' \\ ' " '''
    print(field)
    print(MySQLUtil.addslashes(field))

    start = time.time()
    # db = MySQLUtil(host='1.2.3.4', user='user', passwd='passwd', db='db_name', port=3306, charset='utf8', )
    try:
        # input_collection = MySQLUtil(host='localhost', user='root', passwd='user_passwd', db='wikipedia_korean', port=3306, charset='utf8', auto_connect=False)
        input_collection = MySQLUtil(host=MYSQL['host'], user=MYSQL['user'], passwd=MYSQL['passwd'], db=MYSQL['db']['kr_news'], port=MYSQL['port'], charset=MYSQL['charset'], auto_connect=False)
        for row in input_collection.select('SELECT prefix_url FROM news_doc_rule LIMIT 10'):
            print(row)
    except:
        traceback.print_exc()
    print('OK')
    print(time.time() - start)
    # for row in db.execute("select pncode, region from region_pncode order by pncode"):
    #     pn = str(row['pncode'])
    # db.execute("""insert into room(room_id, name, last_message_id) values(%s, %s, %s)""", room_id, name, last_message_id)
