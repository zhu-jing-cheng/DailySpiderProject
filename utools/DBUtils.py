import pymysql
import redis
from pymongo import MongoClient
from pymysql import IntegrityError


class MysqlUtils:
    __db = None
    __cursor = None
    __dict_cursor = None

    def __init__(self, dbconfig):
        self.__db = pymysql.connect(dbconfig['host'], dbconfig['username'], dbconfig['password'], dbconfig['db'],
                                    dbconfig['port'])
        self.__cursor = self.__db.cursor()
        self.__dict_cursor = self.__db.cursor(cursor=pymysql.cursors.DictCursor)

    def get_cursor(self):
        return self.__cursor

    def get_dict_cursor(self):
        return self.__dict_cursor

    def get_db(self):
        return self.__db

    def close(self):
        self.__db.close()

    def query(self, sql, args):
        self.__dict_cursor.execute(sql, args)
        rs = self.__dict_cursor.fetchall()
        return rs

    def multiple_insert(self, sql, data):
        try:
            self.__cursor.executemany(sql, tuple(data))
            self.__db.commit()
        except Exception as e:
            print(e)
            self.__db.rollback()


class RedisUtils:
    __pool = None
    __rclient = None

    def __init__(self, redis_config):
        self.__pool = redis.ConnectionPool(host=redis_config['host'], port=redis_config['port'], decode_responses=True,
                                           db=redis_config['db_index'])
        self.__rclient = redis.Redis(connection_pool=self.__pool)

    def close(self):
        self.__rclient.close()

    def get_client(self):
        return self.__rclient

    # data [(key, value) ... ]
    def insert_batch(self, data, prefix=''):
        if prefix != '' and prefix is not None:
            prefix += ':'
        else:
            prefix = ''

        with self.__rclient.pipeline(transaction=True) as p:
            for i in data:
                p.set(prefix + str(i[0]), i[1])

            p.execute()

    def insert_set(self, key, values):
        self.__rclient.sadd(key, *values)


class MongoUtils:
    __client = None
    __m_db = None
    __db = ''

    def __init__(self, mongo_zhishiku):
        self.__db = mongo_zhishiku['db']
        self.__client = MongoClient(mongo_zhishiku['host'], mongo_zhishiku['port'])
        self.__m_db = self.__client.admin
        self.__m_db.authenticate(mongo_zhishiku['user'], mongo_zhishiku['pwd'],
                                 mechanism=mongo_zhishiku['encrypt_algorithm'])
        # dataSet = collection.find()

    def close(self):
        self.__client.close()

    def get_collection(self, collection_name):
        return self.__client[self.__db][collection_name]
