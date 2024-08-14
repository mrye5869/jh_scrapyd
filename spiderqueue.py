import redis

from jh_scrapyd.common.jh_queue import RedisQueue
from jh_scrapyd import debug_log, is_unified_queue


class JsonRedisPriorityQueue(object):

    def __init__(self, config, project, table='default'):
        # 队列参数
        self.config = config
        self.project = project
        self.table = table
        # 更新队列
        self.queue = None
        self.update_queue()

    def add(self, name, priority=0.0, **spider_args):
        d = spider_args.copy()
        d['name'] = name
        # 补充字段
        d['_project'] = self.project
        # 写入
        self.put(d, priority)

    def put(self, message, priority):
        # 调试日志
        debug_log(message, title='队列put方法调度')

        return self.queue.put(self.project, message['_job'], message, float(priority))

    def pop(self):
        # 调试日志
        debug_log('project:', self.project, title='队列pop方法调度')

        return self.queue.pop(self.project)

    def count(self):
        # 个数
        c = self.queue.count(self.project)
        # 调试日志
        debug_log('count:', c, title='队列count方法调度')

        return c

    def list(self, count: int = -1):
        # 调试日志
        debug_log('project:', self.project, title='队列list方法调度')

        return self.queue.list(self.project, True, count)

    def remove(self, func):
        # 调试日志
        debug_log('project:', self.project, title='队列remove方法调度')

    def clear(self):
        # 调试日志
        debug_log('project:', self.project, title='队列clear方法调度')

        self.queue.clear(self.project)

    def cancel(self, jobid):
        # 调试日志
        debug_log('project:', self.project, jobid, title='队列cancel方法调度')

        return self.queue.remove(self.project, jobid)

    def update_queue(self):
        section = self.config.SECTION
        # 获取redis配置
        self.config.SECTION = 'jh_scrapyd'
        conf = {
            'host': self.config.get('host', 'localhost'),
            'port': self.config.getint('port', 6379),
            'db': self.config.getint('db', 0)
        }
        password = self.config.get('password')
        if password:
            conf['password'] = password
        redis_obj = redis.StrictRedis(
            **conf
        )
        # 获取表名称
        table = self.config.get('queue_prefix', self.table)
        # 创建队列对象
        self.queue = RedisQueue(redis_obj, table, is_unified_queue())
        # 恢复配置分组
        self.config.SECTION = section