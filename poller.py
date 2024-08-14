from twisted.internet.defer import DeferredQueue, inlineCallbacks, maybeDeferred, returnValue
from zope.interface import implementer

from scrapyd.interfaces import IPoller
from jh_scrapyd.common import get_spider_queues
from jh_scrapyd import is_unified_queue



@implementer(IPoller)
class QueuePoller(object):

    def __init__(self, config):
        self.config = config
        self.update_projects()
        self.dq = DeferredQueue()

    @inlineCallbacks
    def poll(self):
        if not self.dq.waiting:
            return
        for p, q in self.queues.items():
            c = yield maybeDeferred(q.count)
            if c:
                msg = yield maybeDeferred(q.pop)
                if msg is not None:  # In case of a concurrently accessed queue
                    returnValue(self.dq.put(self._message(msg, p)))

    def next(self):
        return self.dq.get()

    def update_projects(self):
        self.queues = get_spider_queues(self.config)

    def _message(self, queue_msg, project):
        d = queue_msg.copy()
        # TODO Unified queue processing
        if not is_unified_queue():
            # Non-uniform queue
            d['_project'] = project
        d['_spider'] = d.pop('name')
        return d
