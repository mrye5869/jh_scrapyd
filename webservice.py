import traceback
from twisted.python import log
from copy import copy

from scrapyd.utils import JsonResource
from scrapyd.utils import native_stringify_dict

import os
import signal



class WsResource(JsonResource):

    def __init__(self, root):
        JsonResource.__init__(self)
        self.root = root

    def render(self, txrequest):
        try:
            return JsonResource.render(self, txrequest).encode('utf-8')
        except Exception as e:
            if self.root.debug:
                return traceback.format_exc().encode('utf-8')
            log.err()
            r = {"node_name": self.root.nodename, "status": "error", "message": str(e)}
            return self.render_object(r, txrequest).encode('utf-8')


class JhCancel(WsResource):
    def render_POST(self, txrequest):
        args = {k: v[0] for k, v in native_stringify_dict(copy(txrequest.args), keys_only=False).items()}
        project = args['project']
        jobid = args['job']

        # 删除running
        _is_ok = self._rm_by_running(project, jobid, args)

        prevstate = None
        if _is_ok:
            prevstate = 'running'
        else:
            _is_ok = self._rm_by_pending(project, jobid)
            if _is_ok:
                prevstate = 'pending'

        # 进程id
        job_pid = args.get('pid')
        if job_pid:
            try:
                os.kill(int(job_pid), signal.SIGKILL)
                log.msg(f"Process {job_pid} has been terminated.")
            except Exception as e:
                log.msg(f"Unable to terminate process {job_pid}:{e}")

        return {"node_name": self.root.nodename, "status": "ok" if _is_ok else "error", "prevstate": prevstate}

    def _rm_by_running(self, project, jobid, args) -> bool:
        # 参数
        signal = args.get('signal', 'TERM')
        # 结果
        _is = False
        del_index = None
        spiders = self.root.launcher.processes
        for index in spiders:
            spider = spiders[index]
            if spider.project == project and spider.job == jobid:
                spider.transport.signalProcess(signal)
                _is = True
                del_index = index
                break
        if del_index:
            # 删除指定索引的任务
            del self.root.launcher.processes[del_index]
        return _is


    def _rm_by_pending(self, project, jobid) -> bool:
        # 创建调度对象
        queue = self.root.poller.queues[project]
        return queue.cancel(jobid)
