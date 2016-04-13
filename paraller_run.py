#!/usr/bin/env python
"""
This class help run batches of commands

Published by seanow @ github, with lots of helps from the internet
"""
import json
import logging
import math
import sys
import subprocess
import shlex
import time

from multiprocessing import Process, Manager, Queue
import Queue as ori_Queue

TIMEOUT = -1  # < 0 means no timeout (wait for ever)
MAX_RETRY_TIMES = 2
RETRY_INTERVAL = 5
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("lib.run_cmd")


class ParallelRunManager(object):

    """
    Run multiple commands in parallel

    self.cmd_id store a list of cmd id, use it to get the cmd & callback
        of each command
    """

    def __init__(self, callback=None):
        self._cmd_id = []
        self._cmds = {}
        self._callbacks = {}
        self._tags = {}

        # Sometimes we need to parallel run functions, not commands
        self._functions = {}
        self._args = {}
        self._kwargs = {}

        # The last auto generated id.
        self._last_auto_id = 0

        self._post_callback = callback

    def add_cmd(self, cmd, callback=None, tag=None, cmd_id=None):
        if cmd_id is None:
            cmd_id = self._new_id()

        if cmd_id in self._cmd_id:
            raise KeyError("The cmd with this ID already exists: {}".format(
                cmd_id))

        self._cmds[cmd_id] = cmd
        self._callbacks[cmd_id] = callback
        self._tags[cmd_id] = tag
        self._cmd_id.append(cmd_id)

    def add_function(self, function, args=None, kwargs=None, callback=None,
                     tag=None, cmd_id=None):
        if cmd_id is None:
            cmd_id = self._new_id()

        if cmd_id in self._cmd_id:
            raise KeyError("The cmd with this ID already exists: {}".format(
                cmd_id))

        self._functions[cmd_id] = function
        self._args[cmd_id] = args
        self._kwargs[cmd_id] = kwargs
        self._callbacks[cmd_id] = callback
        self._tags[cmd_id] = tag
        self._cmd_id.append(cmd_id)

    def list_cmd_id(self):
        return self._cmd_id

    def list_cmd(self):
        cmd_list = []
        for cmd_id in self._cmd_id:
            cmd_list.append(self._cmds.get(cmd_id, None))

        return cmd_list

    def list_function(self):
        func_list = []
        for cmd_id in self._cmd_id:
            func_list.append(self._functions.get(cmd_id, None))

        return func_list

    def list_all_detail(self):
        results = []
        for cmd_id in self._cmd_id:

            result = {
                'cmd_id': cmd_id,
                'cmd': self._cmds.get(cmd_id, None),
                'callback': str(self._callbacks.get(cmd_id, None)),
                'tag': self._tags.get(cmd_id, None),
                'functions': str(self._functions.get(cmd_id, None)),
                'args': self._args.get(cmd_id, None),
                'kwargs': self._kwargs.get(cmd_id, None)
            }

            results.append(result)
        return results

    def execute(self, num_workers=None,
                job_title='tasks',
                max_retry_times=MAX_RETRY_TIMES,
                timeout=TIMEOUT,
                retry_interval=RETRY_INTERVAL,
                progress_bar=True):

        if len(self._cmd_id) == 0:
            return

        if num_workers is None:
            num_workers = len(self.list_cmd_id())

        print "Start running {} ({} missions)".format(job_title,
                                                      len(self._cmd_id))

        finished_order_list = self._execute(
            num_workers, job_title, max_retry_times, timeout, retry_interval,
            progress_bar)

        for cmd_id in self._cmd_id:
            tag = self._tags.get(cmd_id, None)
            callback = self._callbacks.get(cmd_id, None)

            finished_order = [order for order in finished_order_list
                              if order['cmd_id'] == cmd_id][0]
            response = finished_order['response']
            exception = finished_order['exception']

            # Try execute specified callback, then general (default) callback
            callback_para = {}
            keywords = ['cmd_id', 'response', 'exception', 'tag']
            for key in keywords:
                callback_para[key] = locals().get(key, None)

            if callback is not None:
                callback(**callback_para)

            if self._post_callback is not None:
                self._post_callback(**callback_para)

    def _execute(self, num_workers, job_title, max_retry_times, timeout,
                 retry_interval, progress_bar):
        """
        Input: integrated work order
            [{'cmd_id': $ID1, 'cmd': $CMD1}, ...]

        Output: integrated work order with response and excpetion (optional)
            [{'cmd_id': $ID1, 'cmd': $CMD1, 'response': $RESPONSE1,
              'exception': $EXCEPTION1}, ...]
        """
        TOTAL_CMD_NUM = len(self._cmd_id)

        # create integrated work order
        order_queue = Queue()
        for cmd_id in self._cmd_id:

            order = {'cmd_id': cmd_id,
                     'cmd': self._cmds.get(cmd_id, None),
                     # function marked due to cannot be pickled
                     # 'function': self._functions.get(cmd_id, None),
                     'args': self._args.get(cmd_id, None),
                     'kwargs': self._kwargs.get(cmd_id, None),
                     'tag': self._tags.get(cmd_id, None)
                     }
            order_queue.put(order)

        try:
            if order_queue.qsize() != TOTAL_CMD_NUM:
                raise Exception(
                    "Amount of cmds and orders in the queue not match")
        except NotImplementedError:
            pass

        finished_order_list = Manager().list()

        # start for workers
        pool = []

        if progress_bar:
            managing_process = Process(
                target=self.worker_hr, args=(
                    finished_order_list, TOTAL_CMD_NUM, job_title))
            managing_process.start()

        for i in xrange(num_workers):
            p = Process(
                target=self.worker, args=(order_queue, finished_order_list,
                                          max_retry_times, timeout,
                                          retry_interval))
            p.start()
            pool.append(p)

        if progress_bar:
            pool.append(managing_process)

        for p in pool:
            p.join()

        if TOTAL_CMD_NUM != len(finished_order_list):
            raise Exception(("Incomming jobs ({}) and finished_order_list "
                             "numbers ({}) not matched!".format(
                                 TOTAL_CMD_NUM, len(finished_order_list))))

        sys.stdout.write('\n')
        sys.stdout.flush()

        # Do not check if the command fail, leave that to callback function
        return finished_order_list

    def worker_hr(self, finished_order_list, TOTAL_CMD_NUM, job_title):
        """Draw the progress bar"""

        i = 0
        while True:

            # exit signal
            result_cnt = len(finished_order_list)
            curr_progress = float(result_cnt) / TOTAL_CMD_NUM * 100

            sys.stdout.write('\r')
            sys.stdout.write(
                "%s [%-25s] %03.2f%% finished (%ss passed)" %
                (job_title, '=' * int(math.floor(curr_progress / 4)),
                    curr_progress, i))
            sys.stdout.flush()

            if result_cnt == TOTAL_CMD_NUM:
                return

            i += 1
            time.sleep(1)

    def worker(self, order_queue, finished_order_list, max_retry_times,
               timeout, retry_interval=0):
        """
        Keep taking order from order_queue, exit when order_queue is empty

        Take a work order from the order_queue:
            {'cmd_id': $ID1, 'cmd': $CMD1, 'function':$FUNC1,
             'parameter':$PARA1}

        For each order, check if it is a command or function and run

        Then add a finished order to the finished_order_list:
            {'cmd_id': $ID1, 'cmd': $CMD1, 'function':$FUNC1,
             'parameter':$PARA1 'response': $RESPONSE1,
             'exception': $EXCEPTION1}
        """

        while True:
            # print "worker have que_size: %s "%order_queue.qsize()

            # exit signal
            try:
                order = order_queue.get_nowait()
            except ori_Queue.Empty:
                return

            resp_exception = None

            if 'cmd' in order and order['cmd'] is not None:
                readable_cmd = order['cmd']
                cmd = shlex.split(readable_cmd)
                function = subprocess.check_output
                args = None
                kwargs = {'args': cmd, 'stderr': subprocess.STDOUT,
                          'universal_newlines': True}

            elif 'args' in order and order['args'] is not None:
                function = self._functions[order['cmd_id']]
                args = order['args']
                kwargs = order['kwargs']
            else:
                raise Exception("Worker found a work order with nither cmd "
                                "or function assigned, order detail: "
                                "{}".format(json.dumps(order)))

            response = ""
            resp_exception = None
            for i in range(max_retry_times + 1):
                try:

                    # Run the task in another process to handle timeout
                    resp_que = Queue()
                    proc = Process(
                        target=self.run_function,
                        name="I am working order %s" % order['cmd_id'],
                        kwargs={
                            'function': function,
                            'args': args,
                            'kwargs': kwargs,
                            'resp_que': resp_que
                        }
                    )
                    proc.start()
                    if int(timeout) > 0:
                        proc.join(timeout)
                    else:
                        proc.join()

                    if proc.is_alive():
                        proc.terminate()
                        proc.join()
                        raise Exception(
                            "Timeout after {} seconds, task tag is: {}".format(
                                timeout, order['tag']))

                    rtn_code, response = resp_que.get()
                    if rtn_code != 0:
                        print "parallelr_run error: ", Exception(response)
                        raise Exception(response)

                    break

                except subprocess.CalledProcessError as excp:
                    if i == max_retry_times:
                        msg = ("[ERROR] cmd '{0}' failed, exception: '{1}',"
                               "failed.".format(readable_cmd, excp.output))
                        # print msg
                        response = msg
                        resp_exception = excp
                    else:
                        msg = ("[WARNING] cmd '{0}' failed, found exception "
                               "'{1}', try again in 5 sec.".format(
                                   readable_cmd, excp.output
                               ))

                        # print msg
                        time.sleep(5)
                        response = msg
                        resp_exception = excp

                except Exception as err:
                    resp_exception = err

            # Create a finished order
            finished_order = order.copy()
            finished_order['response'] = response
            if resp_exception is not None:
                resp_exception = str(resp_exception)
            finished_order['exception'] = resp_exception

            finished_order_list.append(finished_order)
            time.sleep(float(retry_interval))

    def run_function(self, function, args, kwargs, resp_que):
        """
        A function wrapper that help get the return value in the subprocess
        """
        try:
            if args is not None and kwargs is not None:
                resp_que.put((0, function(*args, **kwargs)))
            elif args is None:
                resp_que.put((0, function(**kwargs)))
            elif kwargs is None:
                resp_que.put((0, function(*args)))
            else:
                raise Exception(
                    "Either args ({}) or kwargs ({}) needs to be "
                    "assign to function {}, but both are None".format(
                        args, kwargs, function))
        except subprocess.CalledProcessError as excp:
            print "subprocess error at lib.parallel_run.run_function():"
            " return code={}; output=[{}]; error=[{}]", excp.returncode,
            excp.output, excp
            resp_que.put((1, excp))

        except Exception as excp:
            msg = ("Exception '{excp}' at lib.parallel_run.run_function(): "
                   "{function} ran into unexpected error, "
                   " detail: {detail}".format(
                       excp=excp, function=function, detail=excp.__doc__))
            """
            msg += ("{function}({args}, {kwargs}) ran into unexpected error, "
                    " detail: {detail}".format(
                        excp=excp, function=function, args=args, kwargs=kwargs,
                        detail=excp.__doc__))
            """
            print msg
            import traceback
            traceback.print_exc()
            resp_que.put((1, excp))

    # private helpers ------------------------------
    def _new_id(self):
        """Create a new id.
        Auto incrementing number that avoids conflicts with ids already used.
        Returns:
        string, a new unique id.
        """
        self._last_auto_id += 1
        while str(self._last_auto_id) in self._cmd_id:
            self._last_auto_id += 1
        return str(self._last_auto_id)


if __name__ == '__main__':
    """
    ParallelRunManager allows you to run multiple cmds/functions
        at the same time, you can even mix them while running!

    usage:
        new an instance with or without callback assigned:
        >>> runcmd_mgr = ParallelRunManager(callback)

        add a cmd with or without specified callback (optional):
        >>> runcmd_mgr.add_cmd('docker ps', tag='ccloud')

        add a fucntion with or without specified callback (optional):
        >>> runcmd_mgr.add_function(add, [3, 5], tag='function_add',
                                    callback=callback_add)

        execute them:
        >>> runcmd_mgr.execute()

        The result will be stored in the given callback function
    """

    result = []

    def callback(*args, **kwargs):
        result.append({'cmd_id': kwargs.get('cmd_id', None),
                       'response': kwargs.get('response', None),
                       'exception': kwargs.get('exception', None),
                       'tag': kwargs.get('tag', None)})

    def callback_add(*args, **kwargs):
        result.append({'cmd_id': kwargs.get('cmd_id', None),
                       'response': kwargs.get('response', None),
                       'exception': kwargs.get('exception', None),
                       'tag': kwargs.get('tag', None)})

    def add(num_a, num_b):
        return num_a + num_b

    runcmd_mgr = ParallelRunManager(callback)
    runcmd_mgr.add_cmd('ls', tag={'cmd': 'ls'})
    runcmd_mgr.add_cmd('ls -alh', tag={'cmd': 'ls -alh'})
    runcmd_mgr.add_cmd('df -h', tag={'cmd': 'df -h'})

    runcmd_mgr.add_function(add, [3, 5], tag={'function': 'add',
                                              'paras': [3, 5]},
                            callback=callback_add)

    import time
    runcmd_mgr.add_function(time.sleep, [3288], tag={'function': 'time.sleep',
                                                     'paras': [3288]},
                            callback=callback_add)

    print "List result:\n", json.dumps(runcmd_mgr.list_all_detail(), indent=2)

    runcmd_mgr.execute(max_retry_times=1, timeout=5, progress_bar=False)
    print json.dumps(result, indent=2)
