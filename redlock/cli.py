from __future__ import print_function

import argparse
import datetime
import functools
import logging
import re
import signal
import subprocess
import sys
import textwrap
import time
import redlock

log = logging.getLogger('redlock.cli')
logging.basicConfig(level=logging.INFO)
state = {
    'running': True
}


def time_ms():
    return time.time() * 1000.0


def run_lock(redis, name, key, validity, retry_delay, timeout, force, **_):
    lock_value = key or redlock.get_unique_id()

    dlm = redlock.Redlock(redis)
    t0 = time_ms()
    while True:
        try:
            lock = dlm.lock(name, lock_value, validity, force=force)
            print('Locked name:%s, key:%s, validity:%s' % (name, lock.key, validity))
            return 0
        except redlock.MultipleRedlockException:
            if timeout < 0 or time_ms() < (t0 + timeout):
                time.sleep(retry_delay / 1000.0)
            else:
                log.info('Lock timeout')
                return 1



def run_unlock(redis, name, key, **_):
    try:
        dlm = redlock.Redlock(redis)
        lock = redlock.Lock(0, name, key)
        dlm.unlock(lock)
    except (redlock.CannotObtainLock, redlock.MultipleRedlockException) as e:
        log.error('Error: %s', e)
        return 3

    log.info("ok")
    return 0


def run_command(redis, name, key, validity, retry_delay, cmd, termseq, restart_cmd, **_):
    termseq = parse_termseq(termseq)

    dlm = redlock.Redlock(redis)
    lock_value = key or redlock.get_unique_id()


    def get_lock():
        log.debug('Polling for lock name:%s, key:%s', name, lock_value)
        t0 = datetime.datetime.now()
        lock = None
        while not lock and state['running']:
            try:
                lock = dlm.lock(name, lock_value, validity)
                log.debug('Got lock name:%s, key:%s in %s', name, lock_value, datetime.datetime.now() - t0)
                return lock
            except redlock.MultipleRedlockException:
                time.sleep(retry_delay / 1000.0)

    def run_proc_with_lock(proc, lock):
        """
        Run proc, until finished or lock is lost
        """
        t0 = time_ms()
        while lock:
            while time_ms() < (t0 + lock.validity * 0.5):
                delta_t = (t0 + lock.validity * 0.5) - time_ms()
                sleep_time = min(
                    delta_t,
                    lock.validity * 0.1,
                    100.0
                )
                time.sleep(sleep_time / 1000.0)
                return_code = proc.poll()
                if isinstance(return_code, int):
                    log.info('Process exited with exit code %s', return_code)
                    log.info('Release lock name:%s, key:%s', name, lock_value)
                    dlm.unlock(lock)
                    return return_code

            # subprocess still running, extend lock
            t0 = time_ms()
            try:
                log.debug('Extend lock name:%s, key:%s, validity:%s', name, lock_value, validity)
                lock = dlm.lock(name, lock_value, validity)
            except redlock.MultipleRedlockException:
                lock = None

        # lost lock, kill subprocess
        log.info('Lost lock name:%s', name)
        return terminate_proc(proc, termseq)

    def terminate_proc(proc, termseq):
        # first check if the subprocess has already exited
        return_code = proc.poll()
        if isinstance(return_code, int):
            return return_code

        # run through termseq
        for sig, timeout in termseq:
            t0 = time_ms()
            logging.debug('Send signal %s to pid %s, wait shutdown for %sms', sig, proc.pid, int(timeout))
            proc.send_signal(sig)
            while (t0 + timeout) > time_ms():
                time.sleep(0.05)
                return_code = proc.poll()
                if isinstance(return_code, int):
                    return return_code

    def sighandler(signum, _, proc):
        if signum in (signal.SIGINT, signal.SIGTERM):
            state['running'] = False
            terminate_proc(proc, termseq)

    def __inner():
        lock = get_lock()
        proc = subprocess.Popen(cmd)
        log.info('Run [%s] %s', proc.pid, ' '.join(cmd))
        handler = functools.partial(sighandler, proc=proc)
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        return run_proc_with_lock(proc, lock)

    if restart_cmd:
        while state['running']:
            __inner()
    else:
        return __inner()




def parse_termseq(termseq_str):
    assert isinstance(termseq_str, str)

    result = []
    for item in re.split(r', *', termseq_str):
        term_spec = item.split(':', 1)
        if len(term_spec) == 2:
            sig_name, timeout = term_spec
        else:
            sig_name, timeout = term_spec[0], 0
        if not hasattr(signal, 'SIG%s' % sig_name):
            raise ValueError('Invalid signal name %s' % sig_name)
        sig = getattr(signal, 'SIG%s' % sig_name)
        result.append((sig, float(timeout)))
    return result


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--redis", action="append", default=None, metavar="URL",
        help="Redis URL (eg. redis://localhost:6379/0"
    )
    parser.add_argument(
        "--verbose", action="store_true", default=False,
        help="Show debug log"
    )

    subparsers = parser.add_subparsers(help='See command help via `%(prog)s <command> --help`')
    parser_lock = subparsers.add_parser('lock', help='Acquire a lock', description="""
        For non-blocking behaviour, set --timeout=0.
        For infinitely blocking behaviour with retries every second, set --timeout=-1 and --retry-delay=1000.
    """)
    parser_lock.set_defaults(func=run_lock)
    parser_lock.add_argument("--timeout", type=int, default=0, help="Timeout for acquiring the lock, -1 for infinite")
    parser_lock.add_argument("--retry-delay", type=int, default=200, help="Milliseconds between retries")
    parser_lock.add_argument("--name", required=True, help="Lock resource name")
    parser_lock.add_argument("--key", help="Lock resource value")
    parser_lock.add_argument(
        "--force", action="store_true", default=False,
        help="Forcibly take over the lock, even if someone else has it. "
        "Useful for controlling commands running behind the lock."
    )
    parser_lock.add_argument(
        "--validity", required=True, type=int,
        help="Number of milliseconds the lock will be valid."
    )

    parser_unlock = subparsers.add_parser('unlock', help='Release a lock')
    parser_unlock.set_defaults(func=run_unlock)
    parser_unlock.add_argument("--name", required=True, help="Lock resource name")
    parser_unlock.add_argument("--key", help="Result returned by a prior 'lock' command")

    parser_run = subparsers.add_parser("run", help="Run command with lock")
    parser_run.set_defaults(func=run_command)
    parser_run.add_argument("--retry-delay", type=int, default=200, help="Milliseconds between retries")
    parser_run.add_argument("--name", required=True, help="Lock resource name")
    parser_run.add_argument("--key", help="Lock resource value")
    parser_run.add_argument("--validity", type=int, required=True, help="Number of milliseconds the lock will be valid.")
    parser_run.add_argument(
        "--restart-cmd", action="store_true", default=False,
        help="Run command again when lock is acquired again"
    )
    parser_run.add_argument(
        "--termseq", default="TERM:200,KILL", metavar="SEQUENCE",
        help="Termination sequence. Default: 'TERM:200,KILL'"
    )
    parser_run.add_argument("cmd", metavar="CMD", nargs='+', help="Command to run")



    args = parser.parse_args()
    if args.verbose:
        log.setLevel(level=logging.DEBUG)

    if not args.redis:
        args.redis = ["redis://localhost:6379"]

    result = args.func(**vars(args))

    sys.exit(result)

if __name__ == "__main__":
    main()
