#!/usr/bin/env python
 
"""Cheesy script to execute a given program in parallel.

Example usage: parallel_run.py -n 32 -f output_prefix sleep 1
"""
 
import argparse
import multiprocessing
import subprocess
import sys
import threading
 
# Shady global variable required because Python 2.7 can't access outer scope
# from within a function.
remaining = None
 
def parallel_run(argv, count, num_threads=None, file_prefix=None):
    """Execute a program a given number of times.

    Note that I tried to use multiprocess.Pool, but it has a bug that blocks ctrl-c
    on Python 2.7.  Sorrow.

    http://bugs.python.org/issue8296
    """
 
    global remaining
    remaining = count
    cv = threading.Condition()
 
    def run_loop():
        global remaining
        out = None
 
        while True:
            with cv:
                remaining -= 1
                if remaining < 0:
                    return
                i = remaining
 
            if file_prefix:
                out = open('%s.%d' % (file_prefix, i), 'w+')
 
            subprocess.check_call(argv, stdout=out)
 
            if out:
                out.close()
 
    if num_threads is None:
        num_threads = min(count, multiprocessing.cpu_count())

    print 'Running %d invocations across %d threads, each executing "%s"' % (
        count, num_threads,' '.join(argv))
 
    thds = [threading.Thread(target=run_loop) for i in range(num_threads)]
 
    for t in thds:
        t.start()
    for t in thds:
        t.join()
 
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Repeated execute a program N times.')
    parser.add_argument('-n', '--repeats', type=int, help='Number of executions', default=16)
    parser.add_argument('-f', '--prefix', type=str, help='File prefix', default=None)
    parser.add_argument('-t', '--threads', type=int, help='Number of threads', default=None)

    parser.add_argument('argv', help='Program arguments.', nargs='+')
 
    args = parser.parse_args()
    parallel_run(args.argv, args.repeats, num_threads=args.threads, file_prefix=args.prefix)
