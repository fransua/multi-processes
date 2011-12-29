#!/usr/bin/python
"""
28 Dec 2011


"""

__author__  = "Francois-Jose Serra"
__email__   = "francois@barrabin.org"
__licence__ = "GPLv3"
__version__ = "0.0"

from optparse   import OptionParser
from subprocess import Popen, PIPE
from time       import sleep, time, ctime
from cPickle    import dump
from threading  import Thread
from os         import nice, system
from pprint     import pprint

import readline

def runner (listfile, refresh, nprocs, results, procs):
    i = 0
    try:
        for line in listfile:
            i += 1
            procs[i] = {'p': Popen(line, shell=True, stderr=PIPE, stdout=PIPE,
                                   preexec_fn=lambda : nice(5)),
                        'cmd': line, 't': time()}
            if len (procs) < nprocs[0]:
                continue
            while len (procs) >= nprocs[0]:
                for p in procs:
                    if procs[p]['p'].poll() is None:
                        sleep(refresh)
                        continue
                    results[p] = {'cmd': procs[p]['cmd'], 't0': ctime(procs[p]['t']),
                                  't': timit(time()-procs[p]['t'])}
                    results[p]['out'], results[p]['err'] = procs[p]['p'].communicate()
                    del (procs[p])
                    break
    except:
        print 'ERROR at', i
    print '\n\n\n\nThe End.'


def timit (t):
    return (int (((t/60/60/24))), int ((t/60/60)%24), int ((t/60)%60), int (t%60))

def in_console(t_runs, results, procs, nprocs, lenlist):
    raw = '| {0:<5} | {1:<25} | {2:0>2}d {3:0>2}h {4:0>2}m {5:0>2}s |'
    header = '\n| {0:^5} | {1:^25} | {2:^15} |'
    while 1:
        try:
            r = raw_input('> ')
        except (KeyboardInterrupt, EOFError):
            print '    X-O\n'
            r = 'q'
        if r=='q':
            if raw_input('  -> really STOP all running jobs (y|N): ')=='y':
                t_runs._Thread__stop()
                break
        if r=='c':
            p = raw_input('  -> type number of CPUs (currently: {0}): '.format(nprocs[0]))
            if p.isdigit():
                nprocs[0] = int (p)
                print 'ok\n'
            else:
                print 'not valid number\n'
        elif r=='d':
            print '\nDone jobs:'
            print '***********'
            print header.format ('job #', 'start time', 'spent time')
            for j in results:
                print raw.format (str(j), results[j]['t0'],
                                  *[i for i in results[j]['t']])
            print ''
        elif r=='r':
            print '\nRunning jobs:'
            print '**************'
            print header.format ('job #', 'start time', 'running time')
            for j in procs:
                print raw.format(str(j), ctime(procs[j]['t']),
                                 *[i for i in timit (time()-procs[j]['t'])])
            print ''
        elif r=='h':
            print '\nHelp:'
            print '******\n'
            print ' * h: help'
            print ' * d: stats about finished jobs'
            print ' * r: stats about running jobs'
            print ' * c: change number of CPUs assigned to jobs'
            print ' * w: number of waiting jobs'
            print ' * q: exit and STOP launching jobs so nicely (running jobs may finish normally, but will not appear in log.)'
            print ' * whatever python command:'
            print '    - [r for r in results if sum(results[r]["t"][:-3]) > 1]: print jobs during more then 1 minute'
            print '    - locals(): print local variables'
            print ''
        elif r=='w':
            print '\n Waiting Jobs: {0}\n'.format((lenlist - (len (results) + len (procs))))
        elif r:
            try:
                exec ('pprint (%s)'%(r))
            except:
                print ' hmmm... this is not working well\n'


def main():
    """
    main function
    """
    opts = get_options()
    nprocs  = [int (opts.nprocs)]
    refresh = int (opts.refresh)
    listfile = open (opts.listfile).readlines()
    results = {}
    procs = {}

    t_runs = Thread (target=runner, args=(listfile, refresh, nprocs, results, procs))
    t_runs.start()
    t_term = Thread (target=in_console, args=(t_runs, results, procs, nprocs, len (listfile)))
    t_term.start()

    while t_runs.is_alive() and t_term.is_alive():
        sleep(1)
    t_term._Thread__stop()

    # this is in order to repair terminal, because of bad ending of raw_input
    system('tset')

    # saving log to pickle
    dump(results, open (opts.log, 'w'))
    

def get_options():
    '''
    parse option from command line call
    '''
    parser = OptionParser(
        version=__version__,
        usage="%prog [options] file [options [file ...]]")
    parser.add_option('-i', dest='listfile', metavar="PATH",
                      help='path to infile')
    parser.add_option('-o', dest='log', metavar="PATH",
                      help='path to log-file')
    parser.add_option('-p', dest='nprocs', metavar="INT", default='4',
                      help='number of procs to use')
    parser.add_option('-r', dest='refresh', metavar="INT", default='2',
                      help='number of seconds between each job refresh')
    opts = parser.parse_args()[0]
    if not opts.listfile:
        parser.print_help()
        exit()
    if not opts.log:
        opts.log = opts.listfile + '.pik'
    return opts


if __name__ == "__main__":
    exit(main())
