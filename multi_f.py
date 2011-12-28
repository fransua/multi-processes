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


def runner (listfile, refresh, nprocs, results, procs):
    i = 0
    try:
        for line in listfile:
            i += 1
            procs[i] = {'p': Popen(line, shell=True, stderr=PIPE, stdout=PIPE),
                        'cmd': line, 't': time()}
            if len (procs) < nprocs:
                continue
            while len (procs) >= nprocs:
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

def main():
    """
    main function
    """
    opts = get_options()
    nprocs  = int (opts.nprocs)
    refresh = int (opts.refresh)
    results = {}
    procs = {}

    t = Thread (target=runner, args=(open (opts.listfile), refresh, nprocs, results, procs))
    t.start()
    while t.is_alive():
        print '>',
        r = raw_input()
        if r=='q':
            t._Thread__stop()
            break
        if r=='f':
            print '\nDone jobs:'
            print '***********'
            print '\n| {0:^5} | {1:^25} | {2:^15} |'.format ('job #', 'start time', 'spent time')
            for j in results:
                print '| {0:<5} | {1:<25} | {2:0>2}d {3:0>2}h {4:0>2}m {5:0>2}s |'.format (str(j), results[j]['t0'],
                                                                                           *[i for i in results[j]['t']])
            print ''
        if r=='r':
            print '\nRunning jobs:'
            print '**************'
            print '\n| {0:^5} | {1:^25} | {2:^15} |'.format ('job #', 'start time', 'running time')
            for j in procs:
                print '| {0:<5} | {1:<25} | {2:0>2}d {3:0>2}h {4:0>2}m {5:0>2}s |'.format(str(j), ctime(procs[j]['t']),
                                                                                         *[i for i in timit (time()-procs[j]['t'])])
            print ''
        if r=='h':
            print '\nHelp:'
            print '******\n'
            print ' * h: help'
            print ' * f: stats about finished jobs'
            print ' * r: stats about running jobs'
            print ''
       
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
