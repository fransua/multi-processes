#!/usr/bin/python
"""
28 Dec 2011

Something to manage running a list of jobs in local but several CPUs
"""

__author__  = "Francois-Jose Serra"
__email__   = "francois@barrabin.org"
__licence__ = "GPLv3"
__version__ = "0.1"

from optparse   import OptionParser
from subprocess import Popen, PIPE
from time       import sleep, time, ctime
from cPickle    import dump, load, UnpicklingError
from threading  import Thread
from os         import nice
from pprint     import pprint
from sys        import exc_info, stdout
from Queue import Queue, Empty
# this import is to allow arrow navigation inside command line
import readline
import rlcompleter

NICE=19
    
def main():
    """
    main function
    """
    opts = get_options()
    
    nprocs  = [int (opts.nprocs)]
    refresh = float (opts.refresh)

    # parse array jobs and load/create results dict for log
    if opts.resume:
        try:
            results_o = load (open (opts.resume))
        except UnpicklingError:
            print '\nThis is not a log file...\n'
            exit()
        try:
            listfile  = results_o ['pending']
        except KeyError:
            print '\nNo pending jobs here...\n'
            listfile = []
        items = results_o.keys()
        if 'pending' in results_o:
            del (results_o['pending'])
        results = {}
        for r in xrange (len (results_o)):
            results[r+1] = results_o[items[r]]
        del (results_o)
    else:
        listfile = [line.strip() for line in open (opts.listfile).readlines() if line.strip()]
        results = {}
    procs = {}

    # Threads
    # execute array jobs
    t_runs = Thread (target=runner,
                     args  =(listfile, refresh, nprocs, results, procs))
    t_runs.start()
    # open terminal
    t_term = Thread (target=prompter,
                     args  =(t_runs, results, procs, nprocs, listfile, opts.name))
    t_term.start()

    # wait
    wait (t_runs, t_term)

    # this is in order to repair terminal, because of bad ending of raw_input (nothing better found)
    Popen('tset', shell=True).communicate()

    # saving log to pickle
    if listfile:
        results['pending'] = listfile
    dump(results, open (opts.log, 'w'))

    print bye()
    
def enqueue_output(out, queue):
    for c in iter(out.readline, ''):
        queue.put(c)
                    
def runner (listfile, refresh, nprocs, results, procs):
    '''
    where jobs are runned
    '''
    i = len (results)
    done = False
    try:
        while True:
            varz = globals()
            varz.update(locals())
            readline.set_completer(rlcompleter.Completer(varz).complete)
            readline.parse_and_bind("tab: complete")
            if listfile and len (procs)<nprocs[0]:
                line = listfile.pop(0)
                i += 1
                procs[i] = {'p': Popen(line, shell=True, bufsize=-1,
                                       stderr=PIPE, stdout=PIPE, stdin=PIPE,
                                       preexec_fn=lambda : nice(NICE)),
                            'cmd': line, 't': time(), 'status': 'running',
                            'out': '', 'err': '', 'qout': Queue(), 'qerr': Queue()}
                procs[i]['tout'] = Thread(target=enqueue_output,
                                          args=(procs[i]['p'].stdout, procs[i]['qout']))
                procs[i]['terr'] = Thread(target=enqueue_output,
                                          args=(procs[i]['p'].stderr, procs[i]['qerr']))
                procs[i]['tout'].daemon = True
                procs[i]['tout'].start()
                procs[i]['terr'].daemon = True
                procs[i]['terr'].start()
            for p in procs:
                try:
                    line = procs[p]['qout'].get_nowait() # or q.get(timeout=.1)
                except Empty:
                    pass
                else:
                    procs[p]['out'] += line
                try:
                    line = procs[p]['qerr'].get_nowait() # or q.get(timeerr=.1)
                except Empty:
                    pass
                else:
                    procs[p]['err'] += line
                            
                if procs[p]['p'].poll() is None:
                    continue
                returncode = procs[p]['p'].returncode
                if returncode == -9:
                    print ' WAHOOO!!! this was killed:'
                    print procs[p]
                    return
                while not False:
                    try:
                        line = procs[p]['qout'].get_nowait()
                    except Empty:
                        break
                    procs[p]['out'] += line
                while not False:
                    try:
                        line = procs[p]['qerr'].get_nowait()
                    except Empty:
                        break
                    procs[p]['err'] += line
                results[p] = {'cmd': procs[p]['cmd'],
                              't0': ctime(procs[p]['t']),
                              't': timit(time()-procs[p]['t']),
                              'status': str(returncode),
                              'out': procs[p]['out'],
                              'err': procs[p]['err']}
                del (procs[p])
                break
            sleep(refresh)
            if not (listfile or procs) and not done:
                print '\n\nall jobs done...'
                stdout.write('O:-) ')
                stdout.flush()
                done = True
    except Exception as e:
        print 'ERROR at', i
        print e


def timit (t):
    '''
    transform seconds to (days, hours, minutes, seconds)
    '''
    return (int (((t/60/60/24))), int ((t/60/60)%24), int ((t/60)%60), int (t%60))

def untime (ts):
    return ts[0]*60*60*24 + ts[1]*60*60 + ts[2]*60 + ts[3]

def print_cmd(cmd, w=50):
    cmd = cmd.strip()
    return (' ' + cmd if len (cmd)<w else '..'+cmd[-(w-2):])

def prompter (t_runs, results, procs, nprocs, listfile, name):
    '''
    little prompt in order to manage jobs
    '''
    w = 60
    timestr = '{0:0>2}d {1:0>2}h {2:0>2}m {3:0>2}s'
    header = '\n| {0:^5} | {1:^15} | {2:^15} | {3:^%s} | {4:^8} |'%(w-1)
    raw = '| {0:<5} | {1:<15} | {4:0>2}d {5:0>2}h {6:0>2}m {7:0>2}s |{2:<%s} | {3:^8} |'%(w)
    help_s = """
Help:
******
  * h: help
  * a: summary statistics
  * d: stats about finished jobs
  * r: stats about running jobs
  * c: change number of CPUs assigned to jobs
  * w: number of waiting jobs
  * q: exit and STOP launching jobs so nicely (running jobs may finish normally, but will not appear in log.)
  * whatever python command:
    - [r for r in results if sum(results[r]['t'][:-3]) > 1]: print jobs during more then 1 minute
    - locals(): print local variables
"""
    print "\n Welcome!!\n"
    print help_s
    print '   {0} jobs sent to queue\n'.format(len (listfile)+len(procs))
    try:
        while 1:
            try:
                r = raw_input('}:-> ')
            except (KeyboardInterrupt, EOFError):
                print '    X-O\n'
                r = 'q'
            if r=='q':
                r = raw_input('  -> one last question: \n     do you want to [k]ill or [s]ave running jobs, else [N]othing (k|s|N): ')
                if r == 'k':
                    t_runs._Thread__stop()
                    for p in procs:
                        procs[p]['p'].kill()
                        listfile.insert(0, procs[p]['cmd'])
                    break
                elif r == 's':
                    nprocs[0] = 0
                    while len (procs)>0:
                        sleep(1)
                    t_runs._Thread__stop()
                    break
            elif r=='c':
                p = raw_input('  -> type number of CPUs (currently: {0}): '.format(nprocs[0]))
                if p.isdigit():
                    nprocs[0] = int (p)
                    print 'ok\n'
                else:
                    print 'not valid number\n'
            elif r=='d':
                print '\nDone jobs:'
                print '***********'
                print header.format ('job #', 'start time', 'spent time', 'command', 'status')
                print '-'*(58+w)
                for j in results:
                    print raw.format (str(j), results[j]['t0'][4:-5],
                                      print_cmd (results[j]['cmd'], w),
                                      results[j]['status'],
                                      *[i for i in results[j]['t']])
                print ''
            elif r=='r':
                print '\nRunning jobs:'
                print '**************'
                print header.format ('job #', 'start time', 'running time', 'command', 'status')
                print '-'*(58+w)
                for j in procs:
                    print raw.format(str(j), ctime(procs[j]['t'])[4:-5],
                                      print_cmd (procs[j]['cmd'], w),
                                     procs[j]['status'],
                                     *[i for i in timit (time()-procs[j]['t'])])
                print ''
            elif r=='a':
                if len (results) > 0:
                    mean_t = sum([untime(results[r]['t']) for r in results])/len (results)
                else:
                    mean_t = 0
                rest = mean_t*len (listfile)/nprocs[0]
                print mean_t
                print '\nSummary:'
                print '*********\n'
                print 'Job name      : ' + name
                print 'assigned CPUs : ' + str (nprocs[0])
                print 'done jobs     : ' + str (len (results))
                print 'mean time     : ' + timestr.format(*timit (mean_t))
                print 'resting jobs  : ' + str (len (listfile))
                print 'resting time  ~ ' + timestr.format(*timit(rest))
                print ''
            elif r=='h':
                print help_s
            elif r=='w':
                print '\n Waiting Jobs: {0}\n'.format((len(listfile)))
            elif r:
                try:
                    exec ('pprint (%s)'%(r))
                except:
                    try:
                        exec (r)
                    except:
                        print ' hmmm... this is not working well\n'
    except Exception as e:
        t_runs._Thread__stop()
        _, _, exc_tb = exc_info()
        print 'Big Horror!!\n'
        print e, '(line %s)' % exc_tb.tb_lineno
        return


def wait(t_runs, t_term):
    '''
    wait until finish
    '''
    while t_runs.is_alive() and t_term.is_alive():
        sleep(2)
    t_term._Thread__stop()


def bye():
    return ['\n Bye-bye\n', '\n Talogo!!\n', '\n Have a nice day\n',
            '\n And they lived happily ever after and they had a lot of children\n',
            '\n The End.\n', '\n Au revoir\n', 
            '\n Nooooo come back!!!! Quick!!\n', '\n Thanks.. I really enjoyed\n',
            "\n I'm a poor lonesome cowboy, and a long way from home...\n",
            '\n Flying Spaghetti Monster be with you.\n'] [int (str (time())[-1])]


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
    parser.add_option('-N', dest='name',
                      help='array-job name')
    parser.add_option('-p', dest='nprocs', metavar="INT", default='4',
                      help='number of procs to use')
    parser.add_option('-r', dest='refresh', metavar="INT", default='2',
                      help='number of seconds between each job refresh')
    parser.add_option('--resume', dest='resume', metavar="PATH", default='',
                      help='resume array job from pickle')
    opts = parser.parse_args()[0]
    if not opts.listfile and not opts.resume:
        parser.print_help()
        exit()
    if opts.resume:
        opts.log = opts.resume
        opts.listfile = opts.resume.replace('.pik','')
    elif not opts.log:
        opts.log = opts.listfile + '.pik'
    if not opts.name:
        opts.name = opts.listfile.split('/')[-1]
    return opts


if __name__ == "__main__":
    exit(main())
