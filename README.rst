Multi-processes
***************

Simple utility to run a list of jobs, distributing along a given number of CPUs

usage: 


  python multi_f.py -i test.q -o test.pik -p 4


this will launch jobs in test.q and open a terminal that allows to monitorize jobs
running jobs and finished jobs

Log of the runs (stdout, stderr, running time, and command) is stored in a pickle usually:
   [your_infile].pik

if you decided to stop executing jobs (q command in prompter), pending jobs are also stored in the log-file. If you choosed the "k" option to kill running jobs, then pending jobs will include the jobs that are already running. Otherwise, program will wait for running jobs to finish.

at any time jobs can be stopped with "q".

the log of done jobs is writen in a python diccionary in -o path




