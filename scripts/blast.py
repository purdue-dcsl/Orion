#!/usr/bin/env python

import time
import sys, os, subprocess
ORION_HOME='/home/min/a/kmahadik/Orion'
for line in sys.stdin:
  data = line.strip()
  if not data:
	break
  args = dict(pair.split(':') for pair in data.split(','))
  cmd = [args['path'], '-p', args['program'], '-i', args['query'], '-d',
  	args['database']]
  #with open(args['output'], "w") as outfile:
  p1=subprocess.Popen(cmd,stdout=subprocess.PIPE)
  p1.wait()
  cmd= ["%s/scripts/fb_ParseBlastN.pl"%ORION_HOME]
  p2=subprocess.Popen(cmd,stdin=p1.stdout)
  p2.wait()
  p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
  output,err = p2.communicate()

