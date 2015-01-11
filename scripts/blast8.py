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
  	args['database'],'-m','8', '-o', args['output']]
  p=subprocess.call(cmd)

  f = open(args['output'], 'r')
  file_contents = f.read()
  print (file_contents)
  f.close()

  #for line in iter(p.stdout.readline, b''):
  #	print line,
  #p.stdout.close()
  #p.wait()
  #out = proc.communicate()[0]
  #print out 
#print p.communicate()
 

