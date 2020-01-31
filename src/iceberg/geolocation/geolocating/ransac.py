"""
Wrapper for Image Matches filteration . 
Author: Aymen Alsaadi
License: MIT
Copyright: 2019-2020
"""
import warnings
import argparse
import os
import shutil
import time
import random
import json
import sys
import pandas as pd
import subprocess
from ..iceberg_zmq import Publisher, Subscriber

class RansacFilter(object):

    def __init__(self, name, queue_in):
        self._timings = pd.DataFrame(columns=['Image','Start','End','Ransac'])
        self._name = name
        tic = time.time()
        with open(queue_in) as fqueue:
            pub_addr_line, sub_addr_line = fqueue.readlines()

            if pub_addr_line.startswith('PUB'):
                print(pub_addr_line)
                self._in_addr_in = pub_addr_line.split()[1]
            else:
                RuntimeError('Publisher address not specified in %s' % queue_in)

            if sub_addr_line.startswith('SUB'):
                print(sub_addr_line)
                self._in_addr_out = sub_addr_line.split()[1]
            else:
                RuntimeError('Subscriber address not specified in %s' % queue_in)

        self._publisher_in = Publisher(channel=self._name, url=self._in_addr_in)
        self._subscriber_in = Subscriber(channel=self._name, url=self._in_addr_out)
        self._subscriber_in.subscribe(topic=self._name)
	toc = time.time()
        self._timings.loc[len(self._timings)] = ['configure',tic,toc,0]
    

    def _connect(self):
        tic = time.time()
        self._publisher_in.put(topic='request', msg={'name': self._name,
                                                     'request': 'connect',
                                                     'type': 'receiver'})
	toc = time.time()
	self._timings.loc[len(self._timings)] = ['connect',tic,toc,0]


    def _disconnect(self):
        tic = time.time()
        self._publisher_in.put(topic='request', msg={'name': self._name,
                                                     'type': 'receiver',
                                                     'request': 'disconnect'})
	toc = time.time()
        self._timings.loc[len(self._timings)] = ['disconnect',tic,toc,0]


    def _get_message(self):

        self._publisher_in.put(topic='image', msg={'request': 'dequeue',
                                                   'name': self._name})

        _, recv_message = self._subscriber_in.get()

        if recv_message[b'type'] == b'image':
            return recv_message[b'data']
	    print (recv_message[b'data'])

	print ('message not received')
        return None

    
    def _ransac(self, img1, img2, matches):


	# time it
        tic = time.time()

	print ('This is Ransac matching function')
	print (img1, img2, matches)
	
	#Trying stupid name techinque
	base1=os.path.basename(img1)
	base2=os.path.basename(img2)
	name1=os.path.splitext(base1)[0]
	name2=os.path.splitext(base2)[0]

	ransac_name = 'ransaced_matches_'+name1+'_'+name2+'.csv'
	
	#ransac_name = 'ransac.csv'
	output_folder = "/pylon5/mc3bggp/aymen/ransac_out"+'/'+ransac_name
	cmd = 'python /home/aymen/SummerRadical/4DGeolocation/ASIFT/src/PHASE_3_RANSAC_FILTERING/ransac_filter.py'
	os.system(cmd+' -img1_filename '+img1+' -img1_nodata '+'0'+' -img2_filename '+img2+' -img2_nodata '+'0'+' '+matches+' '+output_folder)

	count += 1
	toc = time.time()
	self._timings.loc[len(self._timings)] = [ransac_name,tic,toc,count]

    def run(self):

        self._connect()

        cont = True
	count = 0

        while cont:
            message = self._get_message()
            sys.stdout.flush()
	    
            if message not in ['disconnect','wait']:
                try:
                    print(message)
		    img1, img2, matches = message.split('$')
                    sys.stdout.flush()
                    self._ransac(img1,img2,matches)
		    print('Matches are filtered')
                except:
                    sys.stdout.flush()
                    print('Matches are not filtered')
                    sys.stdout.flush()
            elif message == 'wait':
                time.sleep(1)
            else:
                self._disconnect()
                cont = False

	self._timings.to_csv(self._name + ".csv", index=False)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='validates a CNN at the haul out level')
    parser.add_argument('--name', type=str)
    parser.add_argument('--queue_in', type=str)
    args = parser.parse_args()

    filteration = RansacFilter(name=args.name, queue_in=args.queue_in)

    filteration.run()
