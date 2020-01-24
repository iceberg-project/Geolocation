"""
Wrapper for Segmentation Evaluation. 
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

class ImageMatching(object):

    def __init__(self, name, queue_in, queue_out):
        
	self._timings = pd.DataFrame(columns=['Image','Start','End','Sift'])
        tic = time.time()
        self._name = name
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

	with open(queue_out) as fqueue:
            pub_addr_line, _ = fqueue.readlines()

            if pub_addr_line.startswith('PUB'):
                print(pub_addr_line)
                self._out_addr_in = pub_addr_line.split()[1]
            else:
                RuntimeError('Publisher address not specified in %s' % queue_out)

        self._publisher_in = Publisher(channel=self._name, url=self._in_addr_in)
        self._subscriber_in = Subscriber(channel=self._name, url=self._in_addr_out)
        self._subscriber_in.subscribe(topic=self._name)
	self._publisher_out = Publisher(channel=self._name, url=self._out_addr_in)
	toc = time.time()
	self._timings.loc[len(self._timings)] = ['configure',tic,toc,0]
    
        

    def _connect(self):
        tic = time.time()
        self._publisher_in.put(topic='request', msg={'name': self._name,
                                                     'request': 'connect',
                                                     'type': 'receiver'})
       
	
	time.sleep(1)
        self._publisher_out.put(topic='request', msg={'name': self._name,
                                                      'request': 'connect',
                                                      'type': 'sender'})
        toc = time.time()
	self._timings.loc[len(self._timings)] = ['connect',tic,toc,0]

    def _disconnect(self):
        tic = time.time()
        self._publisher_in.put(topic='request', msg={'name': self._name,
                                                     'type': 'receiver',
                                                     'request': 'disconnect'})
	
	time.sleep(1)
        self._publisher_out.put(topic='request', msg={'name': self._name,
                                                      'request': 'disconnect',
                                                      'type': 'sender'})
        toc = time.time()
	self._timings.loc[len(self._timings)] = ['disconnect',tic,toc,0]
        

    def _get_message(self):

        self._publisher_in.put(topic='image', msg={'request': 'dequeue',
                                                   'name': self._name})

        _, recv_message = self._subscriber_in.get()

        if recv_message[b'type'] == b'image':
	    print recv_message
            return recv_message[b'data']

        return None

    
    def _matching(self, img1,img2,x1,y1,x2,y2):
        
	print ('This is matching function')
	cmd = '/home/aymen/SummerRadical/SIFT-GPU/cudasift'
	subprocess.call([cmd, img1, '0', '0', str(x1),str(y1), 
			      img2, '0', '0', str(x2), str(y2)])
        sys.stdout.flush()

    def run(self):
     
        
        self._connect()

        cont = True

        while cont:

	    message = self._get_message()
	   

            if message not in ['disconnect','wait']:
                try:
	   	    
            	    print ("This is the message : ", message)
		    img1, img2, x1, y1, x2, y2 = message.split('$')
            	    self._matching(img1,img2,x1,y1,x2,y2)
		  
		    sift_out = '/home/aymen/cuda_out/sift_matches_'+img1+'_'+img2+'.csv'
		    new_message = '%s$%s$%s' % (img1, img2, sift_out)
		    print ('New message will be sent to Q2: ', new_message)
		    self._publisher_out.put(topic='image', msg={'name': self._name,
                                                    'request': 'enqueue',
                                                    'data': new_message})
		    sys.stdout.flush()
                except:
                    sys.stdout.flush()
                    print('Images are not matched ')
                    sys.stdout.flush()
            elif message == 'wait':
                time.sleep(1)
	    else:
                self._disconnect()
                cont = False
      




if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='validates a CNN at the haul out level')
    parser.add_argument('--name', type=str)
    parser.add_argument('--queue_in', type=str)
    parser.add_argument('--queue_out', type=str)
    args = parser.parse_args()

    match = ImageMatching(name=args.name, queue_in=args.queue_in, queue_out=args.queue_out)
    match.run()









