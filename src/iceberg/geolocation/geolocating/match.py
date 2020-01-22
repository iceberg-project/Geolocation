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

from ..iceberg_zmq import Publisher, Subscriber

class ImageMatching(object):

    def __init__(self, name, queue_in, cfg):
         
        self._name = name
        self._timings = pd.DataFrame(columns=['Image','Start','End','Seals'])
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
    
        with open(cfg) as conf:
            self._cfg = json.load(conf)
        toc = time.time()
        self._timings.loc[len(self._timings)] = ['config',tic,toc,0]

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

    def _get_image(self):

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
	img1 = img1
	img2 = img2
	x1 = x1
	y1 = y1
	x2 = x2
	y2 = y2
	subprocess.check_call([cmd, img1, '0', '0', x1, y1, img2, '0', '0', x2, y2])
	
       

    def run(self):
        

        
        self._connect()

        cont = True

        while cont:
	    message = self._get_image()
	    print ("This is the message : ", message)
	    img1, img2, x1, y1, x2, y2 = message.split('$')
	    print img1
            print img2
            print x1
            print y1
            print x2
            print y2
	    sys.stdout.flush()

            if img1 and img2 not in ['disconnect','wait']:
                try:
                    print(img1)
                    print(img2)
                    sys.stdout.flush()
                    self._match(img1,img2,x1,y1,x2,y2)
                except:
                    sys.stdout.flush()
                    print('Images are not matched :', img1,img2)
		    
                    sys.stdout.flush()
            elif image == 'wait':
                time.sleep(1)
            else:
                self._disconnect()
                cont = False
      




if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='validates a CNN at the haul out level')
    parser.add_argument('--name', type=str)
    parser.add_argument('--queue_in', type=str)
    parser.add_argument('--config_file',type=str)
    args = parser.parse_args()

    pred = PenguinsPredict(name=args.name, queue_in=args.queue_in, cfg=args.config_file)









