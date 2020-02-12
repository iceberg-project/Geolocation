"""
Wrapper for Segmentation Evaluation.
Author: Aymen Alsaadi
License: MIT
Copyright: 2019-2020
"""
import argparse
import time
import os
import time
import sys
import subprocess
import pandas as pd


from ..iceberg_zmq import Publisher, Subscriber

class ImageMatching(object):

    def __init__(self, name, queue_in, queue_out):
        self._timings = pd.DataFrame(columns=['Image', 'Start', 'End', 'Sift'])
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
        self._timings.loc[len(self._timings)] = ['configure', tic, toc, 0]
 
    def _connect(self):
        tic = time.time()
        self._publisher_in.put(topic='request', msg={'name': self._name,
                                                     'request': 'connect',
                                                     'type': 'receiver'})
        print('Sent receiver connect message')
        time.sleep(1)
        self._publisher_out.put(topic='request', msg={'name': self._name,
                                                      'request': 'connect',
                                                      'type': 'sender'})
        time.sleep(1)
        print('Sent sender connect message')
        sys.stdout.flush()
        toc = time.time()
        self._timings.loc[len(self._timings)] = ['connect', tic, toc, 0]

    def _disconnect(self):
        tic = time.time()
        self._publisher_in.put(topic='request', msg={'name': self._name,
                                                     'type': 'receiver',
                                                     'request': 'disconnect'})
        print('Sent receiver disconnect message') 
        time.sleep(1)
        self._publisher_out.put(topic='request', msg={'name': self._name,
                                                      'request': 'disconnect',
                                                      'type': 'sender'})
        print('Sent sender disconnect message')
        time.sleep(5)
        sys.stdout.flush()
        toc = time.time()
        self._timings.loc[len(self._timings)] = ['disconnect', tic, toc, 0]

    def _get_message(self):

        self._publisher_in.put(topic='image', msg={'request': 'dequeue',
                                                   'name': self._name})

        _, recv_message = self._subscriber_in.get()

        if recv_message[b'type'] == b'image':
            print(recv_message)
            return recv_message[b'data']

        return None

    def _matching(self, img1, img2, x1, y1, x2, y2):

        # time it
        tic = time.time()
        count = 0
        images = img1+'_'+img2
        print('This is matching function')
        cmd = '/pylon5/mc3bggp/paraskev/gpuSift/CudaSift/cudasift'
        subprocess.call([cmd, img1, '0', '0', str(x1), str(y1), img2, '0', '0', str(x2), str(y2)])

        count += 1

        toc = time.time()
        elapsed = toc - tic
        self._timings.loc[len(self._timings)] = [images, tic, toc, count]

        sys.stdout.flush()

    def run(self):

        self._connect()
        cont = True
        count = 0

        while cont:
            message = self._get_message()
            if message not in ['disconnect', 'wait']:
                try:
                    print("This is the message : ", message)
                    img1, img2, x1, y1, x2, y2 = message.split('$')
                    self._matching(img1, img2, x1, y1, x2, y2)
                    base1 = os.path.basename(img1)
                    base2 = os.path.basename(img2)
                    name1 = os.path.splitext(base1)[0]
                    name2 = os.path.splitext(base2)[0]
 
                    sift_out = '/pylon5/mc3bggp/paraskev/cuda_out/sift_matches_'+name1+'_'+name2+'.csv'
                    new_message = '%s$%s$%s' % (img1, img2, sift_out)
                    print('New message will be sent to Q2: ', new_message)
                    self._publisher_out.put(topic='image', msg={'name': self._name,
                                                                'request': 'enqueue',
                                                                'data': new_message})
                    count += 1
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
        self._timings.to_csv(self._name + ".csv", index=False)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='validates a CNN at the haul out level')
    parser.add_argument('--name', type=str)
    parser.add_argument('--queue_in', type=str)
    parser.add_argument('--queue_out', type=str)
    args = parser.parse_args()

    match = ImageMatching(name=args.name, queue_in=args.queue_in, queue_out=args.queue_out)
    match.run()
