"""
Image Discovery Kernel
==========================================================

This script takes as input a path and returns a dataframe
with all the images and their size.

Author: Aymen Alsaadi
License: MIT
Copyright: 2019-2020
"""
from glob import glob
import argparse
import os
import math
import pandas as pd
import cv2
from ..iceberg_zmq import Publisher

class Discovery(object):

    def __init__(self, name='simple',queue_out='simple',source_img=None,path=None):

        self._name = name
        self._src_path = source_img
        self._trg_path = path
        with open(queue_out) as fqueue:
            pub_addr_line, _ = fqueue.readlines()
        print(pub_addr_line)
        if pub_addr_line.startswith('PUB'):
            self._addr_in = pub_addr_line.split()[1]
        else:
            RuntimeError('Publisher address not specified in %s' % queue_out)

        self._publisher = Publisher(channel=self._name, url=self._addr_in)

        self.dataset = None

    def _image_discovery(self, filesize=True):
        """
        This function creates a dataframe with image names and size from a path.

        :Arguments:
            :path: Images path, str
            :filename: The filename of the CSV file containing the dataframe.
                    Default Value: list.csv
            :filesize: Whether or not the image sizes should be inluded to the
                    dataframe. Default value: False
            """

        #filepaths1 = glob(self._path + '/*.tif.png')
        #filepaths2 = glob(self._path + '/*.tif.png')
        colonies = ['BEAU','BEDM','BRDN','BRDS','CROZ']
        dataframe=pd.DataFrame(columns=['ImageName1','ImageName2','SIZE1','SIZE2','X1','Y1','X2','Y2'])
        for colony in colonies:
            src_path = self._src_path+colony+'/Aerial/'
            trg_path = self._trg_path+colony+'/Satellite/'
            print src_path
            print trg_path
            for path, dirs,src_images in os.walk(src_path):
                for img in src_images :
                    if img.startswith("CA") and img.endswith(".tif"):
                        img1=cv2.imread(src_path+img)
                        print ('found Source GEOTIFF Images '+img)
                        for path, dirs, files in os.walk(trg_path):
                            for filename in files:
                                if filename.startswith("WV") and filename.endswith(".tif"):
                                    #img1=cv2.imread(args.source_img+img)
                                    img2 = cv2.imread(trg_path+filename)
                                    size1 = int(math.ceil(os.path.getsize(src_path+img)/(1024*1024)))
                                    size2 = int(math.ceil(os.path.getsize(trg_path+filename)/(1024*1024)))
                                    print ('found Target GEOTIFF Images '+filename)
                                    try:
                                            session_data = {
                                                                    'ImageName1'      : src_path+img,
                                                                    'ImageName2'      : trg_path+filename,
                                                                    'SIZE1'           : size1,
                                                                    'SIZE2'           : size2,
                                                                    'X1'              : img1.shape[0],
                                                                    'Y1'              : img1.shape[1],
                                                                    'X2'              : img2.shape[0],
                                                                    'Y2'              : img2.shape[1]
                                                            }
                                    except Exception as e:
                                                            print e
                                                            session_data = {
                                                                    'ImageName1'      : src_path+img,
                                                                    'ImageName2'      : trg_path+filename,
                                                                    'SIZE1'           : size1,
                                                                    'SIZE2'           : size2,
                                                                    'X1'              : img1.shape[0],
                                                                    'Y1'              : img1.shape[1],
                                                                    'X2'              : img2.shape[0],
                                                                    'Y2'              : img2.shape[1]
                                                            }
                                                            pass
                                                        
                                    dataframe =  dataframe.append(session_data, ignore_index=True)
                                    #dataframe =  dataframe.reset_index().drop('index', axis=1)
                                    #dataframe.to_csv('Desc1CSV.csv',index=False)
        self.dataset = dataframe



    def _connect(self):

        self._publisher.put(topic='request', msg={'name': self._name,
                                                  'request': 'connect',
                                                  'type': 'sender'})
    def _send_data(self):
        
        for img1, img2, _, _, x1, y1, x2, y2  in self.dataset.values:
            message = '%s$%s$%s$%s$%s$%s' % (img1, img2, x1, y1, x2, y2)
            print('image {request: enqueue, data: %s}' % message)
            
            self._publisher.put(topic='image', msg={'request': 'enqueue',
                                                    'data': message})

    def _disconnect(self):

        self._publisher.put(topic='request', msg={'name': self._name,
                                                  'type': 'sender',
                                                  'request': 'disconnect'})

    def run(self):

        self._connect()

        self._image_discovery()

        self._send_data()

        self._disconnect()

        return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source_img', type=str)
    parser.add_argument('path', type=str)
    parser.add_argument('name', type=str)
    parser.add_argument('queue_file', type=str)

    args = parser.parse_args()

    discovery = Discovery(name=args.name, queue_out=args.queue_file,
                          source_img=args.source_img,path=args.path)
    discovery.run()
