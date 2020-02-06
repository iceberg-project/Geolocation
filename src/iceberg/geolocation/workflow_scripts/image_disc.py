"""
Image Discovery Kernel
==========================================================

This script takes as input a path and returns a dataframe
with all the images and their size.

Author: Aymen Alsaadi
License: MIT
Copyright: 2019-2020
"""
import argparse
import os
import math
import pandas as pd
import cv2
from ..iceberg_zmq import Publisher

class Discovery(object):

    def __init__(self, name='simple', queue_out='simple', path=None):

        self._name = name
        self._path = path
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
        #colonies = ['ADAR', 'AMBU', 'ANDI', 'ANNE', 'ARDL', 'ARMS', 'AUST', 'AVIA', 'BACK',
        #            'BARC', 'BATT', 'BEAG', 'BEAU', 'BECH', 'BELG', 'BERK', 'BERT', 'BIRD', 'BISC',
        #            'BRAS', 'BRAT', 'BRDM', 'BRDN', 'BRDS', 'BROW', 'BSON', 'BURK', 'CBAR', 'CHRI',
        #            'CIOL', 'CONT', 'CROZ', 'CURZ', 'DARB', 'DAVI', 'DEEI', 'DEMS', 'DEVI', 'DURO', 'DUTH',
        #            'EARL', 'EDMO', 'EDWA', 'ETNA', 'EVEN', 'EVER', 'FERR', 'FISH', 'FRNC', 'GEPT', 'GIBS',
        #            'GOSL', 'GOUR', 'HANN', 'HBAY', 'HERO', 'HOLD', 'HOLL', 'HOPE', 'IFOI', 'IVAN', 'JOUB',
        #            'JULE', 'KIRB', 'KIRT', 'KUNO', 'KUZI', 'LAUF', 'LLAN', 'LONG', 'LOVI', 'LSAY', 'MACK',
        #            'MADI', 'MAND', 'MART', 'MAWS', 'MBIS', 'MEDL', 'MICH', 'MIZU', 'MYAL', 'NMED', 'NORF',
        #            'NVEG', 'ODBE', 'OLDH', 'OMGA', 'ONGU', 'PATE', 'PAUL', 'PCHA', 'PENG', 'PETE', 'PGEO',
        #            'PIGE', 'PISL', 'PMAR', 'POSS', 'POWE', 'RAUE', 'RNVG', 'ROOK', 'RUMP', 'SAXU', 'SCUL',
        #            'SHEI', 'SHLY', 'SIGA', 'STAN', 'STEN', 'SVIS', 'TAYH', 'TRYN', 'TURR', 'VESN', 'VESS',
        #            'WATT', 'WAYA', 'WEDD', 'WIDE', 'WINK', 'WPEC', 'YALO', 'YTRE']
        colonies = ['ADAR', 'AMBU', 'ANDI', 'ANNE', 'ARDL']
        dataframe = pd.DataFrame(columns=['ImageName1', 'ImageName2', 'SIZE1', 'SIZE2', 'X1', 'Y1', 'X2', 'Y2'])
        data_path = self._path
        for image in os.listdir(data_path):
            print(image)
            for colony in range(1, len(colonies)):
                if image.startswith('['+colonies[colony]+']'):
                    print('found Source GEOTIFF Images '+ data_path+image)
                    img1 = cv2.imread(data_path+image)
                    for image2 in os.listdir(data_path):
                        if image2.startswith('['+colonies[colony]+']'):
                            img2 = cv2.imread(data_path+image2)
                            size1 = int(math.ceil(os.path.getsize(data_path+image)/(1024*1024)))
                            size2 = int(math.ceil(os.path.getsize(data_path+image2)/(1024*1024)))
                            print('found Target GEOTIFF Images '+ data_path+image2)
                            try:
                                session_data = {
                                    'ImageName1'      : data_path+image,
                                    'ImageName2'      : data_path+image2,
                                    'SIZE1'           : size1,
                                    'SIZE2'           : size2,
                                    'X1'              : img1.shape[1],
                                    'Y1'              : img1.shape[0],
                                    'X2'              : img2.shape[1],
                                    'Y2'              : img2.shape[0]}
                            except Exception as e:
                                print(e)
                                session_data = {
                                    'ImageName1'      : data_path+image,
                                    'ImageName2'      : data_path+image2,
                                    'SIZE1'           : size1,
                                    'SIZE2'           : size2,
                                    'X1'              : img1.shape[1],
                                    'Y1'              : img1.shape[0],
                                    'X2'              : img2.shape[1],
                                    'Y2'              : img2.shape[0]}
                            dataframe = dataframe.append(session_data, ignore_index=True)
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
    parser.add_argument('path', type=str)
    parser.add_argument('name', type=str)
    parser.add_argument('queue_file', type=str)

    args = parser.parse_args()

    discovery = Discovery(name=args.name, queue_out=args.queue_file,
                          path=args.path)
    discovery.run()
