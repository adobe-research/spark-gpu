#!/home/ec2-user/anaconda/bin/python

###########################################################################
##
## Copyright (c) 2015 Adobe Systems Incorporated. All rights reserved.
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##
###########################################################################

import sys, getopt
import numpy as np

def kmeansGenerate(k, filename):
    data = ""
    for i in range(k):
    	floatlist = list(np.random.uniform(low=0.1, high=10, size=(3)))
	floatlist = " ".join(map(str, floatlist)) + '\n'
	data = data + floatlist
    target = open(filename, 'w')
    target.write(str(data))
    target.close()


def bayesGenerate(k, filename):
    data = ""
    for i in range(k):
        nplist = list(np.random.uniform(low=0.1, high=10, size=(4)))
        intlist = [int(x) for x in nplist]
	intlist = " ".join(map(str, intlist)) + '\n'
        data = data + intlist
    target = open(filename, 'w')
    target.write(str(data))
    target.close()

def main():
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: generate <length> <file>"
	exit(-1)
    kmeansGenerate(int(sys.argv[1]),sys.argv[2])
    #bayesGenerate(int(sys.argv[1]),sys.argv[2])
if __name__ == "__main__":
    main()


