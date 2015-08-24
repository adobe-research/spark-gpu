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

def generate(k, filename):
    data = ""
    for i in range(k):
	val = np.random.uniform(low = 0, high=10, size=2)
        data += str(tuple(val)) + '\n'
    target = open(filename, 'w')
    target.write(data)
    target.close()


def main():
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: generate <length> <file>"
	exit(-1)
    base = sys.argv[1]
    file_ = sys.argv[2]
    for i in range(1,11):
	size = int(base) * i
	file_out = file_ + str(size)
	generate(size, file_out)
	print "generated ", file_out
if __name__ == "__main__":
    main()


