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

import sys

def generateWords(multiplier, outfile):
    print "reading data"
    dataset = file("The_Complete_Works_of_Willia.txt").read()
    print "creating dataset..."
    bigdataset = ""
    for k in range(int(multiplier)):
        bigdataset += dataset
    target = open(outfile, 'w')
    target.write(bigdataset)
    print "dataset created"
    return bigdataset

if __name__ == "__main__":
    if len(sys.argv) != 3:
    	print >> sys.stderr, """Usage: ./generate_words <base_multiplier> <out_file>
	     		   \nSample multipliers:
			   \n\t 32 GB = 33440
			   \n\t 4GB = 4180	
    			   \n\t 2GB = 2090	
    			   \n\t GB = 1045	
    			   \n\t 500 MB = 522
    			   \n\t 250 MB = 261
    			   \n\t 100 MB = 104 """
    	exit(-1)
    generateWords(sys.argv[1],sys.argv[2])
