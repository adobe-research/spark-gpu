###########################################################################
#
# Copyright (c) 2015 Adobe Systems Incorporated. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
###########################################################################


# This file implements GPU Word Count using Spark's mapPartition. 

import sys, getopt
import numpy as np
import threading
import time
import gc

from pyspark import SparkContext

from pycuda import gpuarray, reduction
import pycuda.driver as cuda

class GPUWCThread(threading.Thread):
    def __init__(self, dev_id, rdd):
        threading.Thread.__init__(self)
        self.dev_id = dev_id
	self.rdd = rdd

    def gpuWordCount(self):
	def gpuFunc(iterator):
	    # 1. Data preparation
            iterator = iter(iterator)
            cpu_data = list(iterator)
            cpu_dataset = " ".join(cpu_data)
            ascii_data = np.asarray([ord(x) for x in cpu_dataset], dtype=np.uint8)

	    # 2. Driver initialization and data transfer
	    cuda.init()
	    dev = cuda.Device(0)
	    contx = dev.make_context()
            gpu_dataset = gpuarray.to_gpu(ascii_data)

	    # 3. GPU kernel.
	    # The kernel's algorithm counts the words by keeping 
	    # track of the space between them
            countkrnl = reduction.ReductionKernel(long, neutral = "0",
            		map_expr = "(a[i] == 32)*(b[i] != 32)",
                        reduce_expr = "a + b", arguments = "char *a, char *b")

            results = countkrnl(gpu_dataset[:-1],gpu_dataset[1:]).get()
            yield results

	    # Release GPU context resources
	    contx.pop() 
	    del gpu_dataset
            del contx
	   
	    gc.collect()            
		    	
    	vals = self.rdd.mapPartitions(gpuFunc)
	return vals

def toascii(data):
    strs = " ".join(data)
    return np.asarray([ord(x) for x in data], dtype=np.uint8)

if __name__ == "__main__":
    if len(sys.argv) != 2:
  	print >> sys.stderr, "Usage: ./spark-submit gpuwc <file>"
        exit(-1)
    start = time.time()
    sc = SparkContext(appName = "wordCount")    
    Rdd = sc.textFile(sys.argv[1])
    gpuwc_thread = GPUWCThread(0,Rdd)
    partial_count_rdd = gpuwc_thread.gpuWordCount()
    count = partial_count_rdd.reduce(lambda x,y: x+y)
    print "total count", count
    sc.stop
    stop = time.time()
    #Just a convenient way to measure time in local mode 
    print "execution time: ", stop-start, "seconds"
