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

# This program implements KMeans Clustering on GPU through Spark's mapPartitions.

import sys
import numpy
import random
import time
import threading
import random

from pyspark import SparkContext

from pycuda import gpuarray
import pycuda.autoinit
from pycuda.compiler import SourceModule
import pycuda.driver as cuda


def parseVector(line):
    return numpy.array([float(x) for x in line.split(' ')])

class GPUKMeansThread(threading.Thread):
    def __init__(self, dev_id, rdd, k):
        threading.Thread.__init__(self)
        self.dev_id = dev_id
        self.rdd = rdd
        self.k = k

    def gpuKMeans(self):
	k = numpy.asarray(self.k)
        def gpuFunc(iterator):
            iterator = iter(iterator)
            cpu_data = numpy.asarray(list(iterator), dtype = numpy.float32)
	    datasize = numpy.asarray(len(cpu_data))
	    gridNum = (datasize * 3)/256 + 1 # * 3 for data dimensions. /256 for block size. 
					     # +1 for overprovisioning in case there is dangling threads
            centroids = numpy.empty((datasize), gpuarray.vec.float4) 
            cuda.init()
            dev = cuda.Device(0)
            contx = dev.make_context()
	    
	    # The GPU kernel below takes centroids IDs and 3-D data points in form of float4 (x,y,z,w).
	    # X is for the centroid ID whereas (y,z,w) are the actual point coordinate.
	    mod = SourceModule("""
        	__global__ void assignToCentroid(float *data, int* datasize, int *k, float *kPoints, float4 * clusters){ 
                	//1d grid index
                	unsigned idxInLeftBlocks = blockIdx.x * (blockDim.x * blockDim.y);
                	unsigned idxInCurrBlock  = threadIdx.y * blockDim.x + threadIdx.x;
                	unsigned idx = idxInLeftBlocks + idxInCurrBlock;
                	int size = datasize[0];
                	int K = k[0];   
                	if (idx < size*3){//consider xyz for each data point
                        	if (idx % 3 == 0){
                                	unsigned dataIdx = idx/3;
                                	clusters[dataIdx].x = 999999; 
                                	clusters[dataIdx].y = data[idx];
                                	clusters[dataIdx].z = data[idx+1];
                                	clusters[dataIdx].w = data[idx+2];      
                        	}
                        	float inf =  99999.9999;
                        	float closest = inf;

                        	float tempDist=0, tempDistX=0, tempDistY=0, tempDistZ=0;
				//updateing tempDist
                        	for (int i = 0; i < K; i++){
                                	if( idx % 3 == 0 /*0,3,6*/)
                                        	tempDistX = data[idx] - kPoints[i*3];
                                	else if(idx % 3 == 1 /*1,4,7*/)
                                        	tempDistY = data[idx] - kPoints[i*3 + 1];
                                	else {/*2,5,8*/
                                        	tempDistZ = data[idx] - kPoints[i*3 + 2];
                                        	tempDist = pow(tempDistX,2) + pow(tempDistY,2) + pow(tempDistZ,2);
                                	}

                                	if(tempDist < closest){
                                        	closest = tempDist;
                                        	int dataIdx = idx/3; 
                                        	clusters[dataIdx].x = i;//coordinate belongs to cluster i
                                	}
                        	}
                	}
        	}

    		""")
    	    func = mod.get_function("assignToCentroid")
    	    func(cuda.In(cpu_data), cuda.In(datasize), cuda.In(k), cuda.In(kPoints), cuda.Out(centroids), block=(16, 16, 1), grid=(gridNum, 1), shared=0)
	    closest = [(x[0], ( numpy.asarray([x[1],x[2],x[3]]), 1)) for x in  centroids]
	    contx.pop()
	    del cpu_data
	    del datasize
	    del centroids
	    del contx
            return iter(closest)

        vals = self.rdd.mapPartitions(gpuFunc)
        return vals


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage: ./spark-submit kmeans_mapp <file> <k> <convergeDist>"
        exit(-1)
    
    start = time.time()
    sc = SparkContext(appName="PythonKMeans")	
    lines = sc.textFile(sys.argv[1], 2)		
    print (lines.getNumPartitions())
    Rdd = lines.map(parseVector).cache()	
    
    k = int(sys.argv[2])
    convergeDist = float(sys.argv[3])           

    kPoints = numpy.asarray(Rdd.takeSample(False, k, 1), dtype = numpy.float32) 
    tempDist = float("+inf")

    while tempDist > convergeDist:
	# 1. Assign points to centroids parallely using GPU
    	gpukmeans_thread = GPUKMeansThread(0, Rdd, k)
    	closest = gpukmeans_thread.gpuKMeans()

	# 2. Update the  centroids on CPU
	# TODO: move pointStats calculation on GPU
    	pointStats = closest.reduceByKey(
            lambda (x1, y1), (x2, y2): (x1 + x2, y1 + y2))

        newPoints = pointStats.map(
            lambda (x, (y, z)): (x, y / z)).collect()   

	# Update distance on CPU
        tempDist = sum(numpy.sum((kPoints[x] - y) ** 2) for (x, y) in newPoints)
        for (x, y) in newPoints:                        
            kPoints[x] = y

    print "Final centers: " + str(kPoints)

    sc.stop()
    stop = time.time()
    print "Completion time: ", (stop - start) ," seconds"
