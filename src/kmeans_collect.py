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

# This file implements GPU KMeans Clustering after collecting RDD's data 
# using Spark's collect().

import sys
import numpy
import random
import time
from itertools import groupby
from operator import itemgetter

from pyspark import SparkContext

from pycuda import gpuarray
import pycuda.autoinit
from pycuda.compiler import SourceModule
import pycuda.driver as cuda

def parseVector(line):
    return numpy.array([float(x) for x in line.split(' ')])


def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):	
        tempDist = numpy.sum((p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex

def reduceClusters(obj1, obj2): #(x[0], ((x[1],x[2],x[3]), 1))
    return ( obj1[0], 	
		[obj1[1][0]+obj2[1][0], obj1[1][1]+obj2[1][1], obj1[1][2]+obj2[1][2], obj1[1][3]+obj2[1][3]] ) 

def divideCluster(container, y):
    return [x/y for x in container]



if __name__ == "__main__":

    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage: ./spark-submit kmeans_collect <file> <k> <convergeDist>"
        exit(-1)
    
    start = time.time()
    sc = SparkContext(appName="PythonKMeans")	
    lines = sc.textFile(sys.argv[1], 2)		
    rdddata = lines.map(parseVector).cache()	
    data = numpy.asarray(rdddata.collect(), dtype = numpy.float32)
    
    k = int(sys.argv[2])
    convergeDist = float(sys.argv[3]) 
    kPoints = numpy.asarray(random.sample(data, k))
    datasize = len(data)
    centroids = numpy.empty((datasize), gpuarray.vec.float4)
    mod = SourceModule("""
	__global__ void assignToCentroid(float *data, int* datasize, int *k, float *kPoints, float4 * clusters)
	{ 
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
			for (int i = 0; i < K; i++){//hardwired. TODO: make it an argument
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

    
    gridNum = (datasize * 3)/256 + 1
    datasize = numpy.asarray(datasize)
    k = numpy.asarray(k)
    tempDist = 1.0 
    while tempDist > convergeDist: 
	func = mod.get_function("assignToCentroid")
        func(cuda.In(data), cuda.In(datasize), cuda.In(k), cuda.In(kPoints), cuda.Out(centroids), 
		block=(16, 16, 1), grid=(gridNum, 1), shared=0)

    
        closest = [(x[0], [x[1],x[2],x[3], 1]) for x in  centroids]
    
        statPoints = [reduce(reduceClusters, group) for _, group in groupby(sorted(closest), key=itemgetter(0))]
        newPoints = map(lambda (x, y): (x, 	divideCluster(y[:3],y[3])), statPoints)
    
        tempDist = sum(numpy.sum((kPoints[int(x)] - y) ** 2) for (x, y) in newPoints)
        
   	for (x, y) in newPoints:                        # update centroids
             kPoints[x] = y
 
        print "Current distance: ", tempDist	
    print "Final centers:\n"
    print newPoints
    
    sc.stop()
    stop = time.time()
    print "Completion time: ", (stop - start) ," seconds"
