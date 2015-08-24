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

# This program is adopted from the 
# KMeans algorithm in Apache Spark: $SPARK_HOME/examples

import sys
import numpy as np
from pyspark import SparkContext
import time 

def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])


def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):	# for k centers
        tempDist = np.sum((p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage: kmeans <file> <k> <convergeDist>"
        exit(-1)
    start = time.time()
    sc = SparkContext(appName="PythonKMeans")	# SparkContext
    lines = sc.textFile(sys.argv[1])		# RDD
    data = lines.map(parseVector).cache()	# Read Vectors from file
    K = int(sys.argv[2])			# Get K
    convergeDist = float(sys.argv[3])		# Get convergence distance

    kPoints = data.takeSample(False, K, 1)	# Get initial K points
    tempDist = float("+inf")			# Ensure at least 1 iteration
    
    while tempDist > convergeDist: 		# Check if the centroids have converged
	closest = data.map(
            lambda p: (closestPoint(p, kPoints), (p, 1))) # (clusterID, (xyz, 1))
        
	pointStats = closest.reduceByKey(
            lambda (x1, y1), (x2, y2): (x1 + x2, y1 + y2))# (clusterID, (xyz_totals, total_population)) 
	
        
	newPoints = pointStats.map(
            lambda (x, (y, z)): (x, y / z)).collect() 	# Compute new centroid: (clusterID, xyz_totals/population)
	
        tempDist = sum(np.sum((kPoints[x] - y) ** 2) for (x, y) in newPoints)
        for (x, y) in newPoints:			# update centroids
            kPoints[x] = y

    print "Final centers: " + str(kPoints)

    sc.stop()
    stop = time.time()
    print "Completion time: ", (stop - start) ," seconds" 
