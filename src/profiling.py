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

# This program calls gpuWordCount inside 
# $SPARK_HOME/python/pyspark/rdd.py in order to 
# investigate how time is spend for an execution of 
# Wor Count using GPU.
#
# Note: Use the Updated rdd.py which includes GPU functions. 

from pyspark import SparkContext
import sys, getopt, time
import pycuda.autoinit

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: ./spark-submit profiling <file>"
        exit(-1)
    start = time.time()
    sc = SparkContext(appName = "profiling")    
    text_file = sc.textFile(sys.argv[1])
    text_file.gpuWCProfiling()
    sc.stop()
    stop = time.time()
    print "word count took", (stop-start), "seconds"
