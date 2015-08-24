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

# This application sorts <key, values> using Spark's
# sortByKey

import sys
import numpy as np
import time 

from pyspark import SparkContext

def parseVector(line):
    return eval(line)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: sort <file>"
        exit(-1)
    #start = time.time()
    sc = SparkContext(appName="PythonSort")
    rdd = sc.textFile(sys.argv[1])#.flatMap(lambda x:x.split('\n'))
    data = rdd.map(parseVector).cache()
    data.count() # force action before sorting

    start = time.time()
    sorted_data = data.sortByKey()
    sorted_data.count()
    sc.stop()

    stop = time.time()
    print "execution time is: ", stop - start
