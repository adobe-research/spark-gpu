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


# This file implements CPU Word Count.


from pyspark import SparkContext
import sys, getopt, time

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: ./spark-submit cpuwc <file>"
        exit(-1)
    start = time.time()
    sc = SparkContext(appName = "wordCount")    
    text_file = sc.textFile(sys.argv[1])
    counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: 1) \
             .reduce(lambda a, b: a + b)
    #counts.saveAsTextFile("counts.txt") 
    print counts
    sc.stop()
    stop = time.time()
    print "word count took", (stop-start), "seconds" # Execution time in local mode
