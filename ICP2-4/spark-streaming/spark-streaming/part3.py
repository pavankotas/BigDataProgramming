import sys
import os

# os.environ["SPARK_HOME"] = "C:\\spark\\"
# os.environ["HADOOP_HOME"]="C:\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

"""
This is use for create streaming of text from txt files that creating dynamically 
from files.py code. This spark streaming will execute in each 3 seconds and It'll
show number of words count from each files dynamically
"""


def main():

    sc = SparkContext(appName="PythonStreamingCharacterCount")
    ssc = StreamingContext(sc, 1)

    lines = ssc.socketTextStream("localhost", 9999)
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (len(word), word)) \
        .reduceByKey(lambda a, b: a + ',' + b)


    counts.pprint()

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()


