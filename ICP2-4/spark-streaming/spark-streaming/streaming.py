import sys
import os
os.environ["SPARK_HOME"] = "C:\\spark\\"
os.environ["HADOOP_HOME"]="C:\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def main():

    sc = SparkContext(appName="PythonStreamingWordCount")
    ssc = StreamingContext(sc, 1)

    lines = ssc.socketTextStream("localhost", 9999)
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()
