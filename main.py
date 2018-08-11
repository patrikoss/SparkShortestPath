from pyspark.sql.types import *
import pyspark
import argparse
import sql_edge_extension
import sql_path_merge



METHODS = ['sql_edge_extension','sql_path_merge']



def main(args, spark, sc):
    input, output = args.input, args.output
    shortest_paths = eval(args.method).shortest_paths
    papers = spark.read.load(input, format='json')
    # vertices = papers.rdd.filter(lambda row: len(row.authors) > 1). \
    #     flatMap(lambda r: r.authors).distinct()
    edges = papers.rdd.map(lambda row: row.authors).map(lambda l: [(l[i], l[j], len (l)) \
                    for i in range(len(l)) for j in range(len(l)) if i!=j ]) \
                    .flatMap(lambda l:l) \
                    .map(lambda edge: ((edge[0], edge[1]), edge[2])) \
                    .reduceByKey(min) \
                    .map(lambda t: (t[0][0], t[0][1], t[1]))

    result = shortest_paths(spark, edges)
    result.saveAsTextFile(output)

if __name__=='__main__':
    parser = argparse.ArgumentParser(description="Shortest path calculator")
    parser.add_argument('input', type=str, help='path to folder with input data')
    parser.add_argument('output', type=str, help='path to output folder')
    parser.add_argument('method', choices=METHODS, \
                        help='which method to use to calculate the shortest path')
    args = parser.parse_args()

    spark = pyspark.sql.SparkSession.builder.appName('Shortest path').getOrCreate()
    sc = spark.sparkContext
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    spark.conf.set("spark.default.parallelism", 5)

    main(args, spark, sc)


