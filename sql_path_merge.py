import pyspark
import math

def shortest_paths(spark, edges):

    schema = ['begin', 'end', 'length']
    edgesDF = edges.toDF(schema)
    edgesDF.createOrReplaceTempView('edges')
    
    vertices = spark.sql("SELECT DISTINCT begin FROM edges UNION SELECT DISTINCT end FROM edges")
    n = vertices.count()
    path=spark.sql("SELECT DISTINCT begin, begin as end, 0 as length FROM edges")
    path = path.union(edgesDF)
    path.createOrReplaceTempView('path')


    update_path_sql = """
        SELECT begin, end, min(length) as length FROM 
            ((
                SELECT path1.begin as begin, path2.end as end, (path1.length+path2.length) as length
                FROM path as path1 JOIN path as path2
                ON path1.end = path2.begin
            )
            UNION 
            (
                SELECT * FROM path
            ))

        GROUP BY begin, end
    """
    for k in range(math.ceil(math.log(n,2))):
        path = spark.sql(update_path_sql)
        path.createOrReplaceTempView('path')

    path = spark.sql("SELECT * FROM path WHERE begin != end")
    return path.rdd.map(lambda row: (row.begin, row.end, row.length))
    
