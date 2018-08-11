import pyspark

def shortest_paths(spark, edges):

    schema = ['begin', 'end', 'length']
    edgesDF = edges.toDF(schema)
    edgesDF.createOrReplaceTempView('edges')

    
    vertices = spark.sql("SELECT DISTINCT begin FROM edges UNION SELECT DISTINCT end FROM edges")
    n = vertices.count()
    path=spark.sql("SELECT DISTINCT begin, begin as end, 0 as length FROM edges")
    path.createOrReplaceTempView('path')


    update_path_sql = """
        SELECT begin, end, min(length) as length FROM 
            ((
                SELECT path.begin as begin, edges.end as end, (path.length+edges.length) as length
                FROM path JOIN edges
                ON path.end = edges.begin
            )
            UNION 
            (
                SELECT * FROM path
            ))

        GROUP BY begin, end
    """
    for k in range(n):
        path = spark.sql(update_path_sql)
        path.createOrReplaceTempView('path')

    path = spark.sql("SELECT * FROM path WHERE begin != end")
    return path.rdd.map(lambda row: (row.begin, row.end, row.length) )
    
