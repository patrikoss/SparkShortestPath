# Spark Shortest path
This is an implementation of the algorithm that computes the shortest path relation for a given weighted graph in spark. There are 2 versions of the algorithm: one calculating the path by extending the current result by a single edge at a time, and another calculating the path by merging two shortest paths (a,b) and (b,c) to form a shortest path (a,c)

## Prerequisites
- python 3.5
- pyspark 2.3

## Starting the program
- Set up pyspark on your cluster of computers
- ```spark submit [spark_options] main.py path_to_input_on_hdfs path_to_output_on_hdfs method```
where method is one of: "sql_edge_extension" or "sql_path_merge"

## Files:
- sql_path_merge.py
- sql_edge_extension.py
- tests.py - tests
- main.py
- data_example/small.json - small example of input data

## Input format:
As an example to demonstrate how the program works, we are goin to use the data of the format:

```{"title":"paper-title", "authors":["Joe Doe", "Samantha Timothy", "Black Duck"], "year": null}```

to calculate the shortest path between two authors of some paper.
The vertices of the graph are the authors and the edges are connection between authors. There is an edge between author A and author B of weight d if A and B cooperated on some paper in a group of d authors.
You can download a bigger sample at:

projects.csail.mit.edu/dnd/DBLP/dblp.json.gz

yet you will have to convert the data into the format above

