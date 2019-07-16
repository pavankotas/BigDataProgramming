from spark import *
from graphframes import *

# Vertex DataFrame
v = sqlContext.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)
], ["id", "name", "age"])
# Edge DataFrame
e = sqlContext.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
], ["src", "dst", "relationship"])
# Create a GraphFrame
g = GraphFrame(v, e)

g = Graphs(sqlContext).friends()  # Get example graph

# Display the vertex and edge DataFrames
g.vertices.show()
# +--+-------+---+
# |id|   name|age|
# +--+-------+---+
# | a|  Alice| 34|
# | b|    Bob| 36|
# | c|Charlie| 30|
# | d|  David| 29|
# | e| Esther| 32|
# | f|  Fanny| 36|
# | g|  Gabby| 60|
# +--+-------+---+

g.edges.show()
# +---+---+------------+
# |src|dst|relationship|
# +---+---+------------+
# |  a|  b|      friend|
# |  b|  c|      follow|
# |  c|  b|      follow|
# |  f|  c|      follow|
# |  e|  f|      follow|
# |  e|  d|      friend|
# |  d|  a|      friend|
# |  a|  e|      friend|
# +---+---+------------+

# Get a DataFrame with columns "id" and "inDegree" (in-degree)
vertexInDegrees = g.inDegrees

# Find the youngest user's age in the graph.
# This queries the vertex DataFrame.
g.vertices.groupBy().min("age").show()

# Count the number of "follows" in the graph.
# This queries the edge DataFrame.
numFollows = g.edges.filter("relationship = 'follow'").count()


# Search for pairs of vertices with edges in both directions between them.
motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()

# More complex queries can be expressed by applying filters.
motifs.filter("b.age > 30").show()


from pyspark.sql.functions import col, lit, udf, when
from pyspark.sql.types import IntegerType
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")

# Query on sequence, with state (cnt)
#  (a) Define method for updating state given the next element of the motif.
sumFriends =\
  lambda cnt,relationship: when(relationship == "friend", cnt+1).otherwise(cnt)
#  (b) Use sequence operation to apply method to sequence of elements in motif.
#      In this case, the elements are the 3 edges.
condition =\
  reduce(lambda cnt,e: sumFriends(cnt, col(e).relationship), ["ab", "bc", "cd"], lit(0))
#  (c) Apply filter to DataFrame.
chainWith2Friends2 = chain4.where(condition >= 2)
chainWith2Friends2.show()


from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

# Select subgraph of users older than 30, and edges of type "friend"
v2 = g.vertices.filter("age > 30")
e2 = g.edges.filter("relationship = 'friend'")
g2 = GraphFrame(v2, e2)


from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

# Select subgraph based on edges "e" of type "follow"
# pointing from a younger user "a" to an older user "b".
paths = g.find("(a)-[e]->(b)")\
  .filter("e.relationship = 'follow'")\
  .filter("a.age < b.age")
# "paths" contains vertex info. Extract the edges.
e2 = paths.select("e.src", "e.dst", "e.relationship")
# In Spark 1.5+, the user may simplify this call:
#  val e2 = paths.select("e.*")

# Construct the subgraph
g2 = GraphFrame(g.vertices, e2)



from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

# Search from "Esther" for users of age < 32.
paths = g.bfs("name = 'Esther'", "age < 32")
paths.show()

# Specify edge filters or max path lengths.
g.bfs("name = 'Esther'", "age < 32",\
  edgeFilter="relationship != 'friend'", maxPathLength=3)


from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

results = g.shortestPaths(landmarks=["a", "d"])
results.select("id", "distances").show()

from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

results = g.triangleCount()
results.select("id", "count").show()

from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

# Save vertices and edges as Parquet to some location.
g.vertices.write.parquet("hdfs://myLocation/vertices")
g.edges.write.parquet("hdfs://myLocation/edges")

# Load the vertices and edges back.
sameV = sqlContext.read.parquet("hdfs://myLocation/vertices")
sameE = sqlContext.read.parquet("hdfs://myLocation/edges")

# Create an identical GraphFrame.
sameG = GraphFrame(sameV, sameE)

