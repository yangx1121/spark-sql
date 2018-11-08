
# coding: utf-8

# # Social characteristics of the Marvel Universe

# In[1]:

sc


# In[2]:

spark


# ## Read in the data.
# 
# Run the right cell depending on the platform being used.
# 
# For AWS EMR use:

# In[3]:

file = sc.textFile("s3://bigdatateaching/marvel/porgat.txt")


# For Databricks on Azure use:

# In[ ]:

file = sc.textFile("wasbs://marvel@bigdatateaching.blob.core.windows.net/porgat.txt")


# ## Clean The Data
# 
# The data file is in three parts, with the single file:
# 
# * Marvel Characters
# * Publications
# * Relationships between the two
# 
# We need to pre-process the file before we can use it. The following operations are all RDD operations.

# Let's look at the file.

# Count the number of records in the file.

# In[42]:

file.count()


# Define a new RDD that removes the headers from the file. The headers are lines that begin with a star.

# In[5]:

noHeaders = file.filter(lambda x: len(x)>0 and x[0]!='*')


# Look at the first 5 records of noHeaders

# In[12]:

noHeaders.take(20)


# Extract a pair from each line:  the leading integer and a string for the rest of the line

# In[25]:

paired = noHeaders.map(lambda l:  l.partition(' ')).filter(lambda t:  len(t)==3 and len(t[0])>0 and len(t[2])>0).map(lambda t: (int(t[0]), t[2]))


# In[44]:

paired.take(10)


# In[45]:

p = paired.collect()


# In[48]:

p[30000]


# Filter relationships as they do not start with quotes, then split the integer list

# In[ ]:




# In[26]:

scatteredRelationships = paired.filter(lambda (charId, text):  text[0]!='"').map(lambda (charId, text): (charId, [int(x) for x in text.split(' ')]))


# Relationships for the same character id sometime spans more than a line in the file, so let's group them together

# In[49]:

scatteredRelationships.take(10)


# In[27]:

relationships = scatteredRelationships.reduceByKey(lambda pubList1, pubList2: pubList1 + pubList2)


# Filter non-relationships as they start with quotes ; remove the quotes

# In[28]:

nonRelationships = paired.filter(lambda (index, text):  text[0]=='"').map(lambda (index, text):  (index, text[1:-1].strip()))


# Characters stop at a certain line (part of the initial header ; we hardcode it here)

# In[29]:

characters = nonRelationships.filter(lambda (charId, name): charId<=6486)


# Publications starts after the characters

# In[30]:

publications = nonRelationships.filter(lambda (charId, name): charId>6486)


# The following cells begin to use SparkSQL. 
# 
# Spark SQL works with Data Frames which are a kind of “structured” RDD or an “RDD with schema”.
# 
# The integration between the two works by creating a RDD of Row (a type from pyspark.sql) and then creating a Data Frame from it.
# 
# The Data Frames can then be registered as views.  It is those views we’ll query using Spark SQL.

# In[31]:

from pyspark.sql import Row


# Let's create dataframes out of the RDDs and register them as temporary views for SQL to use
# 

# In[32]:

#  Relationships has a list as a component, let's flat that
flatRelationships = relationships.flatMap(lambda (charId, pubList):  [(charId, pubId) for pubId in pubList])


# In[33]:

#  Let's map the relationships to an RDD of rows in order to create a data frame out of it
relationshipsDf = spark.createDataFrame(flatRelationships.map(lambda t: Row(charId=t[0], pubId=t[1])))


# In[34]:

#  Register relationships as a temporary view
relationshipsDf.createOrReplaceTempView("relationships")


# In[52]:

spark.sql("select count(*) from relationships").collect()


# In[53]:

df = spark.sql("select count(*) from relationships")


# In[54]:

df


# In[55]:

df_python = spark.sql("select count(*) from relationships").collect()


# In[56]:

df_python


# In[50]:

relationshipsDf.count()


# In[58]:

spark.sql("select * from relationships limit 10").show()


# In[40]:

r2 = spark.read.csv("s3://bigdatateaching/marvel/relationship",header=True)


# In[41]:

r2.show()


# In[54]:

#  Let's do the same for characters
charactersDf = spark.createDataFrame(characters.map(lambda t:  Row(charId=t[0], name=t[1])))
charactersDf.createOrReplaceTempView("characters")


# In[55]:

#  and for publications
publicationsDf = spark.createDataFrame(publications.map(lambda t:  Row(pubId=t[0], name=t[1])))
publicationsDf.createOrReplaceTempView("publications")


# In[56]:

relationshipsDf.show(10)


# The following cell is the standard way of running a SQL query on Spark. This query ranks Marvel characters in duo in order of join-appearances in publications. 

# In[40]:

df1 = spark.sql("""SELECT c1.name AS name1, c2.name AS name2, sub.charId1, sub.charId2, sub.pubCount
FROM
(
  SELECT r1.charId AS charId1, r2.charId AS charId2, COUNT(r1.pubId, r2.pubId) AS pubCount
  FROM relationships AS r1
  CROSS JOIN relationships AS r2
  WHERE r1.charId < r2.charId
  AND r1.pubId=r2.pubId
  GROUP BY r1.charId, r2.charId
) AS sub
INNER JOIN characters c1 ON c1.charId=sub.charId1
INNER JOIN characters c2 ON c2.charId=sub.charId2
ORDER BY sub.pubCount DESC
LIMIT 10""").cache()


# In[42]:

df1.take(10)


# In[43]:

df2 = spark.sql("""
SELECT c1.name AS name1, c2.name AS name2, c3.name AS name3, sub.charId1, sub.charId2, sub.charId3, sub.pubCount
FROM
(
  SELECT r1.charId AS charId1, r2.charId AS charId2, r3.charId AS charId3, COUNT(r1.pubId, r2.pubId, r3.pubId) AS pubCount
  FROM relationships AS r1
  CROSS JOIN relationships AS r2
  CROSS JOIN relationships AS r3
  WHERE r1.charId < r2.charId
  AND r2.charId < r3.charId
  AND r1.pubId=r2.pubId
  AND r2.pubId=r3.pubId
  GROUP BY r1.charId, r2.charId, r3.charId
) AS sub
INNER JOIN characters c1 ON c1.charId=sub.charId1
INNER JOIN characters c2 ON c2.charId=sub.charId2
INNER JOIN characters c3 ON c3.charId=sub.charId3
ORDER BY sub.pubCount DESC
LIMIT 10
""").cache()


# In[44]:

df2.show(10)


# In[ ]:

sc.stop()


# This lab was adapted from [https://vincentlauzon.com/2018/01/24/azure-databricks-spark-sql-data-frames/](https://vincentlauzon.com/2018/01/24/azure-databricks-spark-sql-data-frames/)

# Saving a DataFrame to a csv
# ```
# publicationsDf.write\
#     .format("com.databricks.spark.csv")\
#     .option("header", "true")\
#     .save("s3://bigdatateaching/marvel/publication")
# ```
