#!/usr/bin/env python
# coding: utf-8

# ## Data Warehousing Project 
# ### Python-Powered Performance Analysis of NoSQL Document Stores: Couchbase, CouchDB, and MongoDB
# 
# #### Collaborator: Chinmay Dhamapurkar, Bhagavat, Kundana Chowdhary

# 

# In[1]:


import pandas as pd


# In[2]:


df=pd.read_csv("spotify_songs.csv")


# In[3]:


df


# In[4]:


import os
print(os.getcwd())


# In[5]:


import pymongo


# # Connecting MongoDB and starting the Queries

# In[6]:


from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017')
db = client['project']  
collection = db['spotify']  


# In[7]:


documents = collection.find()
for doc in documents:
    print(doc)


# In[8]:


avg_query = [
    {"$group": {
        "_id": "$playlist_genre", 
        "AveragePopularity": {"$avg": "$track_popularity"}
    }}
]

try:
    result = collection.aggregate(avg_query)
    for doc in result:
        print(doc)
except Exception as e:
    print(f"An error occurred: {e}")


# In[9]:


import time

start_time = time.time()

avg_query = [
    {"$group": {
        "_id": "$playlist_genre", 
        "AveragePopularity": {"$avg": "$track_popularity"}
    }}
]
result = list(collection.aggregate(avg_query))

end_time = time.time()
query_execution_time = end_time - start_time
print(f"Query Execution Time: {query_execution_time} seconds")

for doc in result:
    print(doc)


# In[10]:


import time
import pymongo
from pymongo import MongoClient
import psutil 

def run_query_and_measure_performance(collection, query, query_type="find"):
    """
    Run a MongoDB query and measure its performance.

    :param collection: MongoDB collection object
    :param query: Query to execute
    :param query_type: Type of query - 'find' or 'aggregate'
    :return: Query result and performance metrics
    """
    start_cpu = psutil.cpu_percent()
    start_memory = psutil.virtual_memory().used
    start_time = time.time()

    try:
        if query_type == "find":
            result = list(collection.find(query))
        elif query_type == "aggregate":
            result = list(collection.aggregate(query))
        else:
            raise ValueError("Invalid query type")
    except Exception as e:
        result = str(e)

    end_time = time.time()
    end_cpu = psutil.cpu_percent()
    end_memory = psutil.virtual_memory().used

    performance_metrics = {
        "execution_time": end_time - start_time,
        "cpu_usage": end_cpu - start_cpu,
        "memory_usage": end_memory - start_memory
    }

    return result, performance_metrics


# In[11]:


import psutil


# In[12]:


test_query = [
    {"$group": {
        "_id": "$playlist_genre", 
        "AverageDanceability": {"$avg": "$danceability"}
    }}
]
query_type = "aggregate" 

result, metrics = run_query_and_measure_performance(collection, test_query, query_type)

print("Query Result:", result)
print("Performance Metrics:", metrics)


# ## This one will find the top 3 most popular tracks for each playlist_genre, based on track_popularity:

# In[13]:


complex_query = [
    {
        "$sort": {"track_popularity": -1} 
    },
    {
        "$group": {
            "_id": "$playlist_genre",  
            "top_tracks": {
                "$push": {  
                    "track_name": "$track_name",
                    "artist": "$track_artist",
                    "popularity": "$track_popularity"
                }
            }
        }
    },
    {
        "$project": {
            "top_tracks": {"$slice": ["$top_tracks", 3]}  
        }
    }
]
query_type = "aggregate"
result, metrics = run_query_and_measure_performance(collection, complex_query, query_type)
print("Query Result:", result)
print("Performance Metrics:", metrics)


# ## Find Unique Artists in Each Genre:

# In[14]:


unique_artists_query = [
    {"$group": {
        "_id": "$playlist_genre",
        "unique_artists": {"$addToSet": "$track_artist"}
    }}
]

result, metrics = run_query_and_measure_performance(collection, complex_query, query_type)
print("Query Result:", result)
print("Performance Metrics:", metrics)


# In[15]:


avg_loudness_tempo_query = [
    {"$group": {
        "_id": "$key",
        "average_loudness": {"$avg": "$loudness"},
        "average_tempo": {"$avg": "$tempo"}
    }}
]
result, metrics = run_query_and_measure_performance(collection, complex_query, query_type)
print("Query Result:", result)
print("Performance Metrics:", metrics)


# In[16]:


common_key_query = [
    {"$group": {
        "_id": {"genre": "$playlist_genre", "key": "$key"},
        "count": {"$sum": 1}
    }},
    {"$sort": {"count": -1}},
    {"$group": {
        "_id": "$_id.genre",
        "most_common_key": {"$first": "$_id.key"},
        "occurrences": {"$first": "$count"}
    }}
]
result, metrics = run_query_and_measure_performance(collection, complex_query, query_type)
print("Query Result:", result)
print("Performance Metrics:", metrics)


# In[17]:


danceable_tracks_query = {
    "danceability": {"$gt": 0.8},
    "energy": {"$lt": 0.5}
}
result, metrics = run_query_and_measure_performance(collection, complex_query, query_type)
print("Query Result:", result)
print("Performance Metrics:", metrics)


# In[18]:


longest_tracks_query = [
    {"$sort": {"duration_ms": -1}},
    {"$group": {
        "_id": "$playlist_genre",
        "top_longest_tracks": {"$push": {
            "track_name": "$track_name",
            "duration_ms": "$duration_ms"
        }}
    }},
    {"$project": {
        "top_longest_tracks": {"$slice": ["$top_longest_tracks", 5]}
    }}
]
result, metrics = run_query_and_measure_performance(collection, complex_query, query_type)
print("Query Result:", result)
print("Performance Metrics:", metrics)


# ## Connecting Couchbase and starting the Queries

# In[19]:


get_ipython().system('pip install couchbase')


# In[20]:


import couchbase


# In[21]:


import logging
logging.basicConfig(level=logging.DEBUG)


# In[22]:


get_ipython().system('pip show couchbase')


# In[23]:


from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from couchbase.cluster import Cluster
cluster = Cluster('couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com', 
                  ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123')))

bucket = cluster.bucket('spotify')
collection = bucket.default_collection()


# In[24]:


from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from couchbase.cluster import Cluster
cluster = Cluster('couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com', 
                  ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123')))
try:

    result = bucket.default_collection().get("test_document")
    print("Connection successful. Bucket 'spotify' is accessible.")
except Exception as e:
    print(f"An error occurred: {e}")


# In[25]:


query = "SELECT * FROM `spotify` LIMIT 10"

try:
    result = cluster.query(query)
    for row in result:
        print(row)
except Exception as e:
    print(f"An error occurred while fetching data: {e}")


# In[28]:


import couchbase
from couchbase.cluster import Cluster
from couchbase.options import QueryOptions
import time

def run_query_and_measure_performance(cluster, query, query_params=None):
    try:
      
        start_time = time.time()
        result = cluster.query(query, QueryOptions(named_parameters=query_params))
        rows = [row for row in result]
        num_rows = len(rows)
        end_time = time.time()
        execution_time = end_time - start_time
        metrics = {
            "execution_time": execution_time,
            "num_rows": num_rows
        }

        return rows, metrics

    except Exception as e:
        return None, {"error": str(e)}

query = "SELECT * FROM `spotify` LIMIT 5"
query_params = None  

result, metrics = run_query_and_measure_performance(cluster, query, query_params)


if result:
    print("Query Result:")
    for row in result:
        print(row)
    print("Performance Metrics:", metrics)
else:
    print("An error occurred while fetching data:", metrics["error"])


# In[29]:


import couchbase
from couchbase.cluster import Cluster
from couchbase.options import QueryOptions
import time

def run_query_and_measure_performance(cluster, query, query_params=None):
    try:
        
        start_time = time.time()

       
        result = cluster.query(query, QueryOptions(named_parameters=query_params))

        
        rows = [row for row in result]
        num_rows = len(rows)

       
        end_time = time.time()
        execution_time = end_time - start_time

       
        metrics = {
            "execution_time": execution_time,
            "num_rows": num_rows
        }

        return rows, metrics

    except Exception as e:
        return None, {"error": str(e)}

query = """
SELECT playlist_genre, track_name, duration_ms
FROM `spotify`
ORDER BY playlist_genre, duration_ms DESC
"""




query_params = None 

result, metrics = run_query_and_measure_performance(cluster, query, query_params)


if result:
    print("Query Result:")
    for row in result:
        print(row)
    print("Performance Metrics:", metrics)
else:
    print("An error occurred while fetching data:", metrics["error"])


# In[30]:


from pymongo import MongoClient
import time

def mongodb_query_performance(db, collection_name, limit):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db]
    collection = db[collection_name]

    start_time = time.time()

    results = list(collection.find().limit(limit))

    end_time = time.time()
    execution_time = end_time - start_time

    return results, execution_time


# In[31]:


from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator
import time

def couchbase_query_performance(bucket_name, query, limit=10):
    
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com', 
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )
    bucket = cluster.bucket(bucket_name)
    collection = bucket.default_collection()

    
    query = f"{query} LIMIT {limit}"

    start_time = time.time()

    result = cluster.query(query)
    rows = [row for row in result]

    end_time = time.time()
    execution_time = end_time - start_time

    return rows, execution_time


# In[32]:


mongodb_results, mongodb_time = mongodb_query_performance('project', 'spotify', 10)
print("MongoDB Execution Time:", mongodb_time)
print("MongoDB Results:", mongodb_results)


couchbase_query = "SELECT * FROM `spotify`"
couchbase_results, couchbase_time = couchbase_query_performance(
    'spotify', 
    couchbase_query,
    10
)
print("Couchbase Execution Time:", couchbase_time)
print("Couchbase Results:", couchbase_results)



# ## Comparison of performance execution times between Couchbase and Mongodb

# In[33]:


print("Couchbase Execution Time:", couchbase_time)
print("MongoDB Execution Time:", mongodb_time)


# In[34]:


import statistics

def measure_query_performance_multiple_runs(query_function, *args, runs=10):
    execution_times = []

    for _ in range(runs):
        _, time_taken = query_function(*args)
        execution_times.append(time_taken)

    avg_time = statistics.mean(execution_times)
    median_time = statistics.median(execution_times)
    std_dev = statistics.stdev(execution_times) if len(execution_times) > 1 else 0

    return {
        "average_time": avg_time,
        "median_time": median_time,
        "standard_deviation": std_dev,
        "all_times": execution_times
    }


mongodb_performance = measure_query_performance_multiple_runs(mongodb_query_performance, 'project', 'spotify', 10)
couchbase_performance = measure_query_performance_multiple_runs(couchbase_query_performance, 'spotify', "SELECT * FROM `spotify`", 10)

print("MongoDB Performance:", mongodb_performance)
print("Couchbase Performance:", couchbase_performance)


# ## Initial Conclusions for MongoDB vs Couchbase
# 
# ## MongoDB Performance Metrics
# Average Time:0.0107
# 
# 0.0107 seconds. This is the mean time taken for the query to execute across all runs. It's a general indicator of the query's typical performance.
# 
# Median Time: 0.0052 seconds. This is the middle value in the set of recorded times, providing a measure that is less sensitive to outliers than the average
# 
# Standard Deviation: 0.0104 seconds. This value indicates the variability or spread in the execution times. A lower standard deviation means the times are more consistent.
# 
# All Times: A list of individual execution times for each run, ranging from about 0.003 to 0.031 seconds. This spread shows some variability in execution times, possibly due to environmental factors like CPU load, memory usage, or network latency.
# 
# 
# ## Couchbase Performance Metrics
# Average Time: 0.2578
# 
# 0.2578 seconds. The mean time for query execution, significantly higher than MongoDB, indicating slower performance for this particular query.
# 
# Median Time: 0.2427 seconds. The middle value in the execution times, again showing slower performance compared to MongoDB.
# 
# Standard Deviation: 0.0373 seconds. There's a smaller relative spread in execution times compared to MongoDB, indicating more consistent performance  across runs.
# 
# All Times: The execution times range from about 0.234 to 0.351 seconds. The times are more clustered around the average compared to MongoDB, showing less variability.
#  
# 
# ## Interpretation
# 1)MongoDB shows faster performance for this query, with a lower average execution time. However, there's noticeable variability in its execution times, as shown by the higher standard deviation and wider range in the "all times" list.
# 
# 2)Couchbase demonstrates slower query performance on average. Despite this, its execution times are more consistent, with a lower standard deviation.
# 
# ## Considerations
# 1)Test Conditions: Ensure both databases are operating under similar conditions (e.g., data size, network latency, system load).
# 
# 2)Query Nature: The specific query and data structure might favor one database over the other.
# 
# 3)Optimization: Both databases might not be equally optimized. Indexing, query design, and configuration can significantly affect performance.
# 
# 4)Resource Utilization: The tests don't account for resource usage (like CPU, memory), which is also crucial for overall performance assessment.
# 
# 5)Data Freshness: Whether data is cached or requires disk access can affect the performance.

# In[35]:


def mongodb_complex_query_performance(db, collection_name, limit):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db]
    collection = db[collection_name]

    start_time = time.time()

   
    query_filter = {"year": {"$gte": 2000}}  
    results = list(collection.find(query_filter).sort("year", -1).limit(limit))  

    end_time = time.time()
    execution_time = end_time - start_time

    return results, execution_time


# In[36]:


def couchbase_complex_query_performance(bucket_name, limit=10):
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com', 
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )

   
    query = """
    SELECT * FROM `spotify`
    WHERE year >= 2000
    ORDER BY year DESC
    LIMIT $limit
    """

    start_time = time.time()

    result = cluster.query(query, QueryOptions(named_parameters={'limit': limit}))
    rows = [row for row in result]

    end_time = time.time()
    execution_time = end_time - start_time

    return rows, execution_time


# In[37]:


mongodb_complex_results, mongodb_complex_time = mongodb_complex_query_performance('project', 'spotify', 10)
print("MongoDB Complex Query Execution Time:", mongodb_complex_time)
print("MongoDB Complex Query Results:", mongodb_complex_results)


couchbase_complex_results, couchbase_complex_time = couchbase_complex_query_performance('spotify', 10)
print("Couchbase Complex Query Execution Time:", couchbase_complex_time)
print("Couchbase Complex Query Results:", couchbase_complex_results)


# In[38]:


def mongodb_revised_complex_query_performance(db, collection_name, limit):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db]
    collection = db[collection_name]

    start_time = time.time()

    query_filter = {"someField": {"$exists": True}}  
    results = list(collection.find(query_filter).sort("someField", -1).limit(limit))

    end_time = time.time()
    execution_time = end_time - start_time

    return results, execution_time


# In[39]:


def couchbase_revised_complex_query_performance(bucket_name, limit=10):
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com', 
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )

   
    query = """
    SELECT * FROM `spotify`
    WHERE someField IS NOT MISSING
    ORDER BY someField DESC
    LIMIT $limit
    """

    start_time = time.time()

    result = cluster.query(query, QueryOptions(named_parameters={'limit': limit}))
    rows = [row for row in result]

    end_time = time.time()
    execution_time = end_time - start_time

    return rows, execution_time


# In[40]:


mongodb_revised_results, mongodb_revised_time = mongodb_revised_complex_query_performance('project', 'spotify', 10)
print("MongoDB Revised Complex Query Execution Time:", mongodb_revised_time)
print("MongoDB Revised Complex Query Results:", mongodb_revised_results)


couchbase_revised_results, couchbase_revised_time = couchbase_revised_complex_query_performance('spotify', 10)
print("Couchbase Revised Complex Query Execution Time:", couchbase_revised_time)
print("Couchbase Revised Complex Query Results:", couchbase_revised_results)


# In[41]:


from pymongo import MongoClient
import time

def mongodb_complex_query_performance(db, collection_name, limit):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db]
    collection = db[collection_name]

    start_time = time.time()

    
    query_filter = {"year": {"$gte": 2000}} 
    results = list(collection.find(query_filter).sort("year", -1).limit(limit)) 

    end_time = time.time()
    execution_time = end_time - start_time

    return results, execution_time


# In[42]:


from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator
import time

def couchbase_complex_query_performance(bucket_name, query, limit=10):
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com', 
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )
    bucket = cluster.bucket(bucket_name)
    collection = bucket.default_collection()

    
    query = f"{query} LIMIT {limit}"

    start_time = time.time()

    result = cluster.query(query)
    rows = [row for row in result]

    end_time = time.time()
    execution_time = end_time - start_time

    return rows, execution_time


# In[43]:


mongodb_query = {"year": {"$gte": 2000}}
mongodb_results, mongodb_time = mongodb_complex_query_performance('project', 'spotify', 10)
print("MongoDB Execution Time:", mongodb_time)
print("MongoDB Results:", mongodb_results)


# In[44]:


couchbase_query = "SELECT * FROM `spotify` WHERE year >= 2000 ORDER BY year DESC"
couchbase_results, couchbase_time = couchbase_complex_query_performance('spotify', couchbase_query, 10)
print("Couchbase Execution Time:", couchbase_time)
print("Couchbase Results:", couchbase_results)


# In[45]:


from pymongo import MongoClient
import time

def mongodb_simple_query_performance(db, collection_name, limit):
    try:
        client = MongoClient('mongodb://localhost:27017')
        db = client[db]
        collection = db[collection_name]

        start_time = time.time()

        
        results = list(collection.find({"track_artist": "Ed Sheeran"}).limit(limit))

        end_time = time.time()
        execution_time = end_time - start_time

        return results, execution_time
    except Exception as e:
        print(f"An error occurred: {e}")
        return [], 0


mongodb_results, mongodb_time = mongodb_simple_query_performance('your_db_name', 'spotify', 10)
print("MongoDB Execution Time:", mongodb_time)
print("MongoDB Results:", mongodb_results)


# In[46]:


def couchbase_simple_query_performance(bucket_name, limit=10):
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com', 
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )
    bucket = cluster.bucket(bucket_name)
    collection = bucket.default_collection()

    query = f"{query} LIMIT {limit}"

    start_time = time.time()

    result = cluster.query(query)
    rows = [row for row in result]

    end_time = time.time()
    execution_time = end_time - start_time

    return rows, execution_time


couchbase_query = "SELECT id,track_artist,title FROM `spotify` WHERE track_artist = 'Ed Sheeran'"
couchbase_results, couchbase_time = couchbase_query_performance(
    'spotify', 
    couchbase_query,
    10
)
print("Couchbase Execution Time:", couchbase_time)
print("Couchbase Results:", couchbase_results)


# In[47]:


mongodb_results, mongodb_time = mongodb_simple_query_performance('project', 'spotify', 10)
print("MongoDB Execution Time:", mongodb_time)
print("MongoDB Results:", mongodb_results)


# In[48]:


import pymongo
from pymongo import MongoClient
import time
import psutil

def mongodb_complex_query_performance(db_name, collection_name):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db_name]
    collection = db[collection_name]

    start_time = time.time()
    start_cpu = psutil.cpu_percent(interval=None)
    start_memory = psutil.virtual_memory().used

    pipeline = [
        {"$group": {
            "_id": "$playlist_genre",
            "average_danceability": {"$avg": "$danceability"},
            "average_energy": {"$avg": "$energy"}
        }},
        {"$sort": {"average_danceability": -1}}
    ]

    results = list(collection.aggregate(pipeline))

    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=None)
    end_memory = psutil.virtual_memory().used

    performance_metrics = {
        "execution_time": end_time - start_time,
        "cpu_usage": end_cpu - start_cpu,
        "memory_usage": end_memory - start_memory
    }

    return results, performance_metrics


mongodb_complex_results, mongodb_performance_metrics = mongodb_complex_query_performance('project', 'spotify')
print("MongoDB Complex Query Execution Time:", mongodb_performance_metrics['execution_time'])
print("MongoDB CPU Usage:", mongodb_performance_metrics['cpu_usage'])
print("MongoDB Memory Usage:", mongodb_performance_metrics['memory_usage'])
print("MongoDB Complex Query Results:", mongodb_complex_results)


# In[49]:


from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator
import time
import psutil

def couchbase_complex_query_performance(bucket_name, query):
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com', 
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )
    bucket = cluster.bucket(bucket_name)
    collection = bucket.default_collection()

    start_time = time.time()
    start_cpu = psutil.cpu_percent(interval=None)
    start_memory = psutil.virtual_memory().used

    result = cluster.query(query)
    rows = [row for row in result]

    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=None)
    end_memory = psutil.virtual_memory().used

    performance_metrics = {
        "execution_time": end_time - start_time,
        "cpu_usage": end_cpu - start_cpu,
        "memory_usage": end_memory - start_memory
    }

    return rows, performance_metrics


couchbase_complex_query = """
SELECT playlist_genre, AVG(danceability) AS average_danceability, AVG(energy) AS average_energy
FROM `spotify`
GROUP BY playlist_genre
ORDER BY average_danceability DESC
"""
couchbase_complex_results, couchbase_performance_metrics = couchbase_complex_query_performance(
    'spotify', 
    couchbase_complex_query
)
print("Couchbase Complex Query Execution Time:", couchbase_performance_metrics['execution_time'])
print("Couchbase CPU Usage:", couchbase_performance_metrics['cpu_usage'])
print("Couchbase Memory Usage:", couchbase_performance_metrics['memory_usage'])
print("Couchbase Complex Query Results:", couchbase_complex_results)


# In[50]:


def mongodb_indexing_performance_test(db, collection_name, index_field):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db]
    collection = db[collection_name]

    start_time = time.time()
    collection.create_index([(index_field, pymongo.ASCENDING)])
    end_time = time.time()

    return end_time - start_time


indexing_duration = mongodb_indexing_performance_test('project', 'spotify', 'track_artist')
print("MongoDB Indexing Duration:", indexing_duration)


# In[51]:


def couchbase_create_index(bucket_name, index_name, field_name):
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com', 
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )

    
    create_index_query = f"CREATE INDEX {index_name} ON `{bucket_name}`({field_name})"

    start_time = time.time()

    try:
        result = cluster.query(create_index_query)
        for row in result:  
            pass
    except Exception as e:
        print(f"An error occurred: {e}")
        return -1

    end_time = time.time()
    return end_time - start_time


index_duration = couchbase_create_index('spotify', 'index_track_artist', 'track_artist')
print("Couchbase Index Creation Duration:", index_duration)


# The comparison of index creation times between MongoDB and Couchbase in your test case provides insightful data about the performance characteristics of both databases in terms of indexing efficiency. Here's a summary of what you've found and what it implies:
# 
# ## MongoDB Index Creation:
# Duration: 0.306607723236084 seconds (approximately 0.307 seconds)
# Implications: This relatively quick index creation time in MongoDB indicates efficient handling of index operations. MongoDB's performance here suggests that it can rapidly update its internal data structures to accommodate the new index, even in the presence of existing data.
# 
# ## Couchbase Index Creation:
# Duration: 4.897703647613525 seconds (approximately 4.898 seconds)
# Implications: The index creation time in Couchbase is notably longer than MongoDB in this instance. This could be due to several factors like the nature of Couchbase’s indexing mechanism, the current load on the Couchbase server, or the complexity of the index being created. Couchbase uses a global secondary index (GSI) which is powerful but can take more time to build, especially if the index is being built on a large dataset or a heavily loaded server.

# In[52]:


def mongodb_read_write_throughput_test(db_name, collection_name, base_document, num_operations):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db_name]
    collection = db[collection_name]

    
    start_time_write = time.time()
    for i in range(num_operations):
        
        document = base_document.copy()
        document["unique_field"] = i
        collection.insert_one(document)
    end_time_write = time.time()

    write_duration = end_time_write - start_time_write

    
    start_time_read = time.time()
    for i in range(num_operations):
        collection.find_one({"unique_field": i})
    end_time_read = time.time()

    read_duration = end_time_read - start_time_read

    return write_duration, read_duration



write_duration, read_duration = mongodb_read_write_throughput_test('project', 'spotify', {"track_artist": "Ed Sheeran"},10)
print("MongoDB Write Duration:", write_duration)
print("MongoDB Read Duration:", read_duration)


# In[53]:


from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
import time
import uuid  

def couchbase_read_write_throughput_test(bucket_name, base_document, num_operations):
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com',
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )
    bucket = cluster.bucket(bucket_name)
    collection = bucket.default_collection()

    
    start_time_write = time.time()
    for i in range(num_operations):
        unique_key = f"doc-{uuid.uuid4()}"  
        collection.upsert(unique_key, base_document)
    end_time_write = time.time()

    write_duration = end_time_write - start_time_write

    
    start_time_read = time.time()
    for i in range(num_operations):
        collection.get(unique_key)  
    end_time_read = time.time()

    read_duration = end_time_read - start_time_read

    return write_duration, read_duration


base_document = {"track_artist": "Ed Sheeran"}  
write_duration, read_duration = couchbase_read_write_throughput_test('spotify', base_document, 10)
print("Couchbase Write Duration:", write_duration)
print("Couchbase Read Duration:", read_duration)


# ## MongoDB Results:
# Write Duration: 0.01658344268798828 seconds
# Read Duration: 0.2733619213104248 seconds
# Conclusion for MongoDB:
# 
# MongoDB exhibits extremely fast write operations, as indicated by the very short write duration. This suggests efficient handling of insert operations.
# The read operations take significantly longer than write operations. This could be due to the overhead involved in searching and retrieving documents, especially if no specific index is used for the read queries.
# The disparity between read and write times could also be influenced by the size and structure of the documents, as well as the database's current state, such as cache warmth and load.
# 
# ## Couchbase Results:
# Write Duration: 0.5156705379486084 seconds
# Read Duration: 0.49604082107543945 seconds
# Conclusion for Couchbase:
# 
# Couchbase shows a more balanced performance between read and write operations, with both operations taking a comparable amount of time.
# The write duration in Couchbase is longer than MongoDB, which could be attributed to Couchbase’s distributed architecture and the additional network overhead or disk I/O involved in ensuring data consistency and durability.
# The read duration is slightly less than the write duration, suggesting effective retrieval mechanisms, possibly benefiting from Couchbase’s in-memory capabilities.
# 
# ## Overall Conclusion:
# MongoDB seems to excel in write performance but takes longer for read operations in this specific test. This could make it well-suited for write-heavy applications where insert speed is crucial.
# Couchbase, while slightly slower in writes, shows a more uniform performance between reads and writes. This balanced performance might be beneficial in applications where both read and write speeds are equally important.
# These results are indicative of the general performance patterns of each database under the specific test conditions. However, real-world performance can vary based on factors like data model complexity, hardware specifications, network environment, and specific database configurations.
# 
# ## Important Considerations:
# These tests were conducted under specific conditions and with a limited scope. For a comprehensive understanding, consider running a broader range of tests, including those that simulate real-world application scenarios.
# Both databases offer a range of tuning and optimization options that can significantly affect performance, such as indexing strategies, replication configurations, and hardware optimizations.
# It's essential to align the database choice with the specific requirements and constraints of your application or use case.

# In[54]:


from pymongo import MongoClient
from threading import Thread
import time

def mongodb_concurrent_operation(db_name, collection_name, operation, num_threads):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db_name]
    collection = db[collection_name]

    def worker():
        for _ in range(100): 
            operation(collection)

    threads = [Thread(target=worker) for _ in range(num_threads)]
    start_time = time.time()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    end_time = time.time()

    return end_time - start_time


def read_operation(collection):
    collection.find_one({"track_artist": "Ed Sheeran"})


def write_operation(collection):
    collection.insert_one({"track_artist": "Ed Sheeran", "title": "Test Song"})


read_duration = mongodb_concurrent_operation('your_db_name', 'spotify', read_operation, 10)
print("MongoDB Concurrent Read Duration:", read_duration)


write_duration = mongodb_concurrent_operation('your_db_name', 'spotify', write_operation, 10)
print("MongoDB Concurrent Write Duration:", write_duration)


# In[55]:


from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
from threading import Thread
import time
import uuid

def couchbase_concurrent_operation(bucket_name, operation, num_threads, num_operations):
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com',
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )
    bucket = cluster.bucket(bucket_name)
    collection = bucket.default_collection()

    def worker():
        for _ in range(num_operations):
            operation(collection)

    threads = [Thread(target=worker) for _ in range(num_threads)]
    start_time = time.time()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    end_time = time.time()

    return end_time - start_time


def read_operation(collection):
    try:
        collection.get(f"doc-{uuid.uuid4()}")  
    except Exception as e:
        pass 


def write_operation(collection):
    document_key = f"doc-{uuid.uuid4()}" 
    collection.upsert(document_key, {"type": "test", "content": "This is a test document."})


read_duration = couchbase_concurrent_operation('spotify', read_operation, 10, 100)
print("Couchbase Concurrent Read Duration:", read_duration)


write_duration = couchbase_concurrent_operation('spotify', write_operation, 10, 100)
print("Couchbase Concurrent Write Duration:", write_duration)


# 
# The provided durations represent the time taken to execute concurrent read and write operations in both Couchbase and MongoDB. Here's a brief explanation of the results:
# 
# ## Couchbase Results:
# 
# Concurrent Read Duration (Couchbase): Approximately 2.89 seconds.
# 
# This duration indicates that it took approximately 2.89 seconds for multiple threads to complete their concurrent read operations in the Couchbase database.
# 
# It suggests that the Couchbase database might have a slightly higher response time under concurrent read load.
# Concurrent Write Duration (Couchbase): Approximately 3.89 seconds.
# 
# This duration represents the time it took for multiple threads to complete their concurrent write operations in the Couchbase database.
# It indicates that Couchbase performed write operations in approximately 3.89 seconds under concurrent load.
# 
# ## MongoDB Results:
# 
# Concurrent Read Duration (MongoDB): Approximately 0.24 seconds.
# 
# This duration signifies that it took around 0.24 seconds for multiple threads to complete their concurrent read operations in the MongoDB database.
# 
# MongoDB demonstrated faster response times for read operations compared to Couchbase in this test.
# Concurrent Write Duration (MongoDB): Approximately 0.25 seconds.
# 
# This duration represents the time it took for multiple threads to complete their concurrent write operations in the MongoDB database.
# 
# MongoDB showed similar performance to Couchbase in terms of write operations under concurrent load.
# 
# In summary, MongoDB exhibited faster response times for concurrent read operations compared to Couchbase, while both databases demonstrated similar performance for concurrent write operations. These results provide insights into how each database system handles concurrent operations and can help in choosing the right database for specific use cases.

# In[56]:


import matplotlib.pyplot as plt
import numpy as np


operations = ['Read', 'Write']
mongodb_times = [0.24, 0.25]  
couchbase_times = [2.89, 3.89] 

x = np.arange(len(operations))  
width = 0.35 

fig, ax = plt.subplots()
bars1 = ax.bar(x - width/2, mongodb_times, width, label='MongoDB')
bars2 = ax.bar(x + width/2, couchbase_times, width, label='Couchbase')


ax.set_ylabel('Execution Time (s)')
ax.set_title('Execution Time by Operation and Database')
ax.set_xticks(x)
ax.set_xticklabels(operations)
ax.legend()

plt.show()


# ## Comparison using plots

# In[57]:


import pymongo
from pymongo import MongoClient
import time
import psutil

def mongodb_complex_query_performance(db_name, collection_name):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db_name]
    collection = db[collection_name]

    
    execution_times = []
    cpu_usages = []
    memory_usages = []

    
    pipeline = [
        {"$group": {
            "_id": "$playlist_genre",
            "average_danceability": {"$avg": "$danceability"},
            "average_energy": {"$avg": "$energy"}
        }},
        {"$sort": {"average_danceability": -1}}
    ]

    
    start_time = time.time()
    start_cpu = psutil.cpu_percent(interval=None)
    start_memory = psutil.virtual_memory().used

   
    for doc in collection.aggregate(pipeline):
        current_time = time.time()
        execution_times.append(current_time - start_time)
        cpu_usages.append(psutil.cpu_percent(interval=None) - start_cpu)
        memory_usages.append(psutil.virtual_memory().used - start_memory)

    
    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=None)
    end_memory = psutil.virtual_memory().used

    performance_metrics = {
        "execution_times": execution_times,
        "cpu_usages": cpu_usages,
        "memory_usages": memory_usages,
        "total_execution_time": end_time - start_time,
        "total_cpu_usage": end_cpu - start_cpu,
        "total_memory_usage": end_memory - start_memory
    }

    return performance_metrics


mongodb_performance_metrics = mongodb_complex_query_performance('project', 'spotify')
print("MongoDB Complex Query Total Execution Time:", mongodb_performance_metrics['total_execution_time'])
print("MongoDB Total CPU Usage:", mongodb_performance_metrics['total_cpu_usage'])
print("MongoDB Total Memory Usage:", mongodb_performance_metrics['total_memory_usage'])


# In[58]:


from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator
import time
import psutil

def couchbase_complex_query_performance(bucket_name, query):
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com', 
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )
    bucket = cluster.bucket(bucket_name)
    collection = bucket.default_collection()

    
    execution_times = []
    cpu_usages = []
    memory_usages = []

    
    start_time = time.time()
    start_cpu = psutil.cpu_percent(interval=None)
    start_memory = psutil.virtual_memory().used

    
    result = cluster.query(query)
    for row in result:
        current_time = time.time()
        execution_times.append(current_time - start_time)
        cpu_usages.append(psutil.cpu_percent(interval=None) - start_cpu)
        memory_usages.append(psutil.virtual_memory().used - start_memory)

   
    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=None)
    end_memory = psutil.virtual_memory().used

    performance_metrics = {
        "execution_times": execution_times,
        "cpu_usages": cpu_usages,
        "memory_usages": memory_usages,
        "total_execution_time": end_time - start_time,
        "total_cpu_usage": end_cpu - start_cpu,
        "total_memory_usage": end_memory - start_memory
    }

    return performance_metrics


couchbase_performance_metrics = couchbase_complex_query_performance(
    'spotify', 
    couchbase_complex_query
)
print("Couchbase Complex Query Total Execution Time:", couchbase_performance_metrics['total_execution_time'])
print("Couchbase Total CPU Usage:", couchbase_performance_metrics['total_cpu_usage'])
print("Couchbase Total Memory Usage:", couchbase_performance_metrics['total_memory_usage'])


# In[59]:


import matplotlib.pyplot as plt

def plot_performance_metrics(mongodb_metrics, couchbase_metrics, metric_name):
    """
    Plots the performance metrics for MongoDB and Couchbase.

    Parameters:
    mongodb_metrics (dict): A dictionary containing performance metrics for MongoDB.
    couchbase_metrics (dict): A dictionary containing performance metrics for Couchbase.
    metric_name (str): The name of the metric to plot ('execution_times', 'cpu_usages', 'memory_usages').
    """

    
    mongodb_data = mongodb_metrics[metric_name]
    couchbase_data = couchbase_metrics[metric_name]

    
    time_intervals = range(len(mongodb_data))

   
    plt.figure(figsize=(10, 6))
    plt.plot(time_intervals, mongodb_data, label='MongoDB', marker='o')
    plt.plot(time_intervals, couchbase_data, label='Couchbase', marker='x')

    
    plt.xlabel('Time Intervals')
    plt.ylabel(metric_name.replace('_', ' ').title())
    plt.title(f'{metric_name.replace("_", " ").title()} over Time')
    plt.legend()

    
    plt.show()


plot_performance_metrics(mongodb_performance_metrics, couchbase_performance_metrics, 'execution_times')
plot_performance_metrics(mongodb_performance_metrics, couchbase_performance_metrics, 'cpu_usages')
plot_performance_metrics(mongodb_performance_metrics, couchbase_performance_metrics, 'memory_usages')


# In[60]:


import matplotlib.pyplot as plt

def plot_execution_time(mongodb_time, couchbase_time):
    databases = ['MongoDB', 'Couchbase']
    execution_times = [mongodb_time, couchbase_time]

    plt.figure(figsize=(8, 6))
    plt.bar(databases, execution_times, color=['blue', 'green'])
    plt.xlabel('Database')
    plt.ylabel('Execution Time (seconds)')
    plt.title('Execution Time Comparison')
    plt.show()


mongodb_time = 1.5 
couchbase_time = 2.0  

plot_execution_time(mongodb_time, couchbase_time)


# In[61]:


def plot_execution_time():
    databases = ['MongoDB', 'Couchbase']
    execution_times = [0.06682085990905762, 1.8513853549957275] 

    plt.figure(figsize=(8, 6))
    plt.bar(databases, execution_times, color=['blue', 'green'])
    plt.xlabel('Database')
    plt.ylabel('Execution Time (seconds)')
    plt.title('Execution Time Comparison')
    plt.show()

plot_execution_time()


# In[62]:


def plot_cpu_usage():
    databases = ['MongoDB', 'Couchbase']
    cpu_usages = [-7.2, -13.1]  

    plt.figure(figsize=(8, 6))
    plt.bar(databases, cpu_usages, color=['red', 'orange'])
    plt.xlabel('Database')
    plt.ylabel('CPU Usage (%)')
    plt.title('CPU Usage Comparison')
    plt.show()

plot_cpu_usage()


# In[63]:


def plot_memory_usage():
    databases = ['MongoDB', 'Couchbase']
    memory_usages = [-6393856, 331776]  

    plt.figure(figsize=(8, 6))
    plt.bar(databases, memory_usages, color=['purple', 'pink'])
    plt.xlabel('Database')
    plt.ylabel('Memory Usage (bytes)')
    plt.title('Memory Usage Comparison')
    plt.show()

plot_memory_usage()


# ## Simple Query vs Complex Query
# 

# In[64]:


def mongodb_simple_query_performance(db_name, collection_name):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db_name]
    collection = db[collection_name]

   
    simple_query = {"track_artist": "Ed Sheeran"}

   
    execution_times, cpu_usages, memory_usages = [], [], []

   
    start_time = time.time()
    start_cpu = psutil.cpu_percent(interval=None)
    start_memory = psutil.virtual_memory().used

    
    for doc in collection.find(simple_query):
        current_time = time.time()
        execution_times.append(current_time - start_time)
        cpu_usages.append(psutil.cpu_percent(interval=None) - start_cpu)
        memory_usages.append(psutil.virtual_memory().used - start_memory)

    
    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=None)
    end_memory = psutil.virtual_memory().used

    return {
        "execution_times": execution_times,
        "cpu_usages": cpu_usages,
        "memory_usages": memory_usages,
        "total_execution_time": end_time - start_time,
        "total_cpu_usage": end_cpu - start_cpu,
        "total_memory_usage": end_memory - start_memory
    }


# In[65]:


def couchbase_simple_query_performance(bucket_name, simple_query):
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com', 
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )
    bucket = cluster.bucket(bucket_name)

    
    execution_times, cpu_usages, memory_usages = [], [], []

    
    start_time = time.time()
    start_cpu = psutil.cpu_percent(interval=None)
    start_memory = psutil.virtual_memory().used

   
    result = cluster.query(simple_query)
    for row in result:
        current_time = time.time()
        execution_times.append(current_time - start_time)
        cpu_usages.append(psutil.cpu_percent(interval=None) - start_cpu)
        memory_usages.append(psutil.virtual_memory().used - start_memory)

    
    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=None)
    end_memory = psutil.virtual_memory().used

    return {
        "execution_times": execution_times,
        "cpu_usages": cpu_usages,
        "memory_usages": memory_usages,
        "total_execution_time": end_time - start_time,
        "total_cpu_usage": end_cpu - start_cpu,
        "total_memory_usage": end_memory - start_memory
    }


# In[66]:


mongodb_simple_metrics = mongodb_simple_query_performance('your_db_name', 'your_collection_name')
mongodb_complex_metrics = mongodb_complex_query_performance('your_db_name', 'your_collection_name')


# In[67]:


def mongodb_simple_query_performance():
    client = MongoClient('mongodb://localhost:27017')
    db = client['project']
    collection = db['spotify']

  
    simple_query = {"track_artist": "Ed Sheeran"}

    
    execution_times, cpu_usages, memory_usages = [], [], []

   
    start_time = time.time()
    start_cpu = psutil.cpu_percent(interval=None)
    start_memory = psutil.virtual_memory().used

    
    for doc in collection.find(simple_query):
        current_time = time.time()
        execution_times.append(current_time - start_time)
        cpu_usages.append(psutil.cpu_percent(interval=None) - start_cpu)
        memory_usages.append(psutil.virtual_memory().used - start_memory)

    
    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=None)
    end_memory = psutil.virtual_memory().used

    return {
        "execution_times": execution_times,
        "cpu_usages": cpu_usages,
        "memory_usages": memory_usages,
        "total_execution_time": end_time - start_time,
        "total_cpu_usage": end_cpu - start_cpu,
        "total_memory_usage": end_memory - start_memory
    }

mongodb_simple_metrics = mongodb_simple_query_performance()


# In[68]:


def couchbase_simple_query_performance():
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com', 
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )
    bucket = cluster.bucket('spotify')


    simple_query = "SELECT * FROM `spotify` LIMIT 10"

   
    execution_times, cpu_usages, memory_usages = [], [], []

  
    start_time = time.time()
    start_cpu = psutil.cpu_percent(interval=None)
    start_memory = psutil.virtual_memory().used

   
    result = cluster.query(simple_query)
    for row in result:
        current_time = time.time()
        execution_times.append(current_time - start_time)
        cpu_usages.append(psutil.cpu_percent(interval=None) - start_cpu)
        memory_usages.append(psutil.virtual_memory().used - start_memory)

  
    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=None)
    end_memory = psutil.virtual_memory().used

    return {
        "execution_times": execution_times,
        "cpu_usages": cpu_usages,
        "memory_usages": memory_usages,
        "total_execution_time": end_time - start_time,
        "total_cpu_usage": end_cpu - start_cpu,
        "total_memory_usage": end_memory - start_memory
    }
couchbase_simple_metrics = couchbase_simple_query_performance()


# In[69]:


mongodb_simple_metrics = mongodb_simple_query_performance()
mongodb_complex_metrics = mongodb_complex_query_performance('project', 'spotify')
print("MongoDB Simple Query Execution Time:", mongodb_simple_metrics['total_execution_time'])
print("MongoDB Complex Query Execution Time:", mongodb_complex_metrics['total_execution_time'])


# In[70]:


couchbase_complex_query = """
SELECT playlist_genre, AVG(danceability) AS average_danceability, AVG(energy) AS average_energy
FROM `spotify`
GROUP BY playlist_genre
ORDER BY average_danceability DESC
"""
couchbase_simple_metrics = couchbase_simple_query_performance()
couchbase_complex_metrics = couchbase_complex_query_performance('spotify', couchbase_complex_query)
print("Couchbase Simple Query Execution Time:", couchbase_simple_metrics['total_execution_time'])
print("Couchbase Complex Query Execution Time:", couchbase_complex_metrics['total_execution_time'])


# In[71]:


import matplotlib.pyplot as plt

def plot_execution_time_comparison():
    categories = ['MongoDB Simple', 'MongoDB Complex', 'Couchbase Simple', 'Couchbase Complex']
    execution_times = [
        mongodb_simple_metrics['total_execution_time'],
        mongodb_complex_metrics['total_execution_time'],
        couchbase_simple_metrics['total_execution_time'],
        couchbase_complex_metrics['total_execution_time']
    ]

    plt.figure(figsize=(10, 6))
    plt.bar(categories, execution_times, color=['blue', 'green', 'red', 'purple'])
    plt.xlabel('Query Type')
    plt.ylabel('Execution Time (seconds)')
    plt.title('Execution Time Comparison')
    plt.show()

plot_execution_time_comparison()


# In[72]:


def plot_cpu_usage_comparison():
    categories = ['MongoDB Simple', 'MongoDB Complex', 'Couchbase Simple', 'Couchbase Complex']
    cpu_usages = [
        mongodb_simple_metrics['total_cpu_usage'],
        mongodb_complex_metrics['total_cpu_usage'],
        couchbase_simple_metrics['total_cpu_usage'],
        couchbase_complex_metrics['total_cpu_usage']
    ]

    plt.figure(figsize=(10, 6))
    plt.bar(categories, cpu_usages, color=['blue', 'green', 'red', 'purple'])
    plt.xlabel('Query Type')
    plt.ylabel('CPU Usage (%)')
    plt.title('CPU Usage Comparison')
    plt.show()

plot_cpu_usage_comparison()


# In[73]:


def plot_memory_usage_comparison():
    categories = ['MongoDB Simple', 'MongoDB Complex', 'Couchbase Simple', 'Couchbase Complex']
    memory_usages = [
        mongodb_simple_metrics['total_memory_usage'],
        mongodb_complex_metrics['total_memory_usage'],
        couchbase_simple_metrics['total_memory_usage'],
        couchbase_complex_metrics['total_memory_usage']
    ]

    plt.figure(figsize=(10, 6))
    plt.bar(categories, memory_usages, color=['blue', 'green', 'red', 'purple'])
    plt.xlabel('Query Type')
    plt.ylabel('Memory Usage (bytes)')
    plt.title('Memory Usage Comparison')
    plt.show()

plot_memory_usage_comparison()


# ## Query Throughput Measurement Function

# In[74]:


def mongodb_query_throughput(db_name, collection_name, duration_seconds=10):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db_name]
    collection = db[collection_name]

    query_count = 0
    end_time = time.time() + duration_seconds

    while time.time() < end_time:
        try:
            
            for _ in collection.find({}, {'_id': 1}).limit(10):
                pass
            query_count += 1
        except Exception as e:
            print(f"Error during MongoDB query execution: {e}")
            break

    return query_count


mongodb_query_count = mongodb_query_throughput('project', 'spotify', duration_seconds=10)
print(f"MongoDB Query Throughput: {mongodb_query_count} queries in 10 seconds")


# In[75]:


def couchbase_query_throughput(bucket_name, query, duration_seconds=10):
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com',
        ClusterOptions(PasswordAuthenticator('spotify', 'Ambadnyabapu@123'))
    )

    query_count = 0
    end_time = time.time() + duration_seconds

    while time.time() < end_time:
        try:
           
            result = cluster.query(query, QueryOptions(timeout=1))
            for _ in result:
                pass
            query_count += 1
        except Exception as e:
            print(f"Error during Couchbase query execution: {e}")
            break

    return query_count


couchbase_query_count = couchbase_query_throughput('spotify', "SELECT META().id FROM `spotify` LIMIT 10", duration_seconds=10)
print(f"Couchbase Query Throughput: {couchbase_query_count} queries in 10 seconds")


# # Connecting CouchDB and starting the Execution
# 

# In[76]:


import couchdb
couch = couchdb.Server('http://admin:12345678@127.0.0.1:5984/')
db = couch['dbspotify']  


# In[77]:


for i, doc_id in enumerate(db):
    if i < 10:  
        doc = db[doc_id]  
        print(f"Document {i + 1}: {doc.get('_id')} - {doc.get('playlist_genre')} - {doc.get('track_popularity')}")
    else:
        break  
        


# In[78]:


import numpy as np


df = df.where(pd.notnull(df), None)


df = df.replace([np.inf, -np.inf], None)
import numpy as np


df = df.where(pd.notnull(df), None)


df = df.replace([np.inf, -np.inf], None)
records = df.to_dict(orient='records')
for record in records:
    try:
        db.save(record)
    except Exception as e:
        print(f"An error occurred: {e}")


# In[80]:


import couchdb
import time



map_function = """
function(doc) {
    if (doc.playlist_genre && doc.track_popularity) {
        emit(doc.playlist_genre, doc.track_popularity);
    }
}
"""


reduce_function = """
function(keys, values, rereduce) {
    if (rereduce) {
        return sum(values) / values.length;
    } else {
        return sum(values) / values.length;
    }
}
"""


design_doc = {
    "_id": "_design/averagePopularity",
    "language": "javascript",
    "views": {
        "averagePopularity": {
            "map": map_function,
            "reduce": reduce_function
        }
    }
}


db.save(design_doc)


start_time = time.time()


result = db.view('averagePopularity/averagePopularity', group=True)


end_time = time.time()
query_execution_time = end_time - start_time
print(f"Query Execution Time: {query_execution_time} seconds")


for row in result:
    print({"_id": row.key, "AveragePopularity": row.value})


# In[82]:


import time
import couchdb
import psutil  

def run_query_and_measure_performance(db, map_function):
    """
    Run a CouchDB query and measure its performance.

    :param db: CouchDB database object
    :param map_function: Map function for the view
    :return: Query result and performance metrics
    """
    start_cpu = psutil.cpu_percent()
    start_memory = psutil.virtual_memory().used
    start_time = time.time()

    
    design_doc = {
        "_id": "_design/genre_tracks",
        "language": "javascript",
        "views": {
            "by_genre": {
                "map": map_function
            }
        }
    }
    try:
        db.save(design_doc)
    except couchdb.http.ResourceConflict:
       
        pass

    
    result = db.view('genre_tracks/by_genre')

    end_time = time.time()
    end_cpu = psutil.cpu_percent()
    end_memory = psutil.virtual_memory().used

    performance_metrics = {
        "execution_time": end_time - start_time,
        "cpu_usage": end_cpu - start_cpu,
        "memory_usage": end_memory - start_memory
    }

    return result, performance_metrics


map_function = """
function(doc) {
    if (doc.playlist_genre && doc.track_name && doc.track_artist && doc.track_popularity) {
        emit(doc.playlist_genre, {
            track_name: doc.track_name,
            artist: doc.track_artist,
            popularity: doc.track_popularity
        });
    }
}
"""

result, metrics = run_query_and_measure_performance(db, map_function)

top_tracks_per_genre = {}
for row in result:
    genre = row.key
    track = row.value

    if genre not in top_tracks_per_genre:
        top_tracks_per_genre[genre] = []
    
    top_tracks_per_genre[genre].append(track)

for genre, tracks in top_tracks_per_genre.items():
  
    tracks.sort(key=lambda x: -x['popularity'])
    top_tracks_per_genre[genre] = tracks[:3]


for genre, top_tracks in top_tracks_per_genre.items():
    print(f"Genre: {genre}, Top Tracks: {top_tracks}")

print("Performance Metrics:", metrics)


# # Comparisons of Performance for Couchdb,Mongodb and couchbase
# 
# ## First Query: Select * from spotify limit 10

# In[83]:


from pymongo import MongoClient
import time
import psutil

def mongodb_query_performance_test(db_name, collection_name):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db_name]
    collection = db[collection_name]

   
    start_cpu = psutil.cpu_percent()
    start_memory = psutil.virtual_memory().used
    start_time = time.time()

  
    result = list(collection.find().limit(10))

    end_time = time.time()
   
    cpu_usage_during_query = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    time_taken = end_time - start_time
    memory_usage = end_memory - start_memory

    return result, time_taken, cpu_usage_during_query, memory_usage
mongodb_result, mongodb_time, mongodb_cpu, mongodb_memory = mongodb_query_performance_test('project', 'spotify')
print("MongoDB Query Performance:", mongodb_time, mongodb_cpu, mongodb_memory)
print("MongoDB Results:", mongodb_result[:10]) 


# In[84]:


def couchbase_query_performance_test(bucket_name):
    authenticator = PasswordAuthenticator('spotify', 'Ambadnyabapu@123')
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com',
        ClusterOptions(authenticator)
    )
    start_cpu = psutil.cpu_percent(interval=1)
    start_memory = psutil.virtual_memory().used
    start_time = time.time()

    query = f"SELECT * FROM `{bucket_name}` LIMIT 10"
    result = cluster.query(query)

   
    rows = list(result)

    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    return rows, end_time - start_time, end_cpu, end_memory - start_memory
couchbase_result, couchbase_time, couchbase_cpu, couchbase_memory = couchbase_query_performance_test('spotify')
print("Couchbase Query Performance:", couchbase_time, couchbase_cpu, couchbase_memory)
print("Couchbase Results:", couchbase_result[:10])


# In[90]:


map_function = """
function(doc) {
    emit(doc._id, doc);
}

"""
def couchdb_query_performance_test(db):
    start_cpu = psutil.cpu_percent(interval=1)
    start_memory = psutil.virtual_memory().used
    start_time = time.time()


    design_doc_id = "_design/my_design_doc"
    view_name = "my_view"

   
    design_doc = db.get(design_doc_id, {'_id': design_doc_id})
    design_doc['views'] = {view_name: {'map': map_function}}
    db.save(design_doc)

    
    try:
        result = db.view(f'{design_doc_id}/_view/{view_name}', limit=10)
    except couchdb.ResourceNotFound:
        print("Design document or view not found in the database.")
        return None, 0, 0, 0

    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

  
    documents = [{'id': row.id, 'doc': row.value} for row in result]

    return documents, end_time - start_time, end_cpu, end_memory - start_memory

couchdb_result, couchdb_time, couchdb_cpu, couchdb_memory = couchdb_query_performance_test(db)
if couchdb_result is not None:
    print("CouchDB Query Performance:", couchdb_time, couchdb_cpu, couchdb_memory)
    print("CouchDB Results:", couchdb_result[:10])
else:
    print("Query failed.")


# # Second Query: CRUD Operations

# In[103]:


sample_doc = {
    'acousticness': 0.5,
    'danceability': 0.8,
    'duration_ms': 200000,
    'energy': 0.7,
    'instrumentalness': 0.0,
    'key': 5,
    'liveness': 0.1,
    'loudness': -5.0,
    'mode': 1,
    'playlist_genre': 'pop',
    'playlist_id': 'sample_playlist_id',
    'playlist_name': 'Sample Playlist',
    'playlist_subgenre': 'pop',
    'speechiness': 0.05,
    'tempo': 120.0,
    'track_album_id': 'sample_album_id',
    'track_album_name': 'Sample Album',
    'track_album_release_date': '2020-01-01',
    'track_artist': 'Sample Artist',
    'track_id': 'sample_track_id',
    'track_name': 'Sample Track',
    'track_popularity': 50,
    'valence': 0.6
}




from pymongo import MongoClient
import time
import psutil
import uuid

def mongodb_crud_operations(db_name, collection_name, sample_doc):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db_name]
    collection = db[collection_name]

    
    sample_doc['_id'] = str(uuid.uuid4())

    
    start_time = time.time()
    try:
        collection.insert_one(sample_doc)
    except DuplicateKeyError:
        print("Document with the same _id already exists.")
    create_time = time.time() - start_time

    
    start_time = time.time()
    read_result = list(collection.find({"playlist_genre": "pop"}).limit(10))
    read_time = time.time() - start_time

   
    start_time = time.time()
    update_result = collection.update_many({"track_artist": "Ed Sheeran"}, {"$set": {"track_popularity": 80}})
    update_time = time.time() - start_time

   
    start_time = time.time()
    delete_result = collection.delete_many({"track_artist": "Maroon 5"})
    delete_time = time.time() - start_time

    return read_result, create_time, read_time, update_time, delete_time

mongodb_read, mongodb_create_time, mongodb_read_time, mongodb_update_time, mongodb_delete_time = mongodb_crud_operations('project', 'spotify', sample_doc)
print("MongoDB Create Time:", mongodb_create_time)
print("MongoDB Read Time:", mongodb_read_time, "Read Results:", mongodb_read[:2]) 
print("MongoDB Update Time:", mongodb_update_time)
print("MongoDB Delete Time:", mongodb_delete_time)


# In[108]:


sample_doc = {
    'acousticness': 0.5,
    'danceability': 0.8,
    'duration_ms': 200000,
    'energy': 0.7,
    'instrumentalness': 0.0,
    'key': 5,
    'liveness': 0.1,
    'loudness': -5.0,
    'mode': 1,
    'playlist_genre': 'pop',
    'playlist_id': 'sample_playlist_id',
    'playlist_name': 'Sample Playlist',
    'playlist_subgenre': 'pop',
    'speechiness': 0.05,
    'tempo': 120.0,
    'track_album_id': 'sample_album_id',
    'track_album_name': 'Sample Album',
    'track_album_release_date': '2020-01-01',
    'track_artist': 'Sample Artist',
    'track_id': 'sample_track_id',
    'track_name': 'Sample Track',
    'track_popularity': 50,
    'valence': 0.6
}

from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
import time
import psutil
import uuid

def couchbase_crud_operations(bucket_name, sample_doc):
    authenticator = PasswordAuthenticator('spotify', 'Ambadnyabapu@123')
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com',
        ClusterOptions(authenticator)
    )
    bucket = cluster.bucket(bucket_name)
    collection = bucket.default_collection()

    
    unique_id = str(uuid.uuid4())
    sample_doc["id"] = unique_id
    start_time = time.time()
    collection.upsert(unique_id, sample_doc)
    create_time = time.time() - start_time

    
    start_time = time.time()
    read_query = f"SELECT * FROM `{bucket_name}` WHERE playlist_genre = 'pop' LIMIT 10"
    read_result = list(cluster.query(read_query))
    read_time = time.time() - start_time

    start_time = time.time()
    update_query = f"UPDATE `{bucket_name}` SET track_popularity = 80 WHERE track_artist = 'Ed Sheeran'"
    update_result = cluster.query(update_query)
    list(update_result) 
    update_time = time.time() - start_time

   
    start_time = time.time()
    delete_query = f"DELETE FROM `{bucket_name}` WHERE track_artist = 'Maroon 5'"
    delete_result = cluster.query(delete_query)
    list(delete_result) 
    delete_time = time.time() - start_time

    return read_result, create_time, read_time, update_time, delete_time

couchbase_read, couchbase_create_time, couchbase_read_time, couchbase_update_time, couchbase_delete_time = couchbase_crud_operations('spotify', sample_doc)
print("Couchbase Create Time:", couchbase_create_time)
print("Couchbase Read Time:", couchbase_read_time, "Read Results:", couchbase_read[:2]) 
print("Couchbase Update Time:", couchbase_update_time)
print("Couchbase Delete Time:", couchbase_delete_time)


# In[123]:


import couchdb
import time
import uuid
sample_doc = {
    'acousticness': 0.5,
    'danceability': 0.8,
    'duration_ms': 200000,
    'energy': 0.7,
    'instrumentalness': 0.0,
    'key': 5,
    'liveness': 0.1,
    'loudness': -5.0,
    'mode': 1,
    'playlist_genre': 'pop',
    'playlist_id': 'sample_playlist_id',
    'playlist_name': 'Sample Playlist',
    'playlist_subgenre': 'pop',
    'speechiness': 0.05,
    'tempo': 120.0,
    'track_album_id': 'sample_album_id',
    'track_album_name': 'Sample Album',
    'track_album_release_date': '2020-01-01',
    'track_artist': 'Sample Artist',
    'track_id': 'sample_track_id',
    'track_name': 'Sample Track',
    'track_popularity': 50,
    'valence': 0.6
}

def couchdb_crud_operations(db, sample_doc):
    sample_doc['_id'] = str(uuid.uuid4())
    start_time = time.time()
    db.save(sample_doc)
    create_time = time.time() - start_time
    start_time = time.time()
    read_result = list(db.view('my_design_doc/by_genre', key='pop', limit=10))
    read_time = time.time() - start_time
    start_time = time.time()
    docs_to_update = db.view('my_design_doc/by_artist', key='Ed Sheeran', limit=10)
    for row in docs_to_update:
        doc = db[row.id]
        doc['track_popularity'] = 80
        db.save(doc)
    update_time = time.time() - start_time
    start_time = time.time()
    docs_to_delete = db.view('my_design_doc/by_artist', key='Maroon 5', limit=10)
    for row in docs_to_delete:
        db.delete(db[row.id])
    delete_time = time.time() - start_time

    return read_result, create_time, read_time, update_time, delete_time
couchdb_read, couchdb_create_time, couchdb_read_time, couchdb_update_time, couchdb_delete_time = couchdb_crud_operations(db, sample_doc)
print("CouchDB Create Time:", couchdb_create_time)
print("CouchDB Read Time:", couchdb_read_time, "Read Results:", couchdb_read[:2])  
print("CouchDB Update Time:", couchdb_update_time)
print("CouchDB Delete Time:", couchdb_delete_time)


# ## Third Query Aggregation function

# In[149]:


def mongodb_query_performance_test(db_name, collection_name):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db_name]
    collection = db[collection_name]
    start_cpu = psutil.cpu_percent()
    start_memory = psutil.virtual_memory().used
    start_time = time.time()
    pipeline = [
        {"$group": {
            "_id": None,
            "avg_danceability": {"$avg": "$danceability"},
            "sum_energy": {"$sum": "$energy"}
        }}
    ]
    result = list(collection.aggregate(pipeline))
    end_time = time.time()
    cpu_usage_during_query = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    time_taken = end_time - start_time
    memory_usage = end_memory - start_memory

    return result, time_taken, cpu_usage_during_query, memory_usage


mongodb_result, mongodb_time, mongodb_cpu, mongodb_memory = mongodb_query_performance_test('project', 'spotify')
print("MongoDB Query Performance:", mongodb_time, mongodb_cpu, mongodb_memory)
print("MongoDB Results:", mongodb_result[:10])  


# In[150]:


def couchbase_query_performance_test(bucket_name):
    authenticator = PasswordAuthenticator('spotify', 'Ambadnyabapu@123')
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com',
        ClusterOptions(authenticator)
    )
    start_cpu = psutil.cpu_percent(interval=1)
    start_memory = psutil.virtual_memory().used
    start_time = time.time()

    query = f"""
        SELECT AVG(danceability) AS avg_danceability, SUM(energy) AS sum_energy 
        FROM `{bucket_name}`
    """
    result = cluster.query(query)
    rows = list(result)

    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    return rows, end_time - start_time, end_cpu, end_memory - start_memory


couchbase_result, couchbase_time, couchbase_cpu, couchbase_memory = couchbase_query_performance_test('spotify')
print("Couchbase Query Performance:", couchbase_time, couchbase_cpu, couchbase_memory)
print("Couchbase Results:", couchbase_result[:10])


# In[163]:


import couchdb
import time
import psutil

def couchdb_query_performance_test(db):
    start_cpu = psutil.cpu_percent(interval=1)
    start_memory = psutil.virtual_memory().used
    start_time = time.time()
    design_doc_id = "_design/my_design_doc"
    view_name = "my_view"

    map_function = """
    function(doc) {
        emit(doc._id, {
            danceability: doc.danceability,
            energy: doc.energy
        });
    }
    """

   
    design_doc = db.get(design_doc_id, {'_id': design_doc_id})
    design_doc['views'] = {view_name: {'map': map_function}}
    db.save(design_doc)
    try:
        result = db.view(f'{design_doc_id}/_view/{view_name}')
    except couchdb.ResourceNotFound:
        print("Design document or view not found in the database.")
        return None, 0, 0, 0
    total_danceability, total_energy, count = 0, 0, 0
    for row in result:
        doc = row.value
        total_danceability += doc.get('danceability', 0)
        total_energy += doc.get('energy', 0)
        count += 1

    avg_danceability = total_danceability / count if count else 0
    sum_energy = total_energy

    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    return [{"avg_danceability": avg_danceability, "sum_energy": sum_energy}], end_time - start_time, end_cpu, end_memory - start_memory
couchdb_result, couchdb_time, couchdb_cpu, couchdb_memory = couchdb_query_performance_test(db)
if couchdb_result is not None:
    print("CouchDB Query Performance:", couchdb_time, couchdb_cpu, couchdb_memory)
    print("CouchDB Results:", couchdb_result)
else:
    print("Query failed.")


# ## Query 4: Find the top 5 most popular tracks in the 'r&b' genre and subgenre 'new jack swing', and calculate the average danceability and energy of these tracks.

# In[174]:


def mongodb_query_performance_test(db_name, collection_name):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db_name]
    collection = db[collection_name]
    start_cpu = psutil.cpu_percent()
    start_memory = psutil.virtual_memory().used
    start_time = time.time()
    pipeline = [
    {"$match": {"playlist_genre": "r&b", "playlist_subgenre": "new jack swing"}},
    {"$sort": {"track_popularity": -1}},
    {"$limit": 5},
    {"$group": {
        "_id": None,
        "avg_danceability": {"$avg": "$danceability"},
        "avg_energy": {"$avg": "$energy"}
    }}
]
    result = list(collection.aggregate(pipeline))


    end_time = time.time()
   
    cpu_usage_during_query = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    time_taken = end_time - start_time
    memory_usage = end_memory - start_memory

    return result, time_taken, cpu_usage_during_query, memory_usage


mongodb_result, mongodb_time, mongodb_cpu, mongodb_memory = mongodb_query_performance_test('project', 'spotify')
print("MongoDB Query Performance:", mongodb_time, mongodb_cpu, mongodb_memory)
print("MongoDB Results:", mongodb_result[:10])


# In[172]:


def couchbase_query_performance_test(bucket_name):
    authenticator = PasswordAuthenticator('spotify', 'Ambadnyabapu@123')
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com',
        ClusterOptions(authenticator)
    )
    start_cpu = psutil.cpu_percent(interval=1)
    start_memory = psutil.virtual_memory().used
    start_time = time.time()

    query = f"""
    SELECT AVG(sub.danceability) AS avg_danceability, AVG(sub.energy) AS avg_energy
    FROM (
        SELECT danceability, energy
        FROM `{bucket_name}`
        WHERE playlist_genre = 'r&b' AND playlist_subgenre = 'new jack swing'
        ORDER BY track_popularity DESC
        LIMIT 5
    ) AS sub
"""
    result = cluster.query(query)
    rows = list(result)

    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    return rows, end_time - start_time, end_cpu, end_memory - start_memory
couchbase_result, couchbase_time, couchbase_cpu, couchbase_memory = couchbase_query_performance_test('spotify')
print("Couchbase Query Performance:", couchbase_time, couchbase_cpu, couchbase_memory)
print("Couchbase Results:", couchbase_result[:10])


# In[176]:


import couchdb
import time
import psutil

def couchdb_query_performance_test(db):
    start_cpu = psutil.cpu_percent(interval=1)
    start_memory = psutil.virtual_memory().used
    start_time = time.time()

    
    design_doc_id = "_design/track_analysis"
    view_name = "genre_subgenre_popularity"

  
    map_function = """
    function(doc) {
        emit([doc.playlist_genre, doc.playlist_subgenre, doc.track_popularity], {danceability: doc.danceability, energy: doc.energy});
    }
    """

  
    design_doc = db.get(design_doc_id, {'_id': design_doc_id})
    design_doc['views'] = {view_name: {'map': map_function}}
    db.save(design_doc)


    genre = 'r&b'
    subgenre = 'new jack swing'
    result = db.view(f'{design_doc_id}/_view/{view_name}', startkey=[genre, subgenre], endkey=[genre, subgenre, {}])

    
    filtered_data = sorted(result, key=lambda x: x.key[2], reverse=True)[:5]
    total_danceability, total_energy = 0, 0
    for item in filtered_data:
        total_danceability += item.value['danceability']
        total_energy += item.value['energy']

    avg_danceability = total_danceability / len(filtered_data) if filtered_data else 0
    avg_energy = total_energy / len(filtered_data) if filtered_data else 0

    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    time_taken = end_time - start_time
    memory_usage = end_memory - start_memory

    return {"avg_danceability": avg_danceability, "avg_energy": avg_energy}, time_taken, end_cpu, memory_usage

couchdb_result, couchdb_time, couchdb_cpu, couchdb_memory = couchdb_query_performance_test(db)
if couchdb_result is not None:
    print("CouchDB Query Performance:", couchdb_time, couchdb_cpu, couchdb_memory)
    print("CouchDB Results:", couchdb_result)
else:
    print("Query failed.")


# ## Query 5: Find the and calculate the maximum 'track_popularity' for each genre.

# In[182]:


def mongodb_query_performance_test(db_name, collection_name):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db_name]
    collection = db[collection_name]

    
    start_cpu = psutil.cpu_percent()
    start_memory = psutil.virtual_memory().used
    start_time = time.time()


 
    pipeline = [
    {"$group": {
        "_id": "$playlist_genre",
        
        "max_popularity": {"$max": "$track_popularity"}
    }}
]
    result = list(collection.aggregate(pipeline))



    end_time = time.time()
   
    cpu_usage_during_query = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    time_taken = end_time - start_time
    memory_usage = end_memory - start_memory

    return result, time_taken, cpu_usage_during_query, memory_usage
mongodb_result, mongodb_time, mongodb_cpu, mongodb_memory = mongodb_query_performance_test('project', 'spotify')
print("MongoDB Query Performance:", mongodb_time, mongodb_cpu, mongodb_memory)
print("MongoDB Results:", mongodb_result[:10])  


# In[183]:


def couchbase_query_performance_test(bucket_name):
    authenticator = PasswordAuthenticator('spotify', 'Ambadnyabapu@123')
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com',
        ClusterOptions(authenticator)
    )
    start_cpu = psutil.cpu_percent(interval=1)
    start_memory = psutil.virtual_memory().used
    start_time = time.time()

    query = f"""
    SELECT playlist_genre, COUNT(*) AS total_tracks
    FROM `{bucket_name}`
    GROUP BY playlist_genre
"""
    result = cluster.query(query)
    rows = list(result)

    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    return rows, end_time - start_time, end_cpu, end_memory - start_memory


couchbase_result, couchbase_time, couchbase_cpu, couchbase_memory = couchbase_query_performance_test('spotify')
print("Couchbase Query Performance:", couchbase_time, couchbase_cpu, couchbase_memory)
print("Couchbase Results:", couchbase_result[:10])


# In[186]:


import couchdb
import time
import psutil

def couchdb_query_genre_popularity(db):
    start_cpu = psutil.cpu_percent(interval=1)
    start_memory = psutil.virtual_memory().used
    start_time = time.time()

    
    design_doc_id = "_design/genre_popularity_analysis"
    view_name = "max_popularity_by_genre"

   
    map_function = """
    function(doc) {
        emit(doc.playlist_genre, doc.track_popularity);
    }
    """
    reduce_function = """
    function(keys, values, rereduce) {
        return Math.max.apply(null, values);
    }
    """

   
    design_doc = db.get(design_doc_id, {'_id': design_doc_id})
    if 'views' not in design_doc or view_name not in design_doc['views']:
        design_doc['views'] = design_doc.get('views', {})
        design_doc['views'][view_name] = {'map': map_function, 'reduce': reduce_function}
        db.save(design_doc)


    result = db.view(f'{design_doc_id}/_view/{view_name}', group=True)


    genre_popularity = [{"genre": row.key, "max_popularity": row.value} for row in result]

    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    time_taken = end_time - start_time
    memory_usage = end_memory - start_memory

    return genre_popularity, time_taken, end_cpu, memory_usage
couchdb_result, couchdb_time, couchdb_cpu, couchdb_memory = couchdb_query_genre_popularity(db)
if couchdb_result:
    print("CouchDB Genre Popularity Analysis Performance:", couchdb_time, couchdb_cpu, couchdb_memory)
    for genre_info in couchdb_result:
        print(genre_info)
else:
    print("Query failed.")


# ## Query 6: Find all tracks where danceability is above a certain threshold and energy is below a certain threshold, and return the track_name, track_artist, and a new field performance_index which is a calculated value based on danceability and energy.

# In[187]:


import couchdb
import time
import psutil

def couchdb_query_dance_energy_analysis(db):
    start_cpu = psutil.cpu_percent(interval=1)
    start_memory = psutil.virtual_memory().used
    start_time = time.time()


    design_doc_id = "_design/dance_energy_analysis"
    view_name = "dance_energy_filter"

 
    map_function = """
    function(doc) {
        if(doc.danceability > 0.8 && doc.energy < 0.5) {
            emit(doc._id, {
                track_name: doc.track_name,
                track_artist: doc.track_artist,
                danceability: doc.danceability,
                energy: doc.energy
            });
        }
    }
    """

    design_doc = db.get(design_doc_id, {'_id': design_doc_id})
    design_doc['views'] = design_doc.get('views', {})
    design_doc['views'][view_name] = {'map': map_function}
    db.save(design_doc)


    result = db.view(f'{design_doc_id}/_view/{view_name}')


    track_info = []
    for row in result:
        track_data = row.value
        performance_index = track_data['danceability'] + track_data['energy']
        track_info.append({
            "track_name": track_data['track_name'],
            "track_artist": track_data['track_artist'],
            "performance_index": performance_index
        })

    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    time_taken = end_time - start_time
    memory_usage = end_memory - start_memory

    return track_info, time_taken, end_cpu, memory_usage
couchdb_result, couchdb_time, couchdb_cpu, couchdb_memory = couchdb_query_dance_energy_analysis(db)
if couchdb_result:
    print("CouchDB Dance and Energy Analysis Performance:", couchdb_time, couchdb_cpu, couchdb_memory)
    for track in couchdb_result:
        print(track)
else:
    print("Query failed.")


# In[190]:


def couchbase_query_performance_test(bucket_name):
    authenticator = PasswordAuthenticator('spotify', 'Ambadnyabapu@123')
    cluster = Cluster(
        'couchbases://cb.ph7oqthkgxprvsy.cloud.couchbase.com',
        ClusterOptions(authenticator)
    )
    start_cpu = psutil.cpu_percent(interval=1)
    start_memory = psutil.virtual_memory().used
    start_time = time.time()

    query = f"""
    SELECT track_name, track_artist,
           (danceability + energy) AS performance_index
    FROM `{bucket_name}`
    WHERE danceability > 0.8 AND energy < 0.5
    """
    result = cluster.query(query)
    rows = list(result)

    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    return rows, end_time - start_time, end_cpu, end_memory - start_memory


couchbase_result, couchbase_time, couchbase_cpu, couchbase_memory = couchbase_query_performance_test('spotify')
print("Couchbase Query Performance:", couchbase_time, couchbase_cpu, couchbase_memory)
print("Couchbase Results:", couchbase_result[:100])


# In[193]:


def mongodb_query_performance_test(db_name, collection_name):
    client = MongoClient('mongodb://localhost:27017')
    db = client[db_name]
    collection = db[collection_name]

      
    start_cpu = psutil.cpu_percent()
    start_memory = psutil.virtual_memory().used
    start_time = time.time()


    pipeline = [
        {"$match": {"danceability": {"$gt": 0.8}, "energy": {"$lt": 0.5}}},
        {"$project": {
            "track_name": 1,
            "track_artist": 1,
            "danceability": 1,
            "energy": 1,
            "performance_index": {"$add": ["$danceability", "$energy"]}
        }}
    ]
    result = list(collection.aggregate(pipeline))



    end_time = time.time()
 
    cpu_usage_during_query = psutil.cpu_percent(interval=1)
    end_memory = psutil.virtual_memory().used

    time_taken = end_time - start_time
    memory_usage = end_memory - start_memory

    return result, time_taken, cpu_usage_during_query, memory_usage


mongodb_result, mongodb_time, mongodb_cpu, mongodb_memory = mongodb_query_performance_test('project', 'spotify')
print("MongoDB Query Performance:", mongodb_time, mongodb_cpu, mongodb_memory)
print("MongoDB Results:", mongodb_result[:100])


# In[199]:


import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
crud_operations_mongodb = np.mean([0.0029, 0.00099, 0.0019, 0.0167])
crud_operations_couchbase = np.mean([0.84, 0.371, 0.174, 0.117])
crud_operations_couchdb = np.mean([0.024, 0.0189, 0.094, 0.097])
operations = [
    "Retrieve All Data Limit 10",
    "CRUD Operations",
    "Aggregation Function",
    "Top 5 Tracks",
    "Max Track Popularity",
    "Where Conditions"
]

execution_times = {
    'MongoDB': [0.01696, crud_operations_mongodb, 0.038, 0.02392, 0.0239, 0.03488],
    'Couchbase': [0.49541, crud_operations_couchbase, 2.289, 3.1961, 2.198587179, 2.169709],
    'CouchDB': [0.01342, crud_operations_couchdb, 1.68, 0.074, 10.9563911, 11.11]
}

cpu_usages = {
    'MongoDB': [2.3, 1.6, 1.6, 6.1, 3.9, 0.9],
    'Couchbase': [7.9, 4.3, 4.3, 3.7, 1.8, 0.4],
    'CouchDB': [55.7, 1, 1, 0.5, 2.2, 17.2]
}

memory_usages = {
    'MongoDB': [1757184, 1368064, 136804, 24276992, 11829248, 4943872],
    'Couchbase': [12783616, 21262336, 21262336, 34422784, 12247040, 11059200],
    'CouchDB': [2465792, 851968, 851968, 6352896, 17760256, 242466816]
}

max_cpu_usage = max(max(cpu_usages['MongoDB']), max(cpu_usages['Couchbase']), max(cpu_usages['CouchDB']))
max_memory_usage = max(max(memory_usages['MongoDB']), max(memory_usages['Couchbase']), max(memory_usages['CouchDB']))

cpu_usages_normalized = {
    'MongoDB': [x / max_cpu_usage for x in cpu_usages['MongoDB']],
    'Couchbase': [x / max_cpu_usage for x in cpu_usages['Couchbase']],
    'CouchDB': [x / max_cpu_usage for x in cpu_usages['CouchDB']]
}

memory_usages_normalized = {
    'MongoDB': [x / max_memory_usage for x in memory_usages['MongoDB']],
    'Couchbase': [x / max_memory_usage for x in memory_usages['Couchbase']],
    'CouchDB': [x / max_memory_usage for x in memory_usages['CouchDB']]
}

fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(10, 15))

for db in execution_times:
    axes[0].plot(operations, execution_times[db], marker='o', label=db)
axes[0].set_ylabel('Execution Time (s)')
axes[0].set_title('Execution Time Comparison')
axes[0].legend()

for db in cpu_usages_normalized:
    axes[1].plot(operations, cpu_usages_normalized[db], marker='o', label=db)
axes[1].set_ylabel('Normalized CPU Usage')
axes[1].set_title('CPU Usage Comparison')
axes[1].legend()

for db in memory_usages_normalized:
    axes[2].plot(operations, memory_usages_normalized[db], marker='o', label=db)
axes[2].set_ylabel('Normalized Memory Usage')
axes[2].set_title('Memory Usage Comparison')
axes[2].legend()

plt.tight_layout()
plt.show()


# In[200]:


crud_execution_times = {
    'MongoDB': crud_operations_mongodb,
    'Couchbase': crud_operations_couchbase,
    'CouchDB': crud_operations_couchdb
}


crud_cpu_usages = {
    'MongoDB': cpu_usages['MongoDB'][1],
    'Couchbase': cpu_usages['Couchbase'][1],
    'CouchDB': cpu_usages['CouchDB'][1]
}

crud_memory_usages = {
    'MongoDB': memory_usages['MongoDB'][1],
    'Couchbase': memory_usages['Couchbase'][1],
    'CouchDB': memory_usages['CouchDB'][1]
}


crud_cpu_usages_normalized = {db: usage / max_cpu_usage for db, usage in crud_cpu_usages.items()}
crud_memory_usages_normalized = {db: usage / max_memory_usage for db, usage in crud_memory_usages.items()}


fig, ax = plt.subplots(figsize=(10, 6))


ax.bar(crud_execution_times.keys(), crud_execution_times.values(), color=['blue', 'orange', 'green'])
ax.set_ylabel('Execution Time (s)')
ax.set_title('CRUD Operations Execution Time')


plt.show()


# In[ ]:




