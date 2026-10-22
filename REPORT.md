# Hands-on L9: Report

**Name: Lauren Sharpe**
**Student ID: 801371099**
**Email: lsharp22@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`:

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

```bash
    python3 datagen.py 801371099
    docker compose up -d
    docker cp main.py spark-master:/opt/spark/work-dir/
    docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py /opt/spark/work-dir/shared/input /opt/spark/work-dir/shared/output
    docker cp main.py spark-master:/opt/spark/work-dir/
    docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py /opt/spark/work-dir/shared/input /opt/spark/work-dir/shared/output
     docker cp main.py spark-master:/opt/spark/work-dir/
   docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py /opt/spark/work-dir/shared/input /opt/spark/work-dir/shared/output
     docker cp main.py spark-master:/opt/spark/work-dir/
   docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py /opt/spark/work-dir/shared/input /opt/spark/work-dir/shared/output
```

---

## Results

For each task, your output and one or two sentences on what it says about your data.

### Task 1: hashtag trends (DataFrame API)

```
+--------+-----+
|hashtag |count|
+--------+-----+
|#mood   |1597 |
|#love   |1529 |
|#design |1490 |
|#fail   |1016 |
|#update |1003 |
|#ai     |612  |
|#tech   |611  |
|#ux     |605  |
|#social |437  |
|#cleanui|435  |
+--------+-----+
```
The most used hashtag was #mood, which appeared 1597 times, then #love and #design. So it seems like this data set had popular topics of design related content as well as peronal mood and expression. 
How many distinct spellings did your most frequent hashtag have before `lower` and `trim`?

### Task 2: engagement by age group (Spark SQL)

```
+---------+-----+---------+------------+
|age_group|posts|avg_likes|avg_retweets|
+---------+-----+---------+------------+
|Adult    |1857 |54.9     |14.19       |
|Teen     |1216 |47.71    |11.71       |
|Senior   |1927 |47.21    |12.09       |
+---------+-----+---------+------------+
This shows that adults have the highest rates of engagment and similar rates for teens and seniors. Seniors did however have the most posts so while they post more they engage less.

```

### Task 3: sentiment versus engagement (Spark SQL)

```
+---------+-----+---------+------------+
|sentiment|posts|avg_likes|avg_retweets|
+---------+-----+---------+------------+
|Negative |1060 |23.73    |5.9         |
|Neutral  |2256 |45.17    |11.79       |
|Positive |1684 |73.56    |18.44       |
+---------+-----+---------+------------+

```

Does sentiment pay on your data? Say it with the numbers.
Yes it does pay. Positive posts recieved an average of 73.56 likes and 18.44 retweets. They received an average of 210% more likes and 212.5% more retweets than negative posts. This suggests that post with positive sentiment will have higher engagment. 

### Task 4: top verified users (DataFrame API)

```
+-------------+-----------+--------------+-----+
|username     |total_likes|total_retweets|reach|
+-------------+-----------+--------------+-----+
|@critic21    |17692      |4696          |22388|
|@techie12    |15981      |4441          |20422|
|@night_coder6|8330       |2223          |10553|
|@techie10    |8344       |2159          |10503|
|@stream_bot19|7899       |1738          |9637 |
+-------------+-----------+--------------+-----+
```
This shows the user with the most engagment was critic21 with 22388 combined likes and retweets. Reach was calculated by adding both the likes and retweets so it doesn't mean 22388 people intereacted but they got at least that many interactions. 
---

## Two languages, one plan

Paste the two `explain()` outputs of task 2, the SQL version and the DataFrame version:

```
=== task2: physical plan of the SQL version ===
26/10/21 16:03:12 INFO FileSourceStrategy: Pushed Filters: IsNotNull(user_id)
26/10/21 16:03:12 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(user_id#1))
26/10/21 16:03:12 INFO FileSourceStrategy: Pushed Filters: IsNotNull(user_id)
26/10/21 16:03:12 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(user_id#8))
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#53 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#53 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=697]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=694]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=689]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>



=== task2: physical plan of the DataFrame version ===
26/10/21 16:03:12 INFO FileSourceStrategy: Pushed Filters: IsNotNull(user_id)
26/10/21 16:03:12 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(user_id#1))
26/10/21 16:03:12 INFO FileSourceStrategy: Pushed Filters: IsNotNull(user_id)
26/10/21 16:03:12 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(user_id#8))
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#133 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#133 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=747]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=744]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=739]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

Your comparison: are the two plans the same? Which operator is the join and which kind of
join did Spark choose, where are the `Exchange` operators and why are they needed, and what
does this tell you about choosing between SQL and the DataFrame API?

These two plans are functionally the same but they do differ in the enternal expressions. They both use a BroadcastHashJoin to join posts and users so Spark could broadcast the smaller users dataset so it could perform the join without shuffling both datasets across the cluster. The plans contain two regular exchange operations, the first uses hash partioning to redistribute records on age_group for aggregation, and the second uses range partioning to prepare the avg_likes results for sorting. Since they both performed basically the same actions it seems you can use either based on your own familiarity and how easily the query fits into one or the other. 


---

## Views

What did `createOrReplaceTempView("posts")` do, and what did it not do? Was any data copied?
What happens to the views when the program ends, and how would you keep a table for the
next session?
It created a temporary view named posts so the program could query the DataFrame. It did not copy the data, it only made a logical view for the session. When the session ends the temporary view is removed and can't be accessed again.  To keep it you could write the DataFrame to a Parquet file and reload it later. 



---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

Traceback (most recent call last):
  File "/opt/spark/work-dir/main.py", line 161, in <module>
    save(task4_top_verified_users(), "task4")
  File "/opt/spark/work-dir/main.py", line 141, in task4_top_verified_users
    verifiedUsers = users.filer(col("verified") == True)
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/classic/dataframe.py", line 1083, in __getattr__
pyspark.errors.exceptions.base.PySparkAttributeError: [ATTRIBUTE_NOT_SUPPORTED] Attribute `filer` is not supported.

mispelled filter, to fix I fixed the spelling.

