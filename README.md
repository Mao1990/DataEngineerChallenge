# DataEngineerChallenge

This is an interview challenge for PayPay. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times

## Additional questions for Machine Learning Engineer (MLE) candidates:
1. Predict the expected load (requests/second) in the next minute

2. Predict the session length for a given IP

3. Predict the number of unique URL visits by a given IP

## Tools allowed (in no particular order):
- Spark (any language, but prefer Scala or Java)
- Pig
- MapReduce (Hadoop 2.x only)
- Flink
- Cascading, Cascalog, or Scalding

If you need Hadoop, we suggest 
HDP Sandbox:
http://hortonworks.com/hdp/downloads/
or 
CDH QuickStart VM:
http://www.cloudera.com/content/cloudera/en/downloads.html


### Additional notes:
- You are allowed to use whatever libraries/parsers/solutions you can find provided you can explain the functions you are implementing in detail.
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
- For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.
- The log file was taken from an AWS Elastic Load Balancer:
http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format



## How to complete this challenge:

1. Fork this repo in github
2. Complete the processing and analytics as defined first to the best of your ability with the time provided.
3. Place notes in your code to help with clarity where appropriate. Make it readable enough to present to the PayPay interview team.
4. Include the test code and data in your solution. 
5. Complete your work in your own github repo and send the results to us and/or present them during your interview.

## What are we looking for? What does this prove?

We want to see how you handle:
- New technologies and frameworks
- Messy (ie real) data
- Understanding data transformation
This is not a pass or fail test, we want to hear about your challenges and your successes with this particular problem.


## Some script note
```$xslt
.bin/spark-submit \
  --class com.seansun.sessionization.batch.Processor \
  --master 'local[*]' \
  --conf 'spark.sql.shuffle.partitions=12' \
  /path/to/DataEngineerChallenge-assembly-0.1.jar \
  [--srcPath /path/to/2015_07_22_mktplace_shop_web_log_sample.log.gz \]
  [--outputPath /path/to/output \]
  [--maxSessionDuration 15 \]
  [--userIdField ip]
```

### Result

The average session length time in second is: [6.355726760476373]

The following shows the top 10 most engaged user

userId|totalSessionTime|totalSessionCount|totalRequestCount|totalUniqueRequestCount
---|---|---|---|---
52.74.219.71|3908|14|40633|26735
119.81.61.166|3898|14|32829|31083
54.251.151.39|3882|14|4003|8
106.186.23.95|3304|21|14565|13748
54.169.191.85|3253|12|1318|0
220.226.206.7|1995|65|1536|892
188.40.135.194|1883|7|644|644
203.191.34.178|1770|18|486|210
125.19.44.66|1699|40|1460|772
213.239.204.204|1603|6|486|486