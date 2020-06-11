# DataEngineerChallenge

This is an interview challenge for PayPay. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

    Implemented in `com.seansun.sessionization.core.Sessionizer`.
    After some survey on the internet, I implemented it with Spark SQL which could easier be implemented rather than 
    RDD style and usually get the better performance. 
    
2. Determine the average session time

   The average session length time in second is: [100.75318265890739]

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

   The result of unique URL per session would be wrote down into output path.

4. Find the most engaged users, ie the IPs with the longest session times

   The following shows the top 10 most engaged user
   
   userId|totalSessionTime|totalSessionCount|totalRequestCount|totalUniqueRequestCount
   ---|---|---|---|---
   220.226.206.7|6795|13|1536|880
   119.81.61.166|6153|9|32829|22937
   52.74.219.71|5258|10|40633|26217
   54.251.151.39|5236|10|4003|7
   121.58.175.128|4988|10|1044|625
   106.186.23.95|4931|10|14565|13737
   125.19.44.66|4653|10|1460|547
   54.169.191.85|4621|8|1318|0
   207.46.13.22|4535|10|82|82
   180.179.213.94|4512|9|417|407


## Additional questions for Machine Learning Engineer (MLE) candidates:
1. Predict the expected load (requests/second) in the next minute

    The prediction pipeline implemented in `com.seansun.ml.loading.PredictPipeline`.
    The next minute: `2015-07-23 05:11:00.0` requests count prediction is `10195`
    In this model, I extract the features from timestamp, the features are like "week of year", "hour', "minute" etc.
    Use basic Linear Regression model, Cross Validation, and a grid of parameters.

2. Predict the session length for a given IP

    Did not implement yet, but to predict the session length by only given IP 
    which in my opinion is lacking information. The IP can only create some features like IP mask, 
    for instance 192.168.0.1 -> 192.168.x.x, which may represent the area of IP user.
    However, it should not enough to do a proper prediction.
    Maybe we could leverage more information like timestamp and user-agent.

3. Predict the number of unique URL visits by a given IP

    Did not implement yet with the same opinion of above.

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

    For this item, we could actually leverage the user-agent, concat IP and user-agent.
    People may share the same IP, but in one session, people usually not to switch different browsers or devices.
    We can assume that same IP but different user-agent is different user.
    
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


## Solution

The solution implemented by Apache Spark with Scala code.

### How to build

```shell script
sbt assembly
```

### How to run

* Sessionization

    ```shell script
    spark-submit \
      --class com.seansun.sessionization.batch.Processor \
      --master 'local[*]' \
      --conf 'spark.sql.shuffle.partitions=4' \
      /path/to/DataEngineerChallenge-assembly-0.1.jar \
      [--srcPath /path/to/2015_07_22_mktplace_shop_web_log_sample.log.gz \]
      [--outputPath /path/to/output \]
      [--maxSessionDuration 15 \]
      [--userIdField ip]
    ```

* Loading Prediction

    ```shell script
    spark-submit \
      --class  com.seansun.ml.loading.PredictPipeline \
      --master "local[2]" \
      --conf "spark.sql.shuffle.partitions=4" \
      /Users/tseensun/Projects/PayPay/DataEngineerChallenge/target/scala-2.11/DataEngineerChallenge-assembly-0.1.jar \
      [--srcPath /path/to/2015_07_22_mktplace_shop_web_log_sample.log.gz \]
      [--outputPath /path/to/output]
    ```


### Result

The average session length time in second is: [100.75318265890739]

The following shows the top 10 most engaged user

userId|totalSessionTime|totalSessionCount|totalRequestCount|totalUniqueRequestCount
---|---|---|---|---
220.226.206.7|6795|13|1536|880
119.81.61.166|6153|9|32829|22937
52.74.219.71|5258|10|40633|26217
54.251.151.39|5236|10|4003|7
121.58.175.128|4988|10|1044|625
106.186.23.95|4931|10|14565|13737
125.19.44.66|4653|10|1460|547
54.169.191.85|4621|8|1318|0
207.46.13.22|4535|10|82|82
180.179.213.94|4512|9|417|407

