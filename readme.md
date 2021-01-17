# Spark
A general engine for large-scale data processing. 


## Spark Main Components:
- Driver program (Spark context):   
  It runs the Spark script on top of different cluster managers.    

- Cluster Manager:      
Cluster manager create multiple executors per machine, distribute data on executors and coordinate among various executors.
Spark has a built-in manager. But If you have access to a Hadoop cluster, there's a component of Hadoop called Yarn that Spark can also run on top of to distribute work amongst a huge Hadoop cluster.   
  
- Executor:     
Executor runs Spark script on a shard of data. It gives you horizontal scalability.

  
## Spark Advantages:
+ Sparks is faster than Hadoop MapReduce.
+ Run in memory.
+ Using DAG (Directed Acyclic Graph) engine. It optimizes workflow.
+ It doesn't actually do anything until you actually ask it to to deliver results  (lazy evaluation).   
  At that point it creates this graph of all the different steps that it needs to put together to actually achieve the results you want.    
  And it does that in an optimal manner, so it can actually wait until it knows what you're asking for and then figure out the optimal path to answering the question that you want.
  

## External Libraries:
- Spark SQL
- Sparks Streaming
- ML
- GraphX:   
  GraphX can help with problems which we want to measure the properties of graphs (e.g. social network).    
  It can do things like measure things like connectedness or degree distribution and average path length and triangle counts.   
  - RDD formats: VertexRDD, EdgeRDD
  - It doesn't have any API for Python. :( 

## Run Spark on Cloud Cluster (Elastic Map Reduce (EMR))
EMR is not just for map produce anymore. EMR is a managed Hadoop framework.
You can use the yarn component of Hadoop as a cluster manager that spark runs on top of.

By just kicking off an EMR cluster an Amazon Web Services,
you can very quickly get a master node and as many clients as slave nodes as you want for the job that you have at hand.    

+ Spark runs on m3.xlarge instances.
+ EC2 is the elastic compute cloud which is the underlying service that EMR uses 
  to actually spin up (create) the different computers on the cluster.

### Steps of running Spark scripts on EMR on:
1) Create an S3 bucket and copy script and dataset to it.
   To load data directly from Amazon's S3 service. 
   EMR cluster has very fast and very good connectivity to S3.

2) Create key-pair (public and private keys) (PEM) in EC2 service

3) Create an EMR cluster on AWS console     
 https://ca-central-1.console.aws.amazon.co     
Config:
- Applications in Software configuration: Spark (Spark 2.4.7 on Hadoop 2.10.1 YARN and Zeppelin 0.8.2).
- Hardware configuration: Instance type: m4.xlarge, Number of instances: 5
- Security: Choose generated key-pair.

Copy the **Master Public DNS** (e.g. my-instance-public-dns-name) after the cluster has been created.  

4) Connect to EMR  (EC2 instance)   
https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html
(Public DNS) To connect using your instance's public DNS name, enter the following command.
```
ssh -i /path/my-key-pair.pem hadoop@my-instance-public-dns-name  
```

5) Copy scripts and datasets from s3 bucket to master node of EMR 

```
aws s3 cp s3://sundog-spark/MovieSimilarities1M.py ./
aws s3 sp c3://sundog-spark/ml-1m/movies.dat ./
```

6) Run script on EMR (master node)
```
spark-submit --executor-memory 1g MovieSimilarities1M.py 260
```
Set 1 GB of memory per an executor. (default can be lower).

7) Terminate Cluster
Choose the cluster in cluster list of EMR services on AWS console and click terminate.      


## Troubleshooting Spark
Spark offers a console UI that runs by default on Port 8080 and 
it gives you a graphical in-depth details of Spark jobs.

1) Run spark-master.sh:
   (e.g. it's in /usr/local/spark/sbin/start-master.sh)
   Starts a master instance on the machine the script is executed on.
    You can change the port by setting SPARK_MASTER_WEBUI_PORT  env variable.
    export SPARK_MASTER_WEBUI_PORT=8686
2) Go to localhost:8080
   
in EMR, it's hard to connect to the console.

- Logs:     
    + In a standalone mode where you have access,
    it's accessible through the Spark web UI.
      
    + In Yarn, logs are distributed. You need to collect them using the following command:  
        ```yarn logs --applicationID  <appID>```    

- How to fix heartbeat issues in Spark: 
  It means you ask too much of individual executors.      
    + Increase executors' memory    
    + Increase number of machines per cluster   
    + Use partitionBy() to demand less work from each individual executors.  
    