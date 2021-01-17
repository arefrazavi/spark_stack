# [Sparks Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html):     
It is used to analyze (real time) continual streams of data (e.g. log data).  
The idea is to aggregate and analyze data as it's being streamed in at some given interval.   
It can take (listen to) input data from a port, Amazon Kinesis, HDFS, Kafka, Flume, ....  
It can output the (aggregation) result to a database, another stream, port, ....  

## Checkpointing:      
Sparks Streaming has a check pointing feature to increase fault tolerance.  
It continually stores its current state to disk periodically. So that way if your stream goes down, it can pick up where it left off automatically the next time you get it running again.  

## Sparks Streaming Approaches:
### 1) DStream (old way)
The idea of a DStream is that your stream is broken up into distinct RDD's.
It's not a real time stream of information with a DStream.  
We're getting what we call micro batches, just little snapshots of each chunk of data that's coming in and you deal with each chunk.     

- Window Operations:    
Window operations apply transformations over a sliding window of data.  
**RDDs in DStream Dstream only contains that one chunk of incoming data for the batch interval period.**   
Window operations allow us to combine results from multiple batches over some sliding time window.
  

### 2) [Structured Streaming (new way!)](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
Instead of using Dstreams and these distinct RDD's of data, Structured Streaming models our stream 
as a data frame that just keeps on growing over time infinitely.
In order words, we just have a data stream feeding into the unbounded table (data frame) and 
as new data arrives it just results in new rows being appended to that input table. 
As opposed to Dstreams, structured streaming is real-time streaming.    

- Window Operations:
    Aggregations over a sliding event-time window.  In case of window-based aggregations, 
    aggregate values are maintained for each window the event-time of a row falls into.
    + windowDuraion:      
        The duration to consider a row in aggregation.   
        E.g. in streaming latest requests in the past 10 seconds, 10s is our window duration.   
    + slideDuration (Slide interval):    
        How often the query should run and re-evaluate the windowed aggregation(s). 
        E.g Every 5 minutes, stream the latest requests in the past 10 seconds.
  


## Source Code
https://github.com/apache/spark/tree/v3.0.1/examples/src/main/python/streaming