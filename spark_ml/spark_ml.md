# SparkML
It's a series of machine learning algorithms and if you're gonna be doing any sort of machine learning or data mining with Spark, it contains a lot of useful tools.    
For example if you need to do Pearson correlation or get statistical properties of your data.     

## Supervised Learning (Classification/Regression)

### Recommendation System:
- [Collaborative filtering](https://spark.apache.org/docs/3.0.1/ml-collaborative-filtering.html):  
  Find similar movies to recommend new movies based on their similarity.         
  Collaborative filtering is based on the assumption that people who agreed in the past will agree in the future,   
  and that they will like similar kinds of items as they liked in the past. 


### Linear Regression (Classification Algorithm)
- It is fitting a line to a dataset of observations and using that line to make predictions based on the previous points that you train that line to.
  
#### 1) Least-Squares Linear Regression:     

+ Minimizing the squared error between each point in the line to have more accurate line.
+ Line is represented by a slope-intercept equation: `y = mx + b`
+ Slope (m) is the correlation between the two variables, times the standard deviation in y divided by the standard deviation in x.
+ The y-intercept (b) is the mean of y minus (the slope times the mean of x).
    
- Stochastic Gradient Descent (SGD)
    + Useful when that you have more than one feature to predict that label.    
      Spark Streaming uses SGD to implement linear regression.
    + ```linearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)```     
      Train model and make predictions using **tuples that consist of a label, and a vector of features.**      
      The label is Y axis which is the value that we're trying to predict.      
      The X axis or axes are the features.
    + Doesn't handle "Feature Scaling" very well.       
      SGD assumes that dataset is a similar to a normal distribution.   
      If the scale of features (e.g. age and weight) is different, scale your data down, to fit something that's closer to a normal distribution centered around zero, then scale it back up again when you're done.    
  
#### 2) Decision Tree Regression
It is a tree-structured classifier with three types of nodes. The Root Node is the initial node which represents the entire sample and may get split further into further nodes. The Interior Nodes represent the features of a data set, and the branches represent the decision rules. Finally, the Leaf Nodes represent the outcome. 
With a particular data point, it is run completely through the entirely tree by answering True/False questions till it reaches the leaf node. The final prediction is the average of the value of the dependent variable in that particular leaf node.  
Through multiple iterations, the Tree is able to predict a proper value for the data point.     
+ It handles "Feature Scaling" very well.       
+ Useful when that you have more than one feature to predict that label.    
- The problem with it is over-fitting that can be resolved with Random Forest.

XG boost is the novel variation that work with multiple gradient boosted decision trees.