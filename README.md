README 
==========
1. This project is based on Hadoop and Hive.  
   If you don't have set them up, please reference the following instructions: 
   [Hadoop](http://hadoop.apache.org/docs/r2.5.2/index.html) &
   [Hive](https://cwiki.apache.org/confluence/display/Hive/GettingStarted).

2. You can download our testing data from [here](http://grouplens.org/datasets/movielens/).  
   The `README.txt`  has very detailed explaination about the property of their data. 
   We also has descriptions in our wirteup.
   Please replace the `::` in the given data set by `   ` (which is a tab). Just make it easier to process.

3. Please change the directory to the data file you just download.   
   For me, it is  `$cd ~/ml-1m`  
   Please start the hadoop, which is a prerequest for Hive running.  
   Type the command `$hive -f extract.q`.  
   A directory "result" would appear. It stores the data we want to use.
   We have already provided such extracted data, called `new_data.txt` in the source file.

4. Create the folder on the HDFS, we will put the data into the folder:  
   `$ hadoop fs -makedir /hadoop `  
   Put the data on the HDFS:  
  ` $ hadoop -fs copyFromLocal /directory of the data/ /hadoop`  
   For me, it’s `$hadoop -fs copyFromLocal ~/new_data.txt /hadoop`  
   Run the jar code:  
   `$ hadoop jar ./Bayes.jar hw6.MultiMovieRecommender /hadoop/ /hadoop/temp /hadoop/output/`  
   Check the result of training data  
   `$hadoop fs -cat /hadoop/output/part*`

5. Make sure the output of training data is in the directory of `/movie` in the HDFS.  
   Make sure that `BayesHiveUDF.jar` is in your current directory.  
   Run the command `$ hive -f constructtrain.q`  
   Run the command `$ hive -f classification.q`  
   When we want to change the parameters, we can just simply change the `line 11` of classification.q  
   In the directory `result/finalresult`, the reuslt of recommendation is generated.  
   Our sample text result is in the source directory: `test_result.txt`

Thanks for your using.
If you have any questions, feel free to contact us.   
Email: cwang107@jhu.edu