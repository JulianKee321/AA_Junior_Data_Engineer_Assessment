Assessment2.py

USE CASE:
  Case 1.0 - Get total count of user group by gender and email_provider and save as CSV file
  Case 2.0 - Produce list of data like example above and save it as parquet file partition by gender and state
           - current_time column are base on MYT time
           - parquet file are base on single thread

For this practice, the code was executed on a local PC running Windows 10 using a pyspark shell. 
The spark version used was 3.1.1 and the Python3 version used was 3.8.1.

The code used to generate Output_I and Output_II 
