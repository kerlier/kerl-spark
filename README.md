# spark
Spark提交任务的时候，需要知道当前的master的ip地址和端口号
    使用蒙特卡罗算法计算圆周率
        bin/spark-submit  
         --class org.apache.spark.examples.SparkPi  
         --master spark://hadoop-node1:7077 
         --executor-memory 1G  
         --total-executor-cores 2 
         examples/jars/spark-examples_2.11-2.4.0.jar 
         10
	
1.Spark-shell的使用
    单机使用spark-shell
      spark-shell --master local[2]    
      --master local[2]  表示当前采用两个线程计算任务
    
    
    1.1
      读取本地文件
      sc.textFile("file:///root///words.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect
	解释： textFile表示的是读取文件，file开头的是读取的本地文件 如果读取hdfs上的文件，直接写路径就行
	       faltMap读取文件中的内容
		   _表示的文件中每一行的数据，split将这一行用空格切分
		   map((_,1)) _表示的是切分后的每一行数据，使用元组将切分好的数据进行填充
		   reduceByKey 将单词安装元组中的key合并，_+_ 两个_表示相同的单词，意思就是将相同key的单词的value相加
		   collect将结果进行收集。
