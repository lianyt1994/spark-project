# spark-project
使用spark SQL和spark streaming分别进行离线和实时日志分析
系统架构：
离线处理：
    先使用flume将数据从日志文件中传输到hdfs，
    然后通过spark SQL从hdfs中调取数据，经过数据清洗存入hdfs，
    在进行数据分析操作时再从hdfs中取出，后存入mysql，
    然后可以通过echarts进行数据可视化展示
实时处理：
    先使用flume将数据从日志文件中传输到kafka，
    kafka再将数据传给spark streaming(spark-streaming-kafka依赖包)，
    经过数据分析和处理后存入hbase(hbase可以通过table.incrementColumnValue方法一步操作完成取出数据，加上值，再存入)
