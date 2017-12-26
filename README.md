                                        ****DataBase Comparision Tool****

**DESCRIPTION:**

1. This tool is used to compare tables in SqlServer and Hive by running a Spark Job.
2. Details of both the tables in SqlServer and Hive are provided through Arguments.
3. This tool provides an option to exclude columns from both the tables to compare.
4. There is option to provide where clause to query data under particular partition.

**Usage:**

1. Update the application.properties file in /app/db_comparision/conf  with the required SqlServer credentials to connect.
2. Now navigate to /app/db_comparision directory and run the spark job to find the differences between two tables.
    Example spark-submit command:
        spark-submit --driver-class-path /app/db_comparision/conf /app/db_comparision/lib/sqlserver_hive_compare-jar-with-dependencies.jar -sqlDatabase {Sql_database_name} -sqlTable {sql_table_name} -hiveDatabase {hive_database_name} -hiveTable {hive_table_name}
        
        If there are any columns to exclude then add the argument 
            -excludeColumns {comman_seperated_columns_to_exclude}
        
        If you want provide a where clause then add the argument
            -where {where_clause} 

3. If the Tables are equal then we will get a prompt saying Tables are Equal after the Spark Job.
4. If they are not equal then the differences will be shown in a html file.A link will be displayed on the screen to view the file.

**Note: Html File should be hosted to view it.**

**Note: Port Should be forwarded to get the following steps to work.** 

**Port Forwarding:**

1. Forward 9090 port to get this server run.
2. For Telarix this should be the way to connect to jumphost with port forwarding 
        
        ssh -L 40022:10.0.224.64:22 -L 40080:10.0.224.64:80 -L 47180:34.197.46.99:7180 -L 48888:34.197.46.99:8888 -L 48088:34.197.46.99:8088 -L 49888:34.197.46.99:19888 -L 48080:34.197.46.99:8080 -L 45672:34.197.46.99:15672 -L 47187:34.197.46.99:7187 -L 58080:10.0.224.100:8080 -L 50070:34.197.46.99:50070 -L 49090:34.197.46.99:9090 robert.sanders@10.6.5.22
    
**Hosting the Html File:**

1. Navigate to the html output directory as provided in the application.properties file.
2. Now run the command 
    python -m SimpleHTTPServer 9090
3. Files in the html directory will be hosted.      


**RUNNING THE TOOL USING SHELL SCRIPT**

1. Script that helps to run this tool is placed in the bin folder.
2. Run the script by providing the arguments that are required to run the spark-submit for the tool.
    
    **Arguments**
        
        -s   SqlServer Database Name
        -t   SqlServer Table Name
        -h   Hive Database Name
        -p   Hive Table Name
        -e   Columns to exclude
        -w   where clause