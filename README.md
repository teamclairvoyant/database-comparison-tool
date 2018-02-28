                                        ****DataBase Comparision Tool****

**DESCRIPTION:**

1. This tool is used to compare tables in any combination in Sql Databases(mssql,mysql and postgresql) and Hive by running a Spark Job.
2. Details of both source and destination are provided through Arguments.
3. This tool provides an option to exclude columns from both the tables to compare.
4. There is option to provide where clause to query data under particular partition.

**Usage:**

1. Update the application.properties file in /app/db_comparision/conf  with the required SqlServer credentials to connect.
2. Now navigate to /app/db_comparision directory and run the spark job to find the differences between two tables.
    Example spark-submit command:
        
        spark-submit --driver-class-path /app/db_comparision/conf /app/db_comparision/lib/sqlserver_hive_compare-jar-with-dependencies.jar -sourceName mysqltruckstop -destinationName Hive -sourceType mysql -destinationType Hive -sourceDatabase {source_database_name} -sourceTable {source_table_name} -destinationDatabase {destination_database_name} -destinationTable {destination_table_name}
        
        If there are any columns to exclude then add the argument 
            -excludeColumns {comman_seperated_columns_to_exclude}
        
        If you want provide a where clause then add the argument
            -where {where_clause} 

3. If the Tables are equal then we will get a prompt saying Tables are Equal after the Spark Job.
4. If they are not equal then the differences will be shown in a html file. A link will be displayed on the screen to view the file.

**Note: Html File should be hosted to view it.**

**Note: Port Should be forwarded to get the following steps to work.** 

**Port Forwarding:**

1. Forward 9090 port to get this server run.
2. For Telarix this should be the way to connect to jump host with port forwarding 
        
        ssh -L 40022:10.0.224.64:22 -L 40080:10.0.224.64:80 -L 47180:34.197.46.99:7180 -L 48888:34.197.46.99:8888 -L 48088:34.197.46.99:8088 -L 49888:34.197.46.99:19888 -L 48080:34.197.46.99:8080 -L 45672:34.197.46.99:15672 -L 47187:34.197.46.99:7187 -L 58080:10.0.224.100:8080 -L 50070:34.197.46.99:50070 -L 49090:34.197.46.99:9090 robert.sanders@10.6.5.22
    
**Hosting the Html File:**

1. Navigate to the html output directory as provided in the application.properties file.

2. Now run the command 
       
       python -m SimpleHTTPServer 9090

3. Files in the html directory will be hosted.   

RUNNING THE TOOL USING SHELL SCRIPT

Script that helps to run this tool is placed in the bin folder.

Run the script by providing the arguments that are required to run the spark-submit for the tool.

Required Arguments

    --sourceName {name_of_the_host_where_source_database_is_hosted}. (If the Source is Hive then give the name as "Hive")
    --destinationName {name_of_the_host_where_destination_database_is_hosted}. (If the Source is Hive then give the name as "Hive") 
    
    --sourceType {Type_of_database} (i.e mysql or mssql or postgresql or Hive) 
    --destinationType Hive {Type_of_database} (i.e mysql or mssql or postgresql or Hive)
    
    --sourceDatabase {source_database_name} 
    --sourceTable {source_table_name} 
    
    --destinationDatabase {destination_database_name} 
    --destinationTable {destination_table_name}
 

Optional Arguments

    --excludeColumns   Columns to exclude
    --where            where clause


Example:

    sh tes.sh -sourceName {SOURCE_NAME} -destinationName {DESTINATION_NAME} -sourceType {SOURCE_TYPE} -destinationType {DESTINATION_TYPE} -sourceDatabase {SOURCE_DATABASE} -sourceTable {SOURCE_TABLE} -destinationDatabase {DESTINATION_DATABASE} -destinationTable {DESTINATION_TABLE} --excludeColumns {columns_to_exclude} --where {where_clause}

Setup Steps:
    
    1. This tool can be quickly used by downloading the "database_comparision_tool_build" direcotry from the repo into the desired location.
    2. Under this direcotry there is a sub-directory called ""conf. "conf" directory consists of the application.properties file. Fill the application.properties file with your desired database details.
    3. Create "lib" directory as another sub-directory. Build the jar by pulling latest code from the repo. Jar is generated in target directory. Copy the jar file to the "lib" directory.
    4. Once this "database_comparision_tool_build" is downloaded and the application.properties is updated and the jar file is generated and copied from target directory to "lib" the tool is ready to use.

TODO:

1. Validate using primary key.
2. Support Hash compare.(Hashing the entire row)    
    
