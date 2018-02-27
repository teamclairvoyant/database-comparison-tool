package com.clairvoyant.sqlserver_hive_compare;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class SqlServerHiveCompare {

    public static void main(String[] args) throws Exception {

        String sourceSqlUrl;
        String sourceSqlUsername;
        String sourceSqlPassword;
        String sourceSqlDriver = null;
        String sourceSqlPort = null;

        String destinationSqlUrl;
        String destinationSqlUsername;
        String destinationSqlPassword;
        String destinationSqlDriver = null;
        String destinationSqlPort = null;

        String htmlStorageLocation;

        CommandLineArguments arguments = new CommandLineArguments(args);

        String sourceName = CommandLineArguments.getsourceName();
        String destinationName = CommandLineArguments.getdestinationName();

        String sourceType = CommandLineArguments.getsourceType();
        String destinationType = CommandLineArguments.getdestinationType();

        String sourceDatabase = arguments.getsourceDatabase();
        String sourceTable = arguments.getsourceTable();
        String destinationDatabase = arguments.getdestinationDatabase();
        String destinationTable = arguments.getdestinationTable();

        String whereClause = arguments.getwhereClause();
        String excludeColumnsString = arguments.getexcludeColumns();
        List<String> excludeColumns = new ArrayList<>();
        if(excludeColumnsString != null){
            excludeColumns = Arrays.asList(excludeColumnsString.split(","));
        }

        // Getting Timestamp
        SimpleDateFormat formatter = new SimpleDateFormat("ddMMyyyyHHmmss");
        Date date = new Date();
        String timestamp = formatter.format(date);

        //getting beans from spring context
        AbstractApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);

        //getting JavaSpark context
        JavaSparkContext javaSparkContext = (JavaSparkContext) context.getBean("sc");

        //getting HIveContext
        HiveContext hiveContext = (HiveContext) context.getBean("hiveContext");

        //getting SparkConf
        SparkConf sparkConf = (SparkConf) context.getBean("sparkConf");

        // Masking the info Logs
        javaSparkContext.setLogLevel("WARN");

        sparkConf.setAppName("SqlServerHiveCompare");

        //load SqlServer properties

        ArrayList serverNames;
        serverNames = (ArrayList) context.getBean("serverNames");


        sourceSqlUrl = (String) context.getBean("sourceSqlServerUrl");
        sourceSqlUsername = (String) context.getBean("sourceSqlServerUsername");
        sourceSqlPassword = (String) context.getBean("sourceSqlServerPassword");
        if (Objects.equals(sourceType, "mssql")){
            sourceSqlDriver = (String) context.getBean("sqlServerDriver");
            sourceSqlPort = (String) context.getBean("sqlServerPort");
        }else if(Objects.equals(sourceType, "mysql")) {
            sourceSqlDriver = (String) context.getBean("mySqlDriver");
            sourceSqlPort = (String) context.getBean("mysqlPort");
        }else if(Objects.equals(sourceType, "postgresql")) {
            sourceSqlDriver = (String) context.getBean("postgresqlDriver");
            sourceSqlPort = (String) context.getBean("postgresqlPort");
        }

        destinationSqlUrl = (String) context.getBean("destinationSqlServerUrl");
        destinationSqlUsername = (String) context.getBean("destinationSqlServerUsername");
        destinationSqlPassword = (String) context.getBean("destinationSqlServerPassword");
        if (Objects.equals(destinationType, "mssql")){
            destinationSqlDriver = (String) context.getBean("sqlServerDriver");
            destinationSqlPort = (String) context.getBean("sqlServerPort");
        }else if(Objects.equals(destinationType, "mysql")) {
            destinationSqlDriver = (String) context.getBean("mySqlDriver");
            destinationSqlPort = (String) context.getBean("mysqlPort");
        }else if(Objects.equals(destinationType, "postgresql")) {
            destinationSqlDriver = (String) context.getBean("postgresqlDriver");
            destinationSqlPort = (String) context.getBean("postgresqlPort");
        }

        htmlStorageLocation = (String) context.getBean("htmlStorageLocation");

        DataFrame sourceTableDF;
        DataFrame destinationTableDF;
        DataFrame sqlServerTableSchema;
        long sourceTableCount;
        long destinationTableCount;

        String[] destinationTableFields;

        // Getting Source Table into a DataFrame
        if (!Objects.equals(sourceName, "Hive")){

            // Get the schema from the table in Sql Source
            Map<String, String> sqlSchemaOptions = new HashMap<>();
            if(Objects.equals(sourceType, "mysql")){
                sqlSchemaOptions.put("url",sourceSqlUrl + ":" + sourceSqlPort +"/"+sourceDatabase);
                sqlSchemaOptions.put("user",sourceSqlUsername);
                sqlSchemaOptions.put("password",sourceSqlPassword);
            }else if(Objects.equals(sourceType, "postgresql")){
                sqlSchemaOptions.put("url", sourceSqlUrl+":"+sourceSqlPort+"/"+sourceDatabase+"?user="+sourceSqlUsername+"&password="+sourceSqlPassword);
            }else if(Objects.equals(sourceType, "mssql")){
                sqlSchemaOptions.put("url", sourceSqlUrl + ":" + sourceSqlPort + ";user=" + sourceSqlUsername + ";password=" + sourceSqlPassword + ";databaseName=" + sourceDatabase);
            }
            sqlSchemaOptions.put("dbtable", "(select DATA_TYPE,COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='" + sourceTable + "') select_telarix");
            sqlSchemaOptions.put("driver", sourceSqlDriver);

            sqlServerTableSchema = hiveContext.read().format("jdbc").options(sqlSchemaOptions).load();
            String sqlQuery = "(SELECT * FROM "+sourceTable;
            if (null != whereClause) {
                sqlQuery += " where (" + whereClause + ")";
            }

            // Get the data from table in Sql Source
            Map<String, String> sqlQueryOptions = new HashMap<>();
            if(Objects.equals(sourceType, "mysql")){
                sqlQueryOptions.put("url",sourceSqlUrl + ":" + sourceSqlPort +"/"+sourceDatabase);
                sqlQueryOptions.put("user",sourceSqlUsername);
                sqlQueryOptions.put("password",sourceSqlPassword);
            }else if(Objects.equals(sourceType, "postgresql")){
                sqlQueryOptions.put("url", sourceSqlUrl+":"+sourceSqlPort+"/"+sourceDatabase+"?user="+sourceSqlUsername+"&password="+sourceSqlPassword);
            }
            else if(Objects.equals(sourceType, "mssql")){
                sqlQueryOptions.put("url", sourceSqlUrl + ":" + sourceSqlPort + ";user=" + sourceSqlUsername + ";password=" + sourceSqlPassword + ";databaseName=" + sourceDatabase);
            }
            sqlQueryOptions.put("dbtable", sqlQuery + ") select_telarix");
            sqlQueryOptions.put("driver", sourceSqlDriver);

            sourceTableDF = hiveContext.read().format("jdbc").options(sqlQueryOptions).load();
            sourceTableCount = sourceTableDF.count();
            if(sourceTableCount == 0){
                System.out.println("Source Table is Empty");
                System.exit(0);
            }

            // Casting the columns in dataframe where Sql table is stored
            sourceTableDF = castColumns(sqlServerTableSchema, sourceTableDF);

        }else{

            // Get the data from Hive Table
            String hiveQuery = "SELECT * FROM " + sourceDatabase + "." + sourceTable;
            if (null != whereClause) {
                hiveQuery += " where (" + whereClause + ")";
            }
            sourceTableDF = hiveContext.sql(hiveQuery);
            destinationTableFields = sourceTableDF.schema().fieldNames();
            for(String column : destinationTableFields){
                sourceTableDF = sourceTableDF.withColumnRenamed(column,column.toLowerCase());
            }
            sourceTableCount = sourceTableDF.count();
            if(sourceTableCount == 0){
                System.out.println("Source Table is Empty");
                System.exit(0);
            }
        }

        // Getting Destination Table into a DataFrame
        if (!Objects.equals(destinationName, "Hive")){

            // Get the schema from the table in sql server
            Map<String, String> sqlSchemaOptions = new HashMap<>();
            if(Objects.equals(destinationType, "mysql")){
                sqlSchemaOptions.put("url",destinationSqlUrl + ":" + destinationSqlPort +"/"+destinationDatabase);
                sqlSchemaOptions.put("user",destinationSqlUsername);
                sqlSchemaOptions.put("password",destinationSqlPassword);
            }else if(Objects.equals(destinationType, "postgresql")){
                sqlSchemaOptions.put("url", destinationSqlUrl+":"+destinationSqlPort+"/"+destinationDatabase+"?user="+destinationSqlUsername+"&password="+destinationSqlPassword);
            }
            else if(Objects.equals(destinationType, "mssql")){
                sqlSchemaOptions.put("url", destinationSqlUrl + ":" + destinationSqlPort + ";user=" + destinationSqlUsername + ";password=" + destinationSqlPassword + ";databaseName=" + destinationDatabase);
            }
            sqlSchemaOptions.put("dbtable", "(select DATA_TYPE,COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='" + destinationTable + "') select_telarix");
            sqlSchemaOptions.put("driver", destinationSqlDriver);

            sqlServerTableSchema = hiveContext.read().format("jdbc").options(sqlSchemaOptions).load();

            String sqlQuery = "(SELECT * FROM "+destinationTable;
            if (null != whereClause) {
                sqlQuery += " where (" + whereClause + ")";
            }

            // Get the data from table in Sql Server
            Map<String, String> sqlQueryOptions = new HashMap<>();
            if(Objects.equals(destinationType, "mysql")){
                sqlQueryOptions.put("url",destinationSqlUrl + ":" + destinationSqlPort +"/"+destinationDatabase);
                sqlQueryOptions.put("user",destinationSqlUsername);
                sqlQueryOptions.put("password",destinationSqlPassword);
            }else if(Objects.equals(destinationType, "postgresql")){
                sqlQueryOptions.put("url", destinationSqlUrl+":"+destinationSqlPort+"/"+destinationDatabase+"?user="+destinationSqlUsername+"&password="+destinationSqlPassword);
            }
            else if(Objects.equals(destinationType, "mssql")){
                sqlQueryOptions.put("url", destinationSqlUrl + ":" + destinationSqlPort + ";user=" + destinationSqlUsername + ";password=" + destinationSqlPassword + ";databaseName=" + destinationDatabase);
            }
            sqlQueryOptions.put("dbtable", sqlQuery + ") select_telarix");
            sqlQueryOptions.put("driver", destinationSqlDriver);

            destinationTableDF = hiveContext.read().format("jdbc").options(sqlQueryOptions).load();
            destinationTableCount = destinationTableDF.count();
            if(destinationTableCount == 0){
                System.out.println("Destination Table is Empty");
                System.exit(0);
            }

            // Casting the columns in dataframe where sqlserver table is stored
            destinationTableDF = castColumns(sqlServerTableSchema, destinationTableDF);

        }else {

            // Get the data from Hive Table
            String hiveQuery = "SELECT * FROM " + destinationDatabase + "." + destinationTable;
            if (null != whereClause) {
                hiveQuery += " where (" + whereClause + ")";
            }
            destinationTableDF = hiveContext.sql(hiveQuery);
            destinationTableFields = destinationTableDF.schema().fieldNames();
            for(String column : destinationTableFields){
                destinationTableDF = destinationTableDF.withColumnRenamed(column,column.toLowerCase());
            }
            destinationTableCount = destinationTableDF.count();
            if(destinationTableCount == 0){
                System.out.println("Destination Table is Empty");
                System.exit(0);
            }

        }

        // Excluding Columns as requested by the User
        if (!excludeColumns.isEmpty()) {
            for (String column : excludeColumns) {
                sourceTableDF = sourceTableDF.drop(column);
                destinationTableDF = destinationTableDF.drop(column.toLowerCase());
            }
        }
        sourceTableDF.registerTempTable("sql_table");
        destinationTableDF.registerTempTable("hive_table");

        // Finding the Common Columns in Both SqlServer and Hive
        String[] sourceTableFields = sourceTableDF.schema().fieldNames();
        ArrayList<String> sourceTableFieldsMismatchedList = new ArrayList<>(Arrays.asList(sourceTableFields));
        destinationTableFields = destinationTableDF.schema().fieldNames();
        ArrayList<String> destinationTableFieldsMismatchedList = new ArrayList<>(Arrays.asList(destinationTableFields));

        ArrayList<String> commonColumnsSource = new ArrayList<>();
        ArrayList<String> commonColumnsDestination = new ArrayList<>();

        // Getting Common Columns from Both SqlServer and Hive
        for (String column : sourceTableFields) {
            if (ArrayUtils.contains(destinationTableFields, column.toLowerCase())) {
                commonColumnsSource.add(column);
                commonColumnsDestination.add(column.toLowerCase());

            }
        }

        if (commonColumnsSource.isEmpty() && commonColumnsDestination.isEmpty()) {
            System.out.println("Schema for Source and destination tables is not matching.");
            System.out.println("==============================================================");
            System.out.println("Tables are  Not equal");
            System.out.println("==============================================================");
            System.exit(0);
        }

        sourceTableFieldsMismatchedList.removeAll(commonColumnsSource);
        destinationTableFieldsMismatchedList.removeAll(commonColumnsDestination);

        StringBuilder matchedColumnsSource = new StringBuilder();
        StringBuilder matchedColumnsDestination = new StringBuilder();
        for (String s : commonColumnsSource) {
            matchedColumnsSource.append(s);
            matchedColumnsSource.append(",");
            matchedColumnsDestination.append(s.toLowerCase());
            matchedColumnsDestination.append(",");
        }
        matchedColumnsSource.setLength(matchedColumnsSource.length() - 1);
        matchedColumnsDestination.setLength(matchedColumnsDestination.length() - 1);

        // Sql Table with Matched Columns
        sourceTableDF = hiveContext.sql("select " + matchedColumnsSource.toString() + " from sql_table");
        sourceTableDF.registerTempTable("sql_table");

        // Hive Table with Matched Columns
        destinationTableDF = hiveContext.sql("SELECT " + matchedColumnsDestination.toString() + " FROM hive_table");
        destinationTableDF.registerTempTable("hive_table");

        // Getting Column names from both Source and Destination To Display them in the Html Page.
        String[] sqlColumns = sourceTableDF.columns();
        StringBuilder columnsForFinalTableDisplay = new StringBuilder();
        for (String s : sqlColumns) {
            destinationTableDF = destinationTableDF.withColumnRenamed(s.toLowerCase(), s + "_hive");
            String destinationColName = s + "_hive";
            columnsForFinalTableDisplay.append(s);
            columnsForFinalTableDisplay.append(",");
            columnsForFinalTableDisplay.append(destinationColName);
            columnsForFinalTableDisplay.append(",");
        }
        columnsForFinalTableDisplay.setLength(columnsForFinalTableDisplay.length() - 1);

        //TODO: Handle Duplicate Rows

        try {

            // Columns in Sql but not in hive
            DataFrame dataInSourceButNotDestination = sourceTableDF.except(destinationTableDF);
            DataFrame dataInDestinationButNotSource = destinationTableDF.except(sourceTableDF);
            if (dataInSourceButNotDestination.count() == 0 && dataInDestinationButNotSource.count() == 0) {
                System.out.println("==============================================================");
                System.out.println("Tables are equal");
                System.out.println("==============================================================");
            } else {

                // Getting Cartesian Product
                sourceTableDF = sourceTableDF.withColumn("index", functions.monotonically_increasing_id());
                DataFrame cartesianProduct = sourceTableDF.join(destinationTableDF);
                StringBuilder concatenatedColumnNames = new StringBuilder();
                List<String> fullColumnsUnMatched = new ArrayList<>();
                double columnSum;

                // Column Comparision
                for (String s : sqlColumns) {
                    Column sqlCol = cartesianProduct.col(s);
                    Column hiveCol = cartesianProduct.col(s + "_hive");

                    String columnName = s + "concat_col";
                    cartesianProduct = cartesianProduct.withColumn(columnName, callUDF("columnsCompare", sqlCol.cast("String"), hiveCol.cast("String")));
                    cartesianProduct = cartesianProduct.withColumn(s+"_comparision", callUDF("columnsStringComparision", sqlCol.cast("String"), hiveCol.cast("String")));
                    cartesianProduct.registerTempTable("unmatched_columns_test");
                    columnSum = hiveContext.sql("select sum("+s+"_comparision) from unmatched_columns_test").collect()[0].getDouble(0);
                    if(columnSum == 0.0){
                        fullColumnsUnMatched.add(s);
                    }
                    concatenatedColumnNames.append(columnName);
                    concatenatedColumnNames.append("+");
                }

                concatenatedColumnNames.setLength(concatenatedColumnNames.length() - 1);
                System.out.println("Fully Unmatched Columns: "+fullColumnsUnMatched);

                cartesianProduct.registerTempTable("cartesian_product");
                DataFrame concatenatedColumnsInCartesianProduct = hiveContext.sql("select *," + concatenatedColumnNames.toString() + " as total  from cartesian_product");
                concatenatedColumnsInCartesianProduct.registerTempTable("final_results");
                hiveContext.sql("select index," + columnsForFinalTableDisplay.toString() + ",total from final_results").registerTempTable("final");
                hiveContext.sql("select index,max(total) as max_total from final_results group by index").registerTempTable("final_grouped");
                DataFrame finalResults = hiveContext.sql("SELECT b.* FROM final_grouped a LEFT JOIN final b ON a.max_total=b.total  and a.index=b.index WHERE a.max_total IS NOT NULL and a.max_total<>" + sourceTableFields.length + " order by b.index");
                finalResults.registerTempTable("final_table");
                finalResults = hiveContext.sql("select index," + columnsForFinalTableDisplay.toString() + " from final_table");
                String columnsOfFinalResults[] = finalResults.columns();


                // Building Html
                StringBuilder htmlStringBuilder = new StringBuilder();

                htmlStringBuilder.append("<h2 align=\"center\" color=\"gray\"> Database Comparision Tool </h2>");

                htmlStringBuilder.append("<html><head><style>table {font-family: arial, sans-serif;border-collapse:collapse;width: 100%;}td, th {border: 1px solid #dddddd;text-align: left;padding: 8px;}</style></head><body><table>");

                htmlStringBuilder.append("<tr><th></th><th>SQL Server</th><th>Hive</th></tr>");
                htmlStringBuilder.append("<tr><td>DataBase name</td><td>").append(sourceDatabase).append("</td><td>").append(destinationDatabase).append("</td>");
                htmlStringBuilder.append("<tr><td>Table name</td><td>").append(sourceTable).append("</td><td>").append(destinationTable).append("</td>");
                htmlStringBuilder.append("<tr><td>Row Count</td><td>").append(sourceTableCount).append("</td><td>").append(destinationTableCount).append("</td>");
                htmlStringBuilder.append("<tr><td>Mis-matched Schema</td><td>").append(sourceTableFieldsMismatchedList).append("</td><td>").append(destinationTableFieldsMismatchedList).append("</td>");
                htmlStringBuilder.append("</tr>");
                htmlStringBuilder.append("</table></body></html>");

                if(!excludeColumns.isEmpty()){
                    htmlStringBuilder.append("<ul><font color=\"blue\"><li> Excluded Columns: ").append(excludeColumns).append(" </li></font></ul>");
                }else{
                    htmlStringBuilder.append("<ul><font color=\"blue\"><li> User didn't Exclude any Columns  </li></font></ul>");
                }

                if (null != whereClause){
                    htmlStringBuilder.append("<ul><font color=\"blue\"><li> where Clause: ").append(whereClause).append(" </li></font></ul>");
                }else{
                    htmlStringBuilder.append("<ul><font color=\"blue\"><li> User didn't provide where Clause  </li></font></ul>");
                }


                htmlStringBuilder.append("<html><head><style>table {font-family: arial, sans-serif;border-collapse:collapse;width: 100%;}td, th {border: 1px solid #dddddd;text-align: left;padding: 8px;}</style></head><body><table>");
                for (String s : columnsOfFinalResults) {
                    htmlStringBuilder.append("<th>").append(s).append("</th>");
                }
                htmlStringBuilder.append("</tr>");

                finalResults = finalResults.withColumn("html_col", lit("<tr>"));
                finalResults = finalResults.withColumn("index", concat(lit("<td>"), finalResults.col("index"), lit("</td>")));
                finalResults = finalResults.withColumn("html_col", concat(finalResults.col("html_col"),finalResults.col("index")));

                for (int i = 1; i < (finalResults.columns().length) - 1; i = i + 2) {
                    Column a = finalResults.col(finalResults.columns()[i]);
                    Column b = finalResults.col(finalResults.columns()[i + 1]);
                    finalResults = finalResults.withColumn("html_col", concat(finalResults.col("html_col"), callUDF("htmlGenerator", a.cast("String"), b.cast("String"))));
                }
                finalResults = finalResults.withColumn("html_col", concat(finalResults.col("html_col"), lit("</tr>")));

                finalResults = finalResults.select("html_col");
                Row[] dataRows = finalResults.collect();

                for (Row row : dataRows) {
                    htmlStringBuilder.append(row.get(0));
                    htmlStringBuilder.append("\n");
                }

                if (fullColumnsUnMatched.isEmpty()) {
                    htmlStringBuilder.append("<ul><font color=\"red\"><li> There are No Fully Unmatched Columns </li></font></ul>");
                } else {
                    // TODO:Change it in Future to Show Full Columns if needed.
                    htmlStringBuilder.append("<p><font color=\"red\"> Fully Unmatched Columns: ").append(fullColumnsUnMatched.toString()).append("</font></p>");
                }

                htmlStringBuilder.append("</table></body></html>");

                BufferedWriter writer = new BufferedWriter(new FileWriter(htmlStorageLocation + sourceTable + timestamp + ".html"));
                writer.write(htmlStringBuilder.toString());
                writer.close();

                System.out.println("==============================================================");
                System.out.println("use http://localhost:49090/" + sourceTable + timestamp + ".html  link to view the Differences");
                System.out.println("==============================================================");

            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.out.println("==============================================================");
            System.out.println("Tables are not equal");
            System.out.println("==============================================================");
        }
    }

    // Casting Columns
    private static DataFrame castColumns(DataFrame tableForSchema, DataFrame table) {
        JavaRDD<Row> row_rdd = tableForSchema.javaRDD();
        List<Row> rows = row_rdd.collect();

        for (Row row : rows) {
            String col1 = row.getString(0);
            String col2 = row.getString(1);
            String map_col;

            switch (col1) {
                case "datetime":
                case "date":
                    map_col = "timestamp";
                    break;
                case "bit":
                    map_col = "Integer";
                    break;
                case "Money":
                    map_col = "decimal(19,4)";
                    break;
                case "decimal":
                    map_col = "decimal(25,6)";
                    break;
                default:
                    map_col = null;
                    break;
            }
            if (map_col != null) {
                table = table.withColumn(col2, table.col(col2).cast(map_col));
            }
        }
        return table;
    }
}
