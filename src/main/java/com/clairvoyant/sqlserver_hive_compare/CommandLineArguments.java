package com.clairvoyant.sqlserver_hive_compare;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


public class CommandLineArguments {

    @Option(name="-sourceSqlDatabase",usage="SqlServer Database name")
    private String sourceSqlDatabase = null;

    @Option(name="-destinationSqlDatabase",usage="SqlServer Database name")
    private String destinationSqlDatabase = null;

    @Option(name="-sourceSqlTable",usage="SqlServer Table name")
    private String sourceSqlTable = null;

    @Option(name="-destinationSqlTable",usage="SqlServer Table name")
    private String destinationSqlTable = null;

    @Option(name="-sourceHiveDatabase",usage="Hive Database name")
    private String sourceHiveDatabase = null;

    @Option(name="-destinationHiveDatabase",usage="Hive Database name")
    private String destinationHiveDatabase = null;

    @Option(name="-sourceHiveTable",usage="Hive Table name")
    private String sourceHiveTable = null;

    @Option(name="-destinationHiveTable",usage="Hive Table name")
    private String destinationHiveTable = null;

    @Option(name="-where",usage="where clause to query partitioned hive tables")
    private String whereClause = null;

    @Option(name="-excludeColumns",usage="Columns to Exclude from Comparision")
    private String excludeColumns = null;

    public CommandLineArguments(String... args) throws CmdLineException {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            throw e;
        }
    }

    public String getsourceSqlDatabase() {
        return sourceSqlDatabase;
    }

    public void setsourceSqlDatabase(String sourceSqlDatabase) {
        this.sourceSqlDatabase = sourceSqlDatabase;
    }

    public String getdestinationSqlDatabase() {
        return destinationSqlDatabase;
    }

    public void setdestinationSqlDatabase(String destinationSqlDatabase) {
        this.destinationSqlDatabase = destinationSqlDatabase;
    }

    public String getsourceSqlTable() {
        return sourceSqlTable;
    }

    public void setsourceSqlTable(String sourceSqlTable) {
        this.sourceSqlTable = sourceSqlTable;
    }

    public String getdestinationSqlTable() {
        return destinationSqlTable;
    }

    public void setdestinationSqlTable(String destinationSqlTable) {
        this.destinationSqlTable = destinationSqlTable;
    }

    public String getsourceHiveDatabase() {
        return sourceHiveDatabase;
    }

    public void setsourceHiveDatabase(String sourceHiveDatabase) {
        this.sourceHiveDatabase = sourceHiveDatabase;
    }

    public String getdestinationHiveDatabase() {
        return destinationHiveDatabase;
    }

    public void setdestinationHiveDatabase(String destinationHiveDatabase) {
        this.destinationHiveDatabase = destinationHiveDatabase;
    }

    public String getsourceHiveTable() {
        return sourceHiveTable;
    }

    public void setsourceHiveTable(String sourceHiveTable) {
        this.sourceHiveTable = sourceHiveTable;
    }

    public String getdestinationHiveTable() {
        return destinationHiveTable;
    }

    public void setdestinationHiveTable(String destinationHiveTable) {
        this.destinationHiveTable = destinationHiveTable;
    }

    public String getwhereClause() {
        return whereClause;
    }

    public void setwhereClause(String where) {
        this.whereClause = whereClause;
    }

    public String getexcludeColumns() {
        return excludeColumns;
    }

    public void setexcludeColumns(String excludeColumns) {
        this.excludeColumns = excludeColumns;
    }
}
