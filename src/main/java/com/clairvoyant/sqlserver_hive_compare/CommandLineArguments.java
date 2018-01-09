package com.clairvoyant.sqlserver_hive_compare;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


public class CommandLineArguments {

    @Option(name="-sqlDatabase",usage="SqlServer Database name")
    private String sqlDatabase;

    @Option(name="-sqlTable",usage="SqlServer Table name")
    private String sqlTable;

    @Option(name="-hiveDatabase",usage="Hive Database name")
    private String hiveDatabase;

    @Option(name="-hiveTable",usage="Hive Table name")
    private String hiveTable;

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

    public String getsqlDatabase() {
        return sqlDatabase;
    }

    public void setsqlDatabase(String sqlDatabase) {
        this.sqlDatabase = sqlDatabase;
    }

    public String getsqlTable() {
        return sqlTable;
    }

    public void setsqlTable(String sqlTable) {
        this.sqlTable = sqlTable;
    }

    public String gethiveDatabase() {
        return hiveDatabase;
    }

    public void sethiveDatabase(String hiveDatabase) {
        this.hiveDatabase = hiveDatabase;
    }

    public String gethiveTable() {
        return hiveTable;
    }

    public void sethiveTable(String hiveTable) {
        this.hiveTable = hiveTable;
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
