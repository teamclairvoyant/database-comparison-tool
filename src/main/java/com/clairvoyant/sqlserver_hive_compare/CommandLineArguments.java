package com.clairvoyant.sqlserver_hive_compare;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


public class CommandLineArguments {

    @Option(name="-sourceName",usage="Source Name. i.e either one of the Sql type databases or Hive")
    private static String sourceName;

    @Option(name="-destinationName",usage="Destination Name. i.e either one of the Sql type databases or Hive")
    private static String destinationName;

    @Option(name="-sourceType",usage="Source Type. i.e Sql or Hive")
    private static String sourceType;

    @Option(name="-destinationType",usage="Destination Type. i.e Sql or Hive")
    private static String destinationType;

    @Option(name="-sourceDatabase",usage="SqlServer Database name")
    private String sourceDatabase;

    @Option(name="-destinationDatabase",usage="SqlServer Database name")
    private String destinationDatabase;

    @Option(name="-sourceTable",usage="SqlServer Table name")
    private String sourceTable;

    @Option(name="-destinationTable",usage="SqlServer Table name")
    private String destinationTable;

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

    public static String getsourceName() {
        return sourceName;
    }

    public void setsourceName(String sourceName) {
        CommandLineArguments.sourceName = sourceName;
    }

    public static String getdestinationName() {
        return destinationName;
    }

    public void setdestinationName(String destinationName) {
        CommandLineArguments.destinationName = destinationName;
    }

    public static String getsourceType() {
        return sourceType;
    }

    public void setsourceType(String sourceType) {
        CommandLineArguments.sourceType = sourceType;
    }

    public static String getdestinationType() {
        return destinationType;
    }

    public void setdestinationType(String destinationType) {
        CommandLineArguments.destinationType = destinationType;
    }

    public String getsourceDatabase() {
        return sourceDatabase;
    }

    public void setsourceDatabase(String sourceDatabase) {
        this.sourceDatabase = sourceDatabase;
    }

    public String getdestinationDatabase() {
        return destinationDatabase;

    }

    public void setdestinationDatabase(String destinationDatabase) {
        this.destinationDatabase = destinationDatabase;
    }

    public String getsourceTable() {
        return sourceTable;
    }

    public void setsourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getdestinationTable() {
        return destinationTable;
    }

    public void setdestinationTable(String destinationTable) {
        this.destinationTable = destinationTable;
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
