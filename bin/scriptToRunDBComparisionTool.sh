#!/bin/bash

function usage()
{
    echo """ Run the script by providing the arguments that are required to run the spark-submit for the tool.


Required Arguments
    --sourceSqlDatabase       Source SqlServer Database Name         **OR**   --sourceHiveDatabase       Source Hive Database Name
    --sourceSqlTable          Source SqlServer Table Name            **OR**   --sourceHiveTable          Source Hive Table Name
    --destinationSqlDatabase  Destination SqlServer Database Name    **OR**   --destinationHiveDatabase  Destination Hive Database Name
    --destinationSqlTable     Destination SqlServer Table Name       **OR**   --destinationHiveTable     Destination Hive Table Name


Optional Arguments
    --excludeColumns   Columns to exclude
    --where            where clause



Example:
        sh scriptToRunDBComparisionTool.sh --sourceSqlDatabase {Sql_database_name} --sourceSqlTable {sql_table_name} --destinationSqlDatabase {Sql_database_name} --destinationSqlTable {sql_table_name} --excludeColumns {columns_to_exclude} --where {where_clause}

        sh scriptToRunDBComparisionTool.sh --sourceSqlDatabase {Sql_database_name} --sourceSqlTable {sql_table_name} --destinationHiveDatabase {hive_database_name} --destinationHiveTable {hive_table_name} --excludeColumns {columns_to_exclude} --where {where_clause}

        sh scriptToRunDBComparisionTool.sh --sourceHiveDatabase {hive_database_name} --sourceHiveTable {hive_table_name} --destinationSqlDatabase {Sql_database_name} --destinationSqlTable {sql_table_name} --excludeColumns {columns_to_exclude} --where {where_clause}

        sh scriptToRunDBComparisionTool.sh --sourceHiveDatabase {hive_database_name} --sourceHiveTable {hive_table_name} --destinationHiveDatabase {hive_database_name} --destinationHiveTable {hive_table_name} --excludeColumns {columns_to_exclude} --where {where_clause}"""
}



OPTS=`getopt long help,sourceSqlDatabase,sourceSqlTable,sourceHiveDatabase,sourceHiveTable,destinationSqlDatabase,destinationSqlTable,destinationHiveDatabase,destinationHiveTable: -n 'parse-options' -- "$@"`

if [ $? != 0 ] ; then
  echo "Failed parsing options." >&2
  exit 1
fi

eval set -- "$OPTS"

shift 4;

HELP=false
STACK_SIZE=0

# Spark Submit Command
CMD="""spark-submit --driver-class-path /app/db_comparison/conf /app/db_comparison/lib/sqlserver_hive_compare-jar-with-dependencies.jar"""

while true; do

  case "$1" in

    --help ) usage; exit 0; shift ;;

    # Source Database and Tables
    --sourceSqlDatabase ) SOURCESQLDATABASE="$2" shift 2 ;;

    --sourceSqlTable ) SOURCESQLTABLE="$2" shift 2 ;;

    --sourceHiveDatabase ) SOURCEHIVEDATABASE="$2" shift 2 ;;

    --sourceHiveTable ) SOURCEHIVETABLE="$2" shift 2 ;;

    # Destination Database and Tables
    --destinationSqlDatabase ) DESTINATIONSQLDATABASE="$2" shift 2 ;;

    --destinationSqlTable ) DESTINATIONSQLTABLE="$2" shift 2 ;;

    --destinationHiveDatabase ) DESTINATIONHIVEDATABASE="$2" shift 2 ;;

    --destinationHiveTable ) DESTINATIONHIVETABLE="$2" shift 2 ;;


    # Exclude Columns and Where clause
    --excludeColumns ) EXCLUDECOLUMNS="$2" shift 2 ;;

    --where ) WHERECLAUSE="$2" shift 2 ;;

    -- ) shift break ;;
    * ) if [ -z "$1" ]; then break; else echo "$1 is not a valid option"; exit 1; fi;;
  esac
done

if [[ -z "${SOURCESQLDATABASE}" && -z "${SOURCEHIVEDATABASE}" ]]; then
    echo "Source Database is not given"
    exit -1
fi

if [[ -z "${SOURCESQLTABLE}" && -z "${SOURCEHIVETABLE}" ]]; then
    echo "Source Table is not given"
    exit -1
fi

if [[ -z "${DESTINATIONSQLDATABASE}" && -z "${DESTINATIONHIVEDATABASE}" ]]; then
    echo "Destination Database is not given"
    exit -1
fi

if [[ -z "${DESTINATIONSQLTABLE}" && -z "${DESTINATIONHIVETABLE}" ]]; then
    echo "Destination Table is not given"
    exit -1
fi



if [ "${SOURCESQLDATABASE}" != "" ]; then
    CMD="${CMD} -sourceSqlDatabase ${SOURCESQLDATABASE}"
    CMD="${CMD} -sourceSqlTable ${SOURCESQLTABLE}"
else
     CMD="${CMD} -sourceHiveDatabase ${SOURCEHIVEDATABASE}"
     CMD="${CMD} -sourceHiveTable ${SOURCEHIVETABLE}"
fi

if [ "${DESTINATIONSQLDATABASE}" != "" ]; then
    CMD="${CMD} -destinationSqlDatabase ${DESTINATIONSQLDATABASE}"
    CMD="${CMD} -destinationSqlTable ${DESTINATIONSQLTABLE}"
else
     CMD="${CMD} -destinationHiveDatabase ${DESTINATIONHIVEDATABASE}"
     CMD="${CMD} -destinationHiveTable ${DESTINATIONHIVETABLE}"
fi


if [ "${EXCLUDECOLUMNS}" != "" ]; then
    CMD="${CMD} -excludeColumns ${EXCLUDECOLUMNS}"
fi

if [ "${WHERECLAUSE}" != "" ]; then
    CMD="${CMD} -where ${WHERECLAUSE}"
fi

echo "${CMD}"

$CMD