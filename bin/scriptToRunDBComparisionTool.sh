#!/bin/bash

function usage()
{
    echo """ Run the script by providing the arguments that are required to run the spark-submit for the tool.


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
        sh tes.sh -sourceName {SOURCE_NAME} -destinationName {DESTINATION_NAME} -sourceType {SOURCE_TYPE} -destinationType {DESTINATION_TYPE} -sourceDatabase {SOURCE_DATABASE} -sourceTable {SOURCE_TABLE} -destinationDatabase {DESTINATION_DATABASE} -destinationTable {DESTINATION_TABLE}"""
}



OPTS=`getopt long help,sourceName,destinationName,sourceType,destinationType,sourceDatabase,sourceTable,destinationDatabase,destinationTable: -n 'parse-options' -- "$@"`

if [ $? != 0 ] ; then
  echo "Failed parsing options." >&2
  exit 1
fi

eval set -- "$OPTS"

shift 4;

# Spark Submit Command
CMD="""spark-submit --driver-class-path /app/db_comparison/conf /app/db_comparison/lib/sqlserver_hive_compare-jar-with-dependencies.jar"""

while true; do

  case "$1" in

    --help ) usage; exit 0; shift ;;

    # Source Database and Tables
    --sourceName ) SOURCENAME="$2" shift 2 ;;

    --destinationName ) DESTINATIONNAME="$2" shift 2 ;;

    --sourceType ) SOURCETYPE="$2" shift 2 ;;

    --destinationType ) DESTINATIONTYPE="$2" shift 2 ;;

    --sourceDatabase ) SOURCEDATABASE="$2" shift 2 ;;

    --sourceTable ) SOURCETABLE="$2" shift 2 ;;

    --destinationDatabase ) DESTINATIONDATABASE="$2" shift 2 ;;

    --destinationTable ) DESTINATIONTABLE="$2" shift 2 ;;


    # Exclude Columns and Where clause
    --excludeColumns ) EXCLUDECOLUMNS="$2" shift 2 ;;

    --where ) WHERECLAUSE="$2" shift 2 ;;

    -- ) shift break ;;
    * ) if [ -z "$1" ]; then break; else echo "$1 is not a valid option"; exit 1; fi;;
  esac
done

if [[ -z "${SOURCENAME}" ]]; then
    echo "Source Name is not given"
    exit -1
fi

if [[ -z "${DESTINATIONNAME}" ]]; then
    echo "Destination Name is not given"
    exit -1
fi

if [[ -z "${SOURCETYPE}" ]]; then
    echo "Source Type is not given"
    exit -1
fi

if [[ -z "${DESTINATIONTYPE}" ]]; then
    echo "Destination Type is not given"
    exit -1
fi

if [[ -z "${SOURCEDATABASE}" ]]; then
    echo "Source Database is not given"
    exit -1
fi

if [[ -z "${SOURCETABLE}" ]]; then
    echo "Source Table is not given"
    exit -1
fi

if [[ -z "${DESTINATIONDATABASE}" ]]; then
    echo "Destination Database is not given"
    exit -1
fi

if [[ -z "${DESTINATIONTABLE}" ]]; then
    echo "Destination Table is not given"
    exit -1
fi

CMD="${CMD} -sourceName ${SOURCENAME} -destinationName ${DESTINATIONNAME} -sourceType ${SOURCETYPE} -destinationType ${DESTINATIONTYPE} -sourceDatabase ${SOURCEDATABASE} -sourceTable ${SOURCETABLE} -destinationDatabase ${DESTINATIONDATABASE} -destinationTable ${DESTINATIONTABLE}"

if [[ "${EXCLUDECOLUMNS}" != "" ]]; then
    CMD="${CMD} -excludeColumns ${EXCLUDECOLUMNS}"
fi

if [[ "${WHERECLAUSE}" != "" ]]; then
    CMD="${CMD} -where ${WHERECLAUSE}"
fi

echo "${CMD}"

$CMD
