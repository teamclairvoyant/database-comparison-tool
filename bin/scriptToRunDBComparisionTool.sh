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

        sh scriptToRunDBComparisionTool.sh --sourceSqlDatabase={Sql_database_name} --sourceSqlTable={sql_table_name} --destinationSqlDatabase={Sql_database_name} --destinationSqlTable={sql_table_name} --excludeColumns={columns_to_exclude} --where={where_clause}

        sh scriptToRunDBComparisionTool.sh --sourceSqlDatabase={Sql_database_name} --sourceSqlTable={sql_table_name} --destinationHiveDatabase={hive_database_name} --destinationHiveTable={hive_table_name} --excludeColumns={columns_to_exclude} --where={where_clause}

        sh scriptToRunDBComparisionTool.sh --sourceHiveDatabase={hive_database_name} --sourceHiveTable={hive_table_name} --destinationSqlDatabase={Sql_database_name} --destinationSqlTable={sql_table_name} --excludeColumns={columns_to_exclude} --where={where_clause}

        sh scriptToRunDBComparisionTool.sh --sourceHiveDatabase={hive_database_name} --sourceHiveTable={hive_table_name} --destinationHiveDatabase={hive_database_name} --destinationHiveTable={hive_table_name} --excludeColumns={columns_to_exclude} --where={where_clause}"""
}

CMD="""spark-submit --driver-class-path /home/ixadmin_ext/aravind/spark_code/sqlserver_hive_compare target/sqlserver_hive_compare-jar-with-dependencies.jar"""
#echo $1
while [ "$1" != "" ]; do
    PARAM=`echo $1 | awk -F= '{print $1}'`
    VALUE=`echo $1 | awk -F= '{print $2}'`

    if [ "$PARAM" == "--help" ]; then
        usage
        exit 0
    fi

    if [ "$PARAM" == "--sourceSqlDatabase" ]; then
        SOURCESQLDATABASE="${VALUE}"

    elif [ "$PARAM" == "--sourceSqlTable" ]; then
        SOURCESQLTABLE="${VALUE}"

    elif [ "$PARAM" == "--sourceHiveDatabase" ]; then
        SOURCEHIVEDATABASE="${VALUE}"

    elif [ "$PARAM" == "--sourceHiveTable" ]; then
        SOURCEHIVETABLE="${VALUE}"

    elif [ "$PARAM" == "--destinationSqlDatabase" ]; then
        DESTINATIONSQLDATABASE="${VALUE}"

    elif [ "$PARAM" == "--destinationSqlTable" ]; then
        DESTINATIONSQLTABLE="${VALUE}"

    elif [ "$PARAM" == "--destinationHiveDatabase" ]; then
        DESTINATIONHIVEDATABASE="${VALUE}"

    elif [ "$PARAM" == "--destinationHiveTable" ]; then
        DESTINATIONHIVETABLE="${VALUE}"

    fi

    if [ "$PARAM" == "--excludeColumns" ]; then
        EXCLUDECOLUMNS="${VALUE}"
    fi

    if [ "$PARAM" == "--where" ]; then
        WHERECLAUSE="${VALUE}"
    fi
    shift
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