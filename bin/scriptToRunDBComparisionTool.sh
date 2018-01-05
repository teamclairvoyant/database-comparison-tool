#!/bin/bash
while getopts "s:t:h:p:e:w" o; do
    case "${o}" in
        s)
            s=${OPTARG}
            ;;
        t)
            t=${OPTARG}
            ;;
        h)
            h=${OPTARG}
            ;;
        p)
            p=${OPTARG}
            ;; 
        e)
            e=${OPTARG}
            ;;
        w)
            w=${OPTARG}
            ;;              
    esac
done

if [ -z "${s}" ]; then
    echo "${s} is not set:"
    exit -1
fi

if [ -z "${t}" ]; then
    echo "${t} is not set:"
    exit -1
fi

if [ -z "${h}" ]; then
    echo "${h} is not set:"
    exit -1
fi

if [ -z "${p}" ]; then
    echo "${p} is not set:"
    exit -1
fi

#Spark-submit Command

CMD="""spark-submit --driver-memory 4g --driver-class-path  /app/db_comparision/conf /app/db_comparision/lib/sqlserver_hive_compare-jar-with-dependencies.jar -sqlDatabase ${s} -sqlTable ${t} -hiveDatabase ${h} -hiveTable ${p} """

if [ "${e}" != "" ]; then
    CMD="${CMD} -excludeColumns ${e}" 
fi

if [ "${w}" != "" ]; then
    CMD="${CMD} -where ${e}" 
fi

echo "Executing the Command: "
echo "${CMD}"
$CMD
