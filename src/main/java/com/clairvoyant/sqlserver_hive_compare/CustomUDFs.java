package com.clairvoyant.sqlserver_hive_compare;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import info.debatty.java.stringsimilarity.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;

import java.io.Serializable;

class CustomUDFs extends UDF implements Serializable {

    public UDF1 getHash = new UDF1<String,String>(){

        @Override
        public String call(String s) throws Exception {
            return DigestUtils.md5Hex(s);
        }
    };

    static UDF2<String, String, String> columnsCompare = new UDF2<String, String, String>() {
        Double returnValue;

        public String call(String s1, String s2) throws Exception {
            // TODO: Change it by calling init function.
            JaroWinkler jw = new JaroWinkler();
            returnValue = jw.similarity(s1 == null ? "" : s1, s2 == null ? "" : s2);
            return returnValue.toString();
        }
    };

    static UDF2<String, String, String> htmlGenerator = new UDF2<String, String, String>() {



        public String call(String s1, String s2) throws Exception {
            StringBuilder htmlBuild = new StringBuilder();
            String sqlValue = (s1 != null ? s1 : "NULL");
            String hiveValue = (s2 != null ? s2 : "NULL");

            if (sqlValue.equals(hiveValue)) {
                htmlBuild.append("<td>").append(sqlValue).append("</td>");
                htmlBuild.append("<td>").append(hiveValue).append("</td>");
            } else {
                htmlBuild.append("<td bgcolor=\"LIGHTGREEN\">").append(sqlValue).append("</td>");
                htmlBuild.append("<td bgcolor=\"ORANGE\">").append(hiveValue).append("</td>");
            }
            return htmlBuild.toString();
        }
    };


}