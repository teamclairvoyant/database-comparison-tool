package com.clairvoyant.sqlserver_hive_compare;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;

import java.util.logging.Logger;

@Configuration
@ComponentScan(basePackages = "com.clairvoyant.sqlserver_hive_compare")
@PropertySource(value = { "classpath:application.properties" })
public class AppConfig {

//    private static SparkConf sparkConf = new SparkConf();
//    private static JavaSparkContext context = new JavaSparkContext(sparkConf);
    private static final Logger logger = Logger.getLogger(AppConfig.class.getName());

    @Value("${sqlServerUrl}")
    private String sqlServerUrl;

    @Value("${sqlServerUsername}")
    private String sqlServerUsername;

    @Value("${sqlServerPassword}")
    private String sqlServerPassword;

    @Value("${sqlServerDriver}")
    private String sqlServerDriver;

    @Value("${sqlServerPort}")
    private String sqlServerPort;

    @Autowired
    private Environment environment;


    @Bean
    public String sqlServerUrl() {
        return sqlServerUrl;
    }

    @Bean
    public String sqlServerPort() {
        return sqlServerPort;
    }

    @Bean
    public String sqlServerUsername() {
        return sqlServerUsername;
    }

    @Bean
    public String sqlServerPassword() {
        return sqlServerPassword;
    }

    @Bean
    public String sqlServerDriver() {
        return sqlServerDriver;
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf();
        return sparkConf;
    }

    @Bean
    public JavaSparkContext sc(){
        JavaSparkContext sc = new JavaSparkContext(sparkConf());
        return sc;
    }

    @Bean
    public HiveContext hiveContext() {

        logger.info("Creating hive context");
        HiveContext hiveContext = new HiveContext(sc());

        logger.info("Registering Custom UDF's");
        CustomUDFs customUDFs = new CustomUDFs();
        logger.info("Registering UDF :columnsCompare");
        hiveContext.udf().register("columnsCompare", customUDFs.columnsCompare, DataTypes.StringType);
        logger.info("Registering UDF :htmlGenerator");
        hiveContext.udf().register("htmlGenerator", customUDFs.htmlGenerator, DataTypes.StringType);
        logger.info("Registering UDF :toHash");
        hiveContext.udf().register("toHash", customUDFs.getHash, DataTypes.StringType);
        return hiveContext;
    }

}
