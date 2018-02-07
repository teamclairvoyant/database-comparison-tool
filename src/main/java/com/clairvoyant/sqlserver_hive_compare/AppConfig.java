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

    private static final Logger logger = Logger.getLogger(AppConfig.class.getName());

    @Value("${sourceSqlServerUrl}")
    private String sourceSqlServerUrl;

    @Value("${sourceSqlServerUsername}")
    private String sourceSqlServerUsername;

    @Value("${sourceSqlServerPassword}")
    private String sourceSqlServerPassword;

    @Value("${sourceSqlServerDriver}")
    private String sourceSqlServerDriver;

    @Value("${sourceSqlServerPort}")
    private String sourceSqlServerPort;

    @Value("${destinationSqlServerUrl}")
    private String destinationSqlServerUrl;

    @Value("${destinationSqlServerUsername}")
    private String destinationSqlServerUsername;

    @Value("${destinationSqlServerPassword}")
    private String destinationSqlServerPassword;

    @Value("${destinationSqlServerDriver}")
    private String destinationSqlServerDriver;

    @Value("${destinationSqlServerPort}")
    private String destinationSqlServerPort;

    @Value("${htmlStorageLocation}")
    private String htmlStorageLocation;

    @Autowired
    private Environment environment;


    @Bean
    public String sourceSqlServerUrl() {
        return sourceSqlServerUrl;
    }

    @Bean
    public String sourceSqlServerPort() {
        return sourceSqlServerPort;
    }

    @Bean
    public String sourceSqlServerUsername() {
        return sourceSqlServerUsername;
    }

    @Bean
    public String sourceSqlServerPassword() {
        return sourceSqlServerPassword;
    }

    @Bean
    public String sourceSqlServerDriver() {
        return sourceSqlServerDriver;
    }

    @Bean
    public String destinationSqlServerUrl() {
        return destinationSqlServerUrl;
    }

    @Bean
    public String destinationSqlServerPort() {
        return destinationSqlServerPort;
    }

    @Bean
    public String destinationSqlServerUsername() {
        return destinationSqlServerUsername;
    }

    @Bean
    public String destinationSqlServerPassword() {
        return destinationSqlServerPassword;
    }

    @Bean
    public String destinationSqlServerDriver() {
        return destinationSqlServerDriver;
    }

    @Bean
    public String htmlStorageLocation() {
        return htmlStorageLocation;
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
        logger.info("Registering UDF :parseNull");
        hiveContext.udf().register("parseNull", customUDFs.parseNull, DataTypes.StringType);
        return hiveContext;
    }

}
