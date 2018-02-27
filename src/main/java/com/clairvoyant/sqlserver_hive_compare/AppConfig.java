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

import java.util.List;
import java.util.logging.Logger;

@Configuration
@ComponentScan(basePackages = "com.clairvoyant.sqlserver_hive_compare")
@PropertySource(value = { "classpath:application.properties" })
public class AppConfig {

    private String sourceName = CommandLineArguments.getsourceName();
    private String destinationName = CommandLineArguments.getdestinationName();
    private String sourceType = CommandLineArguments.getsourceType();
    private String destinationType = CommandLineArguments.getdestinationType();

    private static final Logger logger = Logger.getLogger(AppConfig.class.getName());

    @Value("#{'${serverNames}'.split(',')}")
    private List<String> serverNames;

    @Value("${htmlStorageLocation}")
    private String htmlStorageLocation;

    @Autowired
    private Environment environment;


    @Bean
    public List serverNames() {
        return serverNames;
    }

    @Bean
    public String sqlServerDriver() {
        return environment.getProperty("driver.mssql");
    }

    @Bean
    public String postgresqlDriver() {
        return environment.getProperty("driver.postgresql");
    }

    @Bean
    public String mySqlDriver() {
        return environment.getProperty("driver.mysql");
    }

    @Bean
    public String postgresqlPort(){
        return environment.getProperty("port.postgresql");
    }

    @Bean
    public String sqlServerPort(){
        return environment.getProperty("port.mssql");
    }

    @Bean
    public String mysqlPort(){
        return environment.getProperty("port.mysql");
    }

    @Bean
    public String sourceSqlServerUrl() {
        return environment.getProperty("url." + sourceName);
    }

    @Bean
    public String sourceSqlServerUsername() {
        return environment.getProperty("username." + sourceName);
    }

    @Bean
    public String sourceSqlServerPassword() {
        return environment.getProperty("password." + sourceName);
    }

    @Bean
    public String destinationSqlServerUrl() {
        return environment.getProperty("url." + destinationName);
    }

    @Bean
    public String destinationSqlServerUsername() {
        return environment.getProperty("username." + destinationName);
    }

    @Bean
    public String destinationSqlServerPassword() {
        return environment.getProperty("password." + destinationName);
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
        return new SparkConf();
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
        logger.info("Registering UDF :columnsStringComparision");
        hiveContext.udf().register("columnsStringComparision", customUDFs.columnsStringComparision, DataTypes.StringType);
        logger.info("Registering UDF :htmlGenerator");
        hiveContext.udf().register("htmlGenerator", customUDFs.htmlGenerator, DataTypes.StringType);
        logger.info("Registering UDF :toHash");
        hiveContext.udf().register("toHash", customUDFs.getHash, DataTypes.StringType);
        logger.info("Registering UDF :parseNull");
        hiveContext.udf().register("parseNull", customUDFs.parseNull, DataTypes.StringType);
        return hiveContext;
    }

}
