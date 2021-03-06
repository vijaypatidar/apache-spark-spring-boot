package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SparkApplication {

    public static void main(String[] args) {
        SpringApplication.run(SparkApplication.class, args);
    }

    @Bean
    public JavaSparkContext getJavaSparkContext() {
        SparkConf conf = new SparkConf().setMaster("local[*]")
                .setAppName("Spark Demo");
        return new JavaSparkContext(conf);
    }
}
