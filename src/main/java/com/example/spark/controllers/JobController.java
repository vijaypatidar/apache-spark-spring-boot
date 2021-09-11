package com.example.spark.controllers;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequestMapping("/jobs")
@RestController
public class JobController {

    private final JavaSparkContext sc;

    public JobController(JavaSparkContext sc) {
        this.sc = sc;
    }

    @PostMapping()
    public Integer newJob() throws IOException {
        List<Integer> list = Stream.iterate(2, n -> n + 1).limit(1000).collect(Collectors.toList());
        JavaRDD<Integer> parallelize = sc.parallelize(list);
        JavaRDD<Integer> rdd1 = parallelize.map(n -> n * 2);
        JavaRDD<Integer> rdd2 = rdd1.map(n -> n * 2);
        JavaRDD<Integer> rdd3 = rdd1.map(n -> n * 5);
        JavaRDD<Integer> rdd4 = rdd1.map(n -> n * 3);
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> zip = rdd2.zip(rdd3).zip(rdd4);
        JavaRDD<Integer> rdd8 = zip.map(t -> t._1()._1()+t._1()._2()+t._2());
        JavaRDD<Integer> rdd9 = rdd8.map(n -> n * 2);
        return rdd9.reduce(Integer::sum);
    }

    @PostMapping("/warn-logs")
    public List<String> getLog(){
        String log = new File("logs/spring.log").getPath();
        return sc.textFile(log)
                .filter(line->line.contains("WARN"))
                .collect();
    }
}
