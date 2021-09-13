package com.example.spark.controllers;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequestMapping("/jobs")
@RestController
public class JobController {

    private final JavaSparkContext sc;
    private final Logger logger = LoggerFactory.getLogger(JobController.class);

    public JobController(JavaSparkContext sc) {
        this.sc = sc;
    }

    @PostMapping()
    public Integer newJob() {
        Map<String, Object> log = new HashMap<>();
        log.put("START_TIME", System.currentTimeMillis());
        List<Integer> list = Stream.iterate(2, n -> n + 1).limit(1000).collect(Collectors.toList());
        JavaRDD<Integer> parallelize = sc.parallelize(list);
        JavaRDD<Integer> rdd1 = parallelize.map(n -> n * 2);
        JavaRDD<Integer> rdd2 = rdd1.map(n -> n * 2);
        JavaRDD<Integer> rdd3 = rdd1.map(n -> n * 5);
        JavaRDD<Integer> rdd4 = rdd1.map(n -> n * 3);
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> zip = rdd2.zip(rdd3).zip(rdd4);
        JavaRDD<Integer> rdd8 = zip.map(t -> t._1()._1() + t._1()._2() + t._2());
        JavaRDD<Integer> rdd9 = rdd8.map(n -> n * 2);
        log.put("END_TIME", System.currentTimeMillis());
        log.put("MESSAGE", "New Job triggered");
        logger.info(log.toString());
        return rdd9.reduce(Integer::sum);
    }

    @PostMapping("/warn-logs")
    public List<String> getLog() {
        logger.info("getLog " + new Date());
        String log = new File("logs/spring.log").getPath();
        return sc.textFile(log)
                .filter(line -> line.matches(".*(WARN|INFO).*"))
                .collect();
    }

    @PostMapping("/count")
    public List<AbstractMap.SimpleEntry<String, Integer>> wordCount() {
        Map<String, Object> log = new HashMap<>();
        log.put("START_TIME", System.currentTimeMillis());
        logger.info("getLog " + new Date());
        String logFile = new File("logs/spring.log").getPath();
        JavaRDD<String> lines = sc.textFile(logFile);
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).filter(word -> !word.matches(".*[0-9].*"));
        JavaPairRDD<String, Integer> pair = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordPair = pair.reduceByKey(Integer::sum);
        JavaRDD<AbstractMap.SimpleEntry<String, Integer>> map = wordPair.map(tuple -> new AbstractMap.SimpleEntry<>(tuple._1(), tuple._2()));
        JavaRDD<AbstractMap.SimpleEntry<String, Integer>> sorted = map.sortBy(AbstractMap.SimpleEntry::getValue, true, 12);
        log.put("END_TIME", System.currentTimeMillis());
        log.put("MESSAGE", "New Job triggered");
        logger.info(log.toString());
        return sorted.collect();
    }

}
