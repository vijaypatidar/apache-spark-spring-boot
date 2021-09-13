package com.example.spark.schedulers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MyTaskScheduler {
    Logger logger = LoggerFactory.getLogger(MyTaskScheduler.class);
    @Scheduled(fixedDelay = 2000)
    public void task(){

    }
}
