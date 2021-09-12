package com.example.spark;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class MyAppender extends AppenderSkeleton {

    @Override
    protected void append(LoggingEvent event) {
        if(event.getMessage() instanceof HashMap){
            Map<String,Object> map = (HashMap<String, Object>) event.getMessage();
            System.out.println("========= "+map.toString());
        }
        System.out.println(event.getMessage() + event.getProperties().toString());
    }

    public void close() {
    }

    public boolean requiresLayout() {
        return false;
    }

}
