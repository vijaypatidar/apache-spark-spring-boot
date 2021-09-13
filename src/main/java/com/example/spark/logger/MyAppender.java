package com.example.spark.logger;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.env.Environment;
import org.springframework.lang.NonNullApi;
import org.springframework.stereotype.Component;

@Component
public class MyAppender extends AppenderBase<ILoggingEvent> implements ApplicationContextAware {
    @Autowired
    Environment environment;
    @Override
    protected void append(ILoggingEvent event) {
        System.out.println(environment);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

    }
}
