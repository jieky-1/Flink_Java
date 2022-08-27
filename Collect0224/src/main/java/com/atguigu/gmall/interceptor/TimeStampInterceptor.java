package com.atguigu.gmall.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TimeStampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1. 取出body
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        //2. 将json字符串解析成对象
        JSONObject jsonObject = JSON.parseObject(body);
        //3. 从对象中获取ts
        String ts = jsonObject.getString("ts");
        //4.将ts的值设置到event的header中.
        event.getHeaders().put("timestamp",ts);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
