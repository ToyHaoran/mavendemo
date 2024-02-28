package com.haoran.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {

    @Override
    public void initialize() {}

    @Override
    public Event intercept(Event event) {
        // 处理单条event
        //1、获取header和body的数据
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody(), StandardCharsets.UTF_8);

        // 2 判断首位是数字还是字母
        char c = body.charAt(0);
        if (c >= '0' && c <= '9'){
            headers.put("type","number");  // 数字
        }else if((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')){
            headers.put("type","letter");  // 字母
        }else{
            return null;  // 丢弃
        }

        // 3 返回修改后的event
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        // 处理多条event
        // events.removeIf(event -> intercept(event) == null);
        Iterator<Event> iterator = events.iterator();
        while (iterator.hasNext()){
            Event event = intercept(iterator.next());
            if (event == null) {
                iterator.remove();  // 迭代器可移除元素
            }
        }
        return events;
    }

    @Override
    public void close() {}

    public static class Builder implements Interceptor.Builder{
        // 静态内部类，负责生产MyInterceptor类的实例
        @Override  // 创建拦截器
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override  // 配置方法
        public void configure(Context context) {
        }
    }
}
