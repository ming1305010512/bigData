package com.alsa.flume.customSource;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/10
 * @Time:9:57
 * @Description: 自定义source
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

    /**
     * 定义配置文件将来要读取的字段
     */
    private Long delay;
    private String field;


    public Status process() throws EventDeliveryException {
        //创建事件头信息
        HashMap<String,String> headerMap = new HashMap<String, String>();
        //创建事件
        SimpleEvent event = new SimpleEvent();
        //循环封装事件
        for (int i=0;i<5;i++){
            //给事件设置头信息
            event.setHeaders(headerMap);
            //给事件设置内容
            event.setBody((field+i).getBytes());
            //将事件写入channel
            getChannelProcessor().processEvent(event);
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return Status.READY;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    /**
     * 初始化配置信息
     * @param context
     */
    public void configure(Context context) {
        delay = context.getLong("delay");
        field = context.getString("field","Hello!");
    }
}
