package com.alsa.flume.customSink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/10
 * @Time:10:13
 * @Description: 自定义sink
 */
public class MySink extends AbstractSink implements Configurable {

    /**
     * 创建logger对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);

    private String prefix;
    private String suffix;

    public Status process() throws EventDeliveryException {

        //声明返回值状态信息
        Status status;
        //获取当前Sink绑定的channel
        Channel ch = getChannel();
        //获取事务
        Transaction txn = ch.getTransaction();
        //声明事件
        Event event;
        //开启事务
        txn.begin();
        //读取channel中的事件，直到读取到事件结束循环
        while (true){
            event = ch.take();
            if (event != null){
                break;
            }
        }
        try {
            //处理事件，打印
            LOG.info(prefix+new String(event.getBody())+suffix);
            //事务提交
            txn.commit();
            status = Status.READY;
        } catch (Exception e) {
            //遇到异常，事务回滚
            txn.rollback();
            status = Status.BACKOFF;
        }finally {
            //关闭事务
            txn.close();
        }
        return status;
    }

    public void configure(Context context) {
        //读取配置文件内容，有默认值
        prefix = context.getString("prefix","hello");
        suffix = context.getString("suffix");
    }
}
