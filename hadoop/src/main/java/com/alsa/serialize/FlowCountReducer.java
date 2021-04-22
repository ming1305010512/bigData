package com.alsa.serialize;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Created with IDEA
 * @author:longming
 * @Date: 2021/4/22
 * @Time: 14:15
 * @Description:
 */
public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (FlowBean flowBean : values) {
            sumDownFlow += flowBean.getDownFlow();
            sumUpFlow += flowBean.getUpFlow();
        }
        FlowBean resultBean = new FlowBean(sumUpFlow,sumDownFlow);

        context.write(key,resultBean);
    }
}
