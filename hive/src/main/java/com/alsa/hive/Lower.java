package com.alsa.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Created with IDEA
 * @author:longming
 * @Date:2020/1/2
 * @Time:14:09
 * @Description:
 */
public class Lower extends UDF {

    public String evaluate(final String s){
        if (s==null){
            return null;
        }
        return s.toLowerCase();
    }
}
