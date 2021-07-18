package com.zoey.mapreducer;

import com.zoey.bean.FlowBean;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author yining.liu@woqutech.com
 * @Date 2021/7/18 4:10 下午
 * @Description:
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        phone.set(split[1]);
        String upFlow = split[split.length - 3];
        String downFlow = split[split.length - 2];
        context.write(phone, new FlowBean(Long.parseLong(upFlow), Long.parseLong(downFlow)));
    }
}
