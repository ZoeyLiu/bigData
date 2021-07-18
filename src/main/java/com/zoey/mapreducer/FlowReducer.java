package com.zoey.mapreducer;

import com.zoey.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author yining.liu@woqutech.com
 * @Date 2021/7/18 4:35 下午
 * @Description:
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Long upFlowSum = 0L;
        Long downFlowSum = 0L;

        for (FlowBean flowBean : values) {
            upFlowSum += flowBean.getUpFlow();
            downFlowSum += flowBean.getDownFlow();
        }
        context.write(key, new FlowBean(upFlowSum, downFlowSum));
    }
}
