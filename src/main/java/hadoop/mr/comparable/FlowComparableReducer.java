package hadoop.mr.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowComparableReducer extends Reducer<FlowBeanComparable, Text, Text, FlowBeanComparable> {

    @Override
    protected void reduce(FlowBeanComparable key, Iterable<Text> values, Context
            context) throws IOException, InterruptedException {
       //遍历 values 集合,循环写出,避免总流量相同的情况 (相同key：compareTo方法返回0)
        for (Text value : values) {
            //调换 KV 位置,反向写出
            //这里输出后key对象的里面的值会发生改变，即这个key在”迭代器“里会被重新写入新的值，如果是相同key的话
            context.write(value,key);
        }
    }
}