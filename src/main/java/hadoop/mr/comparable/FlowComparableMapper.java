package hadoop.mr.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowComparableMapper extends Mapper<LongWritable, Text, FlowBeanComparable,Text> {
    private final Text outK = new Text();
    private final FlowBeanComparable outV = new FlowBeanComparable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //1 获取一行数据,转成字符串
        String line = value.toString();
        //2 切割数据
        String[] split = line.split("\t");
        //3 抓取我们需要的数据:手机号,上行流量,下行流量
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];
        //4 封装 outK outV
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();
        //5 写出 outK outV
        context.write(outV,outK);
    }
}