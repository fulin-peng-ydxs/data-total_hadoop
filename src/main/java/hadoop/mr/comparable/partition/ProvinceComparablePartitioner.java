package hadoop.mr.comparable.partition;

import hadoop.mr.comparable.FlowBeanComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvinceComparablePartitioner extends Partitioner<FlowBeanComparable, Text> {
    @Override
    public int getPartition(FlowBeanComparable flowBean, Text text, int numPartitions) {
        //获取手机号前三位 prePhone
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);
        //定义一个分区号变量 partition,根据 prePhone 设置分区号
        int partition;
        if ("136".equals(prePhone)) {
            partition = 0;
        } else if ("137".equals(prePhone)) {
            partition = 1;
        } else if ("138".equals(prePhone)) {
            partition = 2;
        } else if ("139".equals(prePhone)) {
            partition = 3;
        } else {
            partition = 4;
        }
        //最后返回分区号 partition
        return partition;
    }
}