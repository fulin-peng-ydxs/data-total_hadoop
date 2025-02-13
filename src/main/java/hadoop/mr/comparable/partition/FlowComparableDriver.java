package hadoop.mr.comparable.partition;

import hadoop.mr.comparable.FlowBeanComparable;
import hadoop.mr.comparable.FlowComparableMapper;
import hadoop.mr.comparable.FlowComparableReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowComparableDriver {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        //1 获取 job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2 关联本 Driver 类
        job.setJarByClass(FlowComparableDriver.class);
        //3 关联 Mapper 和 Reducer
        job.setMapperClass(FlowComparableMapper.class);
        job.setReducerClass(FlowComparableReducer.class);
        //4 设置 Map 端输出 KV 类型
        job.setMapOutputKeyClass(FlowBeanComparable.class);
        job.setMapOutputValueClass(Text.class);
        //5 设置程序最终输出的 KV 类型
        job.setOutputKeyClass(FlowBeanComparable.class);
        job.setOutputValueClass(Text.class);

        //8 指定自定义分区器
        job.setPartitionerClass(ProvinceComparablePartitioner.class);
        //9 同时指定相应数量的 ReduceTask
        job.setNumReduceTasks(5);

        //6 设置程序的输入输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/pengshuaifeng/IdeaProjects/data-total_hadoop/src/main/resources/flowphone/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/pengshuaifeng/IdeaProjects/data-total_hadoop/src/main/resources/flowphone/output/comparable/partition"));
        //7 提交 Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}