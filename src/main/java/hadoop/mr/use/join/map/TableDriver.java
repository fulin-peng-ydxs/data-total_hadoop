package hadoop.mr.use.join.map;

import hadoop.mr.use.join.reduce.TableBean;
import hadoop.mr.use.join.reduce.TableReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TableDriver {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TableDriver.class);
        job.setMapperClass(MapJoinMapper.class);
        job.setReducerClass(TableReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);
        // 加载缓存数据
        job.addCacheFile(new URI("file:///Users/pengshuaifeng/IdeaProjects/data-total_hadoop/src/main/resources/join/input/pd.txt"));
//        //如果是集群运行,需要设置 HDFS 路径
//        job.addCacheFile(new URI("hdfs://hadoop102:8020/cache/pd.txt"));
        // Map 端 Join 的逻辑不需要 Reduce 阶段，设置 reduceTask 数量为 0
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, new Path("/Users/pengshuaifeng/IdeaProjects/data-total_hadoop/src/main/resources/join/input/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/pengshuaifeng/IdeaProjects/data-total_hadoop/src/main/resources/join/output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}