package hadoop.mr.use.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text, Text,
        NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 1 获取 1 行数据
        String line = value.toString();
        // 2 解析日志
        boolean result = parseLog(line, context);
        // 3 日志不合法退出
        if (!result) {
            return;
        }
        // 4 日志合法就直接写出
        context.write(value, NullWritable.get());
    }

    // 2 封装解析日志的方法
    private boolean parseLog(String line, Context context) {
        // 1 截取
        String[] fields = line.split(" ");
        // 2 日志长度大于 11 的为合法
        return fields.length > 11;
    }
}