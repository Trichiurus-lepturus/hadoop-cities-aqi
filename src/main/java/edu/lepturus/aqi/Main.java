package edu.lepturus.aqi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Main
 *
 * @author T.lepturus
 * @version 1.0
 */
public final class Main extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new edu.lepturus.aqi.Main(), args);
        System.exit(res);
    }
    public int run(String[] args) throws Exception {
        // 创建配置对象
        Configuration conf = this.getConf();
        // 创建作业实例
        Job job = new Job(conf, "city aqi");
        // 设置作业启动类
        job.setJarByClass(edu.lepturus.aqi.Main.class);

        // 设置Mapper类
        job.setMapperClass(AQIMapper.class);
        // 设置map任务输出键类型
        job.setMapOutputKeyClass(AQIKey.class);
        // 设置map任务输出值类型
        job.setMapOutputValueClass(AQIIndexes.class);

        //设置Reducer类
        job.setReducerClass(AQIReducer.class);
        // 设置Reducer任务输出键类型
        job.setOutputKeyClass(Text.class);
        // 设置Reducer任务输出值类型
        job.setOutputValueClass(AQI.class);

        //设置输入文件操作类
        job.setInputFormatClass(TextInputFormat.class);
        //设置输出文件操作类
        job.setOutputFormatClass(TextOutputFormat.class);

        // 创建输入目录
        FileInputFormat.setInputPaths(job, new Path("input/*"));
        // 创建输出目录
        FileOutputFormat.setOutputPath(job, new Path("output/"));

        //指定自定义分区器
        job.setPartitionerClass(AQIPartitioner.class);

        //指定相应数量的ReduceTask
        job.setNumReduceTasks(1);
        //提交任务
        job.submit();
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
