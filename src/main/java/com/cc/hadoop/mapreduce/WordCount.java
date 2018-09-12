package com.cc.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * User: zhanghaichao
 * Date: 2018-09-11
 * Time: 下午8:13
 * Description: mapreduce开发WordCount应用程序
 */
public class WordCount {

    /**
     * 读取输入
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        LongWritable one = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");

            for (String word : words) {
                context.write(new Text(word),one);
            }
        }
    }

    /**
     * 归并操作
     */
    public static class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0 ;

            for (LongWritable value : values) {
                sum += value.get();
            }

            context.write(key,new LongWritable(sum));
        }
    }

    /**
     * mapreduce 作业的所有信息
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        //清理已经存在的输出路径
        Path path = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(path)){
            fileSystem.delete(path, true);
            System.out.println("output file exists, but is has deleted");
        }

        //创建工作
        Job job = Job.getInstance(configuration,"wordcount");

        //设置执行主类
        job.setJarByClass(WordCount.class);

        //设置输入文件路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置map相关
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置combiner，在map端执行一次reduce
        job.setCombinerClass(MyReduce.class);

        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
