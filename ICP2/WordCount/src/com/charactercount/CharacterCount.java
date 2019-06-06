package com.charactercount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class CharacterCount {
    // Map function
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable();
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Splitting the line on spaces
            String line = value.toString();
            String  tokenizer[] = line.split("");
            for(String SingleChar : tokenizer)
            {
                Text charKey = new Text(SingleChar);
                context.write(charKey,one);
            }

        }
    }

    // Reduce function
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static void main(String[] args)  throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "WC");
        job.setJarByClass(CharacterCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}