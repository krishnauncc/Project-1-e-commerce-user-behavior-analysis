package com.ecommerce.tasks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ecommerce.tasks.Task1.MostEngagedUsersCombiner;
import com.ecommerce.tasks.Task1.MostEngagedUsersMapper;
import com.ecommerce.tasks.Task1.MostEngagedUsersReducer;

public class Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 10 Engaged Users");

        job.setJarByClass(Driver.class);
        job.setMapperClass(MostEngagedUsersMapper.class);
        job.setCombinerClass(MostEngagedUsersCombiner.class); 
        job.setReducerClass(MostEngagedUsersReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
