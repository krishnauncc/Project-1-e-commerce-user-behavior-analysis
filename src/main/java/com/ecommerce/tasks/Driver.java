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
import com.ecommerce.tasks.Task3.PurchasingBehaviorMapper;
import com.ecommerce.tasks.Task3.PurchasingBehaviorReducer;

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

        // Wait for Task 1 to complete before starting Task 3
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        // Task 3
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "User Purchasing Behavior");

        job3.setJarByClass(Driver.class);
        job3.setMapperClass(PurchasingBehaviorMapper.class);
        job3.setReducerClass(PurchasingBehaviorReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job3, new Path(args[3])); // Input path for Task 3 (e.g., transactions.csv)
        FileOutputFormat.setOutputPath(job3, new Path(args[4])); // Output path for Task 3

        System.exit(job3.waitForCompletion(true) ? 0 : 1);

    }
}
