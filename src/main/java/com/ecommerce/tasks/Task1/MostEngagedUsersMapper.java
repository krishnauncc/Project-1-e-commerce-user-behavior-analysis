package com.ecommerce.tasks.Task1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MostEngagedUsersMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text userId = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length > 1) {
            // Assuming UserID is in the second column
            if (!fields[1].equals("UserID")){
                userId.set(fields[1]);
                context.write(userId, one);
            }
            
        }
    }
}
