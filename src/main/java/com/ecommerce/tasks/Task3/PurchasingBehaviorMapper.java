package com.ecommerce.tasks.Task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PurchasingBehaviorMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final IntWritable one = new IntWritable(1);
    private final Text outputKey = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        
        if (fields.length == 7) { // Ensure the row has expected number of columns
            String productCategory = fields[2];
            String transactionTimestamp = fields[6];

            try {
                Date date = dateFormat.parse(transactionTimestamp);
                SimpleDateFormat hourFormat = new SimpleDateFormat("HH");
                String hour = hourFormat.format(date);
                
                outputKey.set(hour + "," + productCategory); // Combine hour and product category as key
                context.write(outputKey, one);

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}
