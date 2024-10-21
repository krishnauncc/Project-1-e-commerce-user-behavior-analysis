package com.ecommerce.tasks.Task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MostEngagedUsersReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final static int TOP_N = 10;
    private final PriorityQueue<Map.Entry<Text, Integer>> topUsers = new PriorityQueue<>(
            (a, b) -> Integer.compare(a.getValue(), b.getValue())); // Min-heap

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalInteractions = 0;
        for (IntWritable val : values) {
            totalInteractions += val.get();
        }
        topUsers.offer(new HashMap.SimpleEntry<>(new Text(key), totalInteractions)); // Directly store entry
        if (topUsers.size() > TOP_N) {
            topUsers.poll(); // Maintain top N users
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Use a list to store the entries for sorting
        List<Map.Entry<Text, Integer>> sortedEntries = new ArrayList<>(topUsers.size());
        sortedEntries.addAll(topUsers);

        // Sort entries in descending order
        sortedEntries.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

        // Output the top N users
        for (Map.Entry<Text, Integer> entry : sortedEntries) {
            context.write(entry.getKey(), new IntWritable(entry.getValue()));
        }
    }
}
