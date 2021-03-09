package edu.gatech.cse6242;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TRIP {
/* TODO: Update variable below with your gtid */
  // final String gtid = "********";

  public static class TripMapper
    extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable offset, Text lineText, Context context)
            throws IOException, InterruptedException {

                String line = lineText.toString();
                String pickUpId = line.split(",")[0];
                String distance = line.split(",")[2];
                String fare = line.split(",")[3];

                if (Double.valueOf(distance) > 0 && Double.valueOf(fare) > 0){
                    context.write(new Text(pickUpId), new Text(fare));
                }
            }
    }

  public static class TripReducer
    extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text pickUpId, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

                int tally = 0;
                double sum = 0;
                for (Text value : values) {
                    tally ++;
                    sum += Double.valueOf(value.toString());
                }

                String tallyString = new DecimalFormat("####").format(tally);
                String sumString = new DecimalFormat("#,###.00").format(sum);

                context.write(pickUpId, new Text(tallyString+","+sumString));
            } 
    }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TRIP");
    job.setJarByClass(TRIP.class); 
    job.setMapperClass(TripMapper.class);
    // job.setCombinerClass(TripReducer.class);
    job.setReducerClass(TripReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
