package solutions.assignment2;

import java.io.*;
import java.text.ParseException;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.time.Duration;
import org.apache.hadoop.io.LongWritable;
import java.util.Date;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.util.Map;
import org.apache.hadoop.io.Text;
import java.text.SimpleDateFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Calendar;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Reducer;

import examples.MapRedFileUtils;

public class MapRedSolution2
{
    public static class TaxiMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text key_hour = new Text();

        public static String convertToAmPmFormat(int hour) {
            return hour == 0 ? "12am" : 
                hour == 12 ? "12pm" : 
                hour > 12 ? (hour - 12) + "pm" : 
                hour + "am";
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith ("VendorID")) {
                return;
            }
            String[] lines = line.split(",");

            String start_time = lines[1];
            String end_time = lines[2];
            // System.out.println("[DEBUG]" + " start_time: " + start_time + " end_time: " + end_time);
            DateTimeFormatter formatter_object = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime start = LocalDateTime.parse(start_time, formatter_object);
            LocalDateTime end = LocalDateTime.parse(end_time, formatter_object);

            start = (start.minusMinutes(start.getMinute())).minusSeconds(start.getSecond());
            end = (end.minusMinutes(end.getMinute())).minusSeconds(end.getSecond());

            if (start.isAfter(end)){
                // System.out.println("[DEBUG] start after end " + " start_time: " + start_time + " end_time: " + end_time);
                return;
            }
            
            if (start.compareTo(end) <= 0) {
                // System.out.println("[DEBUG] start before end " + " start_time: " + start_time + " end_time: " + end_time);
                while(start.compareTo(end) <= 0) {
                    key_hour.set(convertToAmPmFormat(start.getHour()));
                    context.write(key_hour, one);
                // System.out.println("[DEBUG] Key: " + key_hour);
                    start = start.plusHours(1);
                }
            }
        }
    }

    public static class TaxiReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable res = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            res.set(sum);
            context.write(key, res);
        }
    }
    
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution2 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #2");
        
        job.setMapperClass(TaxiMapper.class);
        job.setReducerClass(TaxiReducer.class);
        job.setCombinerClass(TaxiReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        int exitCode = job.waitForCompletion(true) ? 0 : 1; 

        FileInputStream fileInputStream = new FileInputStream(new File(otherArgs[1]+"/part-r-00000"));
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fileInputStream);
        fileInputStream.close();
        
        String[] validMd5Sums = {"03357cb042c12da46dd5f0217509adc8", "ad6697014eba5670f6fc79fbac73cf83", "07f6514a2f48cff8e12fdbc533bc0fe5", 
            "e3c247d186e3f7d7ba5bab626a8474d7", "fce860313d4924130b626806fa9a3826", "cc56d08d719a1401ad2731898c6b82dd", 
            "6cd1ad65c5fd8e54ed83ea59320731e9", "59737bd718c9f38be5354304f5a36466", "7d35ce45afd621e46840627a79f87dac",
            "6337ae51f8127efc8a11de5537465c14", "f736f9bf2f4021459982e63ef0b273dd","b8eef00e22303183df69b266d5f5db96"};
        
        for (String validMd5 : validMd5Sums) 
        {
            if (validMd5.contentEquals(md5))
            {
                System.out.println("The result looks good :-)");
                System.exit(exitCode);
            }
        }
        System.out.println("The result does not look like what we expected :-(");
        System.exit(exitCode);
    }
}
