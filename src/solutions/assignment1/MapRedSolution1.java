package solutions.assignment1;

import java.io.File;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.LongWritable;
import examples.MapRedFileUtils;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MapRedSolution1
{
        private final static IntWritable one = new IntWritable(1);

        public static class UrlMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] urlLine = line.split(" ");
            if(urlLine.length > 1){
                String url = urlLine[6];
                if (!url.startsWith("http://")) {
                    url = "http://localhost" + url;
                }
                word.set(url);
                context.write(word, one);
            }
        }
    }
    public static class MapRecords extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final Text url = new Text();
        final static String host = "http://localhost";
        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] splittedData = line.split(" ");
            if(splittedData.length > 1)
            {
                String webUrl = splittedData[6];
                // Prepend Ihost if the url does not starts with http://
                if (!webUrl.startsWith("http://")) 
                webUrl = "http://localhost" + webUrl;
                url.set(webUrl);
                context.write(url, one);
            }
        }
    }
    public static class UrlReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        // private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // result.set(sum);
            // context.write(key, result);
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution1 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #1");
        
        // job.setJarByClass(MapRedSolution1.class);
        job.setMapperClass(UrlMapper.class);
        // job.setMapperClass(MapRecords.class);
        job.setCombinerClass(UrlReducer.class);
        job.setReducerClass(UrlReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        int exitCode = job.waitForCompletion(true) ? 0 : 1; 
        
        FileInputStream fileInputStream = new FileInputStream(new File(otherArgs[1]+"/part-r-00000"));
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fileInputStream);
        fileInputStream.close();
        
        String[] validMd5Sums = {"af174f7148177b6ad68b7092bc9789f9","878ed82c762143aaf939797f6b02e781"};
        
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
