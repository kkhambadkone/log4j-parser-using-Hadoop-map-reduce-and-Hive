/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class ParseLogFile implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Parse Log File";

        job.setJobName(jobName);
        
        job.setJarByClass(ParseLogFile.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ParseLogFile.ParseLogFileMapper.class);
        //job.setReducerClass(ParseLogFile.ParseLogFileReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //job.setOutputKeyClass(NullWritable.class);
        //job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ParseLogFile(), args);
        System.exit(exitCode);
    }

    public void setConf(Configuration conf) {
       this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }
 
    public static class ParseLogFileMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text keyHolder = new Text();
        private Text valueHolder = new Text();
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(" ");
           //String items = value.toString();
           if (items.length > 3) {

              keyHolder.set(items[0]); //emit url as key and the integer 1 as value

              /*
               * items[0]:   2011-11-03
               * items[1]:   18:30:06,324
               * items[2]:   INFO
               * items[3]:   org.apache.hadoop.hdfs.server.datanode.DataBlockScanner:
               * items[4]:   Verification succeeded for blk_364480670131752448_2380
               */

              valueHolder.set(value.toString());
              //context.write (keyHolder,new Text(items.length+"   "+items[2]));
              //context.write (keyHolder,new Text(value.toString()));

              if (items[2].equals("INFO") ||
                  items[2].equals("TRACE") ||
                  items[2].equals("DEBUG") ||
                  items[2].equals("WARN") ||
                  items[2].equals("ERROR") ||
                  items[2].equals("FATAL")) 
                     context.write(keyHolder, valueHolder);
              }
        }        
    }   
    
    public static class ParseLogFileReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
        private Text valueHolder = new Text();
        private Map<String, Integer> totalCount = new HashMap<String, Integer>();
        
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
            int count = 0;
			int totalTime = 0;
            for (IntWritable value : values){
				++count;
                totalTime += value.get();
            } 
			int avTime = totalTime / count;

            valueHolder.set(key.toString() + "," + avTime);
            context.write(NullWritable.get(), valueHolder);
        }
    }    
    
}
