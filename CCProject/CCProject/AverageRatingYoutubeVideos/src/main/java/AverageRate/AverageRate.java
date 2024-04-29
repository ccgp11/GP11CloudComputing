package AverageRate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import AverageRate.AverageRate_CommentCount_tuple;
import AverageRate.AverageRate_CommentCcount_combiner;
import AverageRate.AverageRate_CommentCount_reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class AverageRate {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AverageRate");
		
		job.setJarByClass(AverageRate.class);
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AverageRate_CommentCount_Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AverageRate_CommentCount_tuple.class);
		job.setCombinerClass(AverageRate_CommentCcount_combiner.class);
        job.setReducerClass(AverageRate_CommentCount_reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AverageRate_CommentCount_tuple.class);
        System.exit(job.waitForCompletion(true)?0:1);
	}

}
