package com.MapReduce.RatingSummarization;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RatingSummarization {
	public static class RatingSummarizationMapper extends Mapper<Object, Text, Text, Text> {
		private Text videoID = new Text();
		private Text videoRating = new Text();
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString().split(",");
			
			videoID.set(input[0]);
			videoRating.set(input[6] + "," + input[7] + "," + input[8]);
			
			context.write(videoID, videoRating);
		}
	}
	
	public static class RatingSummarizationReducer extends Reducer<Text, Text, Text, Text> {
		private int sum = 0;
		private float averageRating = 0.0f;
		private int averageRatingCount = 0;
		private int averageCommentCount = 0;
		private float sumRating = 0.0f;
		private int sumRatingCount = 0;
		private int sumCommentCount = 0;
		private float topRating = 0.0f;
		private int topRatingCount = 0;
		private int topCommentCount = 0;
		private String topRatingID = "";
		private String topRatingCountID = "";
		private String topCommentCountID = "";
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text val : values) {
				sum += 1;
				
				String[] output = val.toString().split(",");
				float rating = Float.valueOf(output[0]).floatValue();
				int ratingCount = (int)(Float.valueOf(output[1]).floatValue());
				int commentCount = (int)(Float.valueOf(output[2]).floatValue());
				
				sumRating += rating;
				sumRatingCount += ratingCount;
				sumCommentCount += commentCount;
				
				if(rating > topRating) {
					topRating = rating;
					topRatingID = key.toString();
				}
				if(ratingCount > topRatingCount) {
					topRatingCount = ratingCount;
					topRatingCountID = key.toString();
				}
				if(commentCount > topCommentCount) {
					topCommentCount = commentCount;
					topCommentCountID = key.toString();
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			averageRating = sumRating / sum;
			averageRatingCount = sumRatingCount / sum;
			averageCommentCount = sumCommentCount / sum;
			
			String result = "";
			result += "The average rating is: " + String.format("%.2f", averageRating) + "\n";
			result += "The average number of rating is: " + Integer.toString(averageRatingCount) + "\n";
			result += "The average number of comment is: " + Integer.toString(averageCommentCount) + "\n";
			result += "The video with highest rating is: ID: " + topRatingID + " with a rating of: " + Float.toString(topRating) + "\n";
			result += "The video with highest number of rating is: ID: " + topRatingCountID + " with a number of rating of: " + Integer.toString(topRatingCount) + "\n";
			result += "The video with highest number of comment is: ID: " + topCommentCountID + " with a number of comment of: " + Integer.toString(topCommentCount) + "\n";
			context.write(new Text("Result: "), new Text(result));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Rating Summarization");
		job.setJarByClass(RatingSummarization.class);
		job.setMapperClass(RatingSummarizationMapper.class);
		job.setReducerClass(RatingSummarizationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
