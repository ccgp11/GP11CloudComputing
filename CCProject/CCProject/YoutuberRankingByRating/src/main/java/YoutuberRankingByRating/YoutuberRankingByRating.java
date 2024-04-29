package YoutuberRankingByRating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparator;
import java.io.IOException;

/**
 * @ClassName: YoutuberRankingByRating
 * @Description: The MapReduce program that gets the ranking of uploaders by avg video rating
 */


public class YoutuberRankingByRating {
    public static class AvgRatingMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private Text uploader = new Text();
        private FloatWritable rate = new FloatWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 6) {
                uploader.set(fields[1]); // uploader
                rate.set(Float.parseFloat(fields[6])); // rate
                context.write(uploader, rate);
            }
        }
    }

    public static class AvgRatingReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static class SortMapper extends Mapper<Object, Text, FloatWritable, Text> {
        private FloatWritable averageRate = new FloatWritable();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\\t");
            averageRate.set(Float.parseFloat(row[1]));
            context.write(averageRate, new Text(row[0]));
        }
    }

    public static class SortReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
        // private int count = 20; // Outputs only the top 20 uploaders

        protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(val, key);
            }
        }
    }


    public static class DescendingComparator extends WritableComparator {
        protected DescendingComparator() {
            super(FloatWritable.class, true);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            Float v1 = Float.intBitsToFloat(readInt(b1, s1));
            Float v2 = Float.intBitsToFloat(readInt(b2, s2));
            return v1.compareTo(v2) * (-1);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Calculate Average Rating");
        job1.setJarByClass(YoutuberRankingByRating.class);
        job1.setMapperClass(AvgRatingMapper.class);
        job1.setCombinerClass(AvgRatingReducer.class);
        job1.setReducerClass(AvgRatingReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));
        boolean complete = job1.waitForCompletion(true);

        if (complete) {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Rank Uploaders by Average Rating");
            job2.setJarByClass(YoutuberRankingByRating.class);
            job2.setMapperClass(SortMapper.class);
            job2.setReducerClass(SortReducer.class);
            job2.setSortComparatorClass(DescendingComparator.class);
            job2.setNumReduceTasks(1);
            job2.setOutputKeyClass(FloatWritable.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[1] + "/temp"));
            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
