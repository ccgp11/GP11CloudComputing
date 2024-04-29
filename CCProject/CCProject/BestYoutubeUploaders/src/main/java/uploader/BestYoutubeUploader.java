package uploader;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
/**
 * @ClassName: BestYoutubeUploader
 * @Description: Obtain the best uploader in dataset
 */



public class BestYoutubeUploader {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text videoCreator = new Text();
        private static final IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] videoDetails = value.toString().split(",");
            if (videoDetails.length > 6) {
                videoCreator.set(videoDetails[1]);
            }
            context.write(videoCreator, one);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total_count = 0;
            for (IntWritable value : values) {
                total_count = total_count + value.get();
            }

            context.write(key, new IntWritable(total_count));
        }
    }

    public static class SortMap extends Mapper<LongWritable, Text, IntWritable, Text> {
        private Text uploader = new Text();
        private IntWritable count = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\t");
            uploader.set(parts[0]);
            count.set(Integer.parseInt(parts[1]));
            context.write(count, uploader);
        }
    }

    public static class SortReduce extends Reducer<IntWritable, Text, Text, IntWritable> {
        private TreeMap<Integer, Text> countMap = new TreeMap<>();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                countMap.put(key.get(), new Text(value));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Entry<Integer, Text> entry : countMap.descendingMap().entrySet()) {
                context.write(entry.getValue(), new IntWritable(entry.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "CountUploaderOccurrences");
        job1.setJarByClass(BestYoutubeUploader.class);
        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));
        boolean job1Completed = job1.waitForCompletion(true);
        if (!job1Completed) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf1, "SortUploadersByOccurrences");
        job2.setJarByClass(BestYoutubeUploader.class);
        job2.setMapperClass(SortMap.class);
        job2.setReducerClass(SortReduce.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setSortComparatorClass(IntWritable.Comparator.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/sorted"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
