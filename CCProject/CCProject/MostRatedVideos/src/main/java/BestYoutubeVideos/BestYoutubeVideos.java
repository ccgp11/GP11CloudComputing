package BestYoutubeVideos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;

/**
 * @ClassName: BestYoutubeVideos
 * @Description: This class ranks YouTube videos by their ratings using MapReduce
 */
public class BestYoutubeVideos {

    public static class MapStage extends Mapper<Object, Text, Text, FloatWritable> {
        private Text videoId = new Text();
        private FloatWritable rating = new FloatWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            videoId.set(data[0]);
            try {
                if (data.length > 6) {
                    rating.set(Float.parseFloat(data[7]));
                }
                context.write(videoId, rating);
            } catch (NumberFormatException e) {
                System.err.println("Invalid rating: " + data[7]);
            }
        }
    }

    public static class ReduceStage extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private Map<Text, FloatWritable> videoRatings = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float total = 0;
            int count = 0;
            for (FloatWritable value : values) {
                total += value.get();
                count++;
            }
            videoRatings.put(new Text(key), new FloatWritable(total / count));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, FloatWritable> sortedRatings = sortRatings(videoRatings);
            int limit = 50;
            int numWritten = 0;
            for (Map.Entry<Text, FloatWritable> entry : sortedRatings.entrySet()) {
                if (numWritten++ == limit) break;
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }

    private static Map<Text, FloatWritable> sortRatings(Map<Text, FloatWritable> ratings) {
        List<Map.Entry<Text, FloatWritable>> list = new ArrayList<>(ratings.entrySet());
        list.sort(Map.Entry.comparingByValue(Collections.reverseOrder()));

        Map<Text, FloatWritable> result = new LinkedHashMap<>();
        for (Map.Entry<Text, FloatWritable> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "BestYoutubeVideos");
        job.setJarByClass(BestYoutubeVideos.class);
        job.setMapperClass(MapStage.class);
        job.setReducerClass(ReduceStage.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
