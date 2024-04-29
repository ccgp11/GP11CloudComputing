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
 * @ClassName: BestYoutuber
 * @Description: MapReduce program that obtain the best YouTube video ranking
 * @Date: 2024/4/24 23:07
 */
public class BestYoutubeVideos {

    public static class Map extends Mapper<Object, Text, Text, FloatWritable> {
        private FloatWritable video_rating = new FloatWritable();
        private Text vid_id = new Text();
        public void map(Object key, Text value, Mapper.Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            vid_id = new Text(fields[0]);
            try {
                if(fields.length > 6) {
                    video_rating = new FloatWritable(Float.parseFloat(fields[7]));
                }
                context.write(vid_id, video_rating);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private java.util.Map<Text, FloatWritable> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            countMap.put(new Text(key), new FloatWritable(sum / count));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            java.util.Map<Text, FloatWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 50) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }

    private static <K extends Comparable, V extends Comparable> java.util.Map<K, V> sortByValues(java.util.Map<K, V> map) {
        List<java.util.Map.Entry<K, V>> entries = new LinkedList<java.util.Map.Entry<K, V>>(map.entrySet());
        Collections.sort(entries, new Comparator<java.util.Map.Entry<K, V>>() {
            @Override
            public int compare(java.util.Map.Entry<K, V> o1, java.util.Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        java.util.Map<K, V> sorted = new LinkedHashMap<K, V>();
        for (java.util.Map.Entry<K, V> entry : entries) {
            sorted.put(entry.getKey(), entry.getValue());
        }
        return sorted;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BestYoutubeVideos");
        job.setJarByClass(BestYoutubeVideos.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
