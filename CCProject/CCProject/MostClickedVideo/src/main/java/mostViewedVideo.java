import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.PriorityQueue;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;
import java.io.IOException;

/**
 * @ClassName: mostViewedVideo
 * @Description: Obtain the most viewed videos
 */





public class mostViewedVideo {

    public static class RankingMapper extends Mapper<LongWritable, Text,Text, FloatWritable> {
        private Text vid_name = new Text();
        private FloatWritable views = new FloatWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // split by comma
            String s = value.toString();
            String str[] = s.split(",");
            if(str.length>=5){
                vid_name.set(str[0]);

                try {
                    float parsed = Float.parseFloat(str[5]);
                    views.set(parsed);
                    context.write(vid_name,views);

                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

    }

    public static class RankingReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private PriorityQueue<Entry<String, Float>> topVideos;

        @Override
        protected void setup(Context context) {
            topVideos = new PriorityQueue<>(50, (a, b) -> Float.compare(a.getValue(), b.getValue()));
        }

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }
            Entry<String, Float> entry = new SimpleEntry<>(key.toString(), sum);
            topVideos.add(entry);
            if (topVideos.size() > 50) {
                topVideos.poll(); // Removes the video with the lowest views if more than 50 entries
            }
        }

        @Override
        protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
                throws IOException, InterruptedException {
            while (!topVideos.isEmpty()) {
                Entry<String, Float> entry = topVideos.poll();
                context.write(new Text(entry.getKey()), new FloatWritable(entry.getValue()));
            }
        }
    }



    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "mostViewedVideo");
        job.setJarByClass(mostViewedVideo.class);
        // Set Input and Output
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        // Set Mapreduce
        job.setMapperClass(RankingMapper.class);
        job.setReducerClass(RankingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setNumReduceTasks(1);
        if(job.waitForCompletion(true))
            System.exit(0);
        else System.exit(1);
    }
}
