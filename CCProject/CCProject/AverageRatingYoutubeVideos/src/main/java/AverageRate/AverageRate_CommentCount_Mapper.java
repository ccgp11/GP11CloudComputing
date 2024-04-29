package AverageRate;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import AverageRate.AverageRate_CommentCount_tuple;

public class AverageRate_CommentCount_Mapper extends Mapper<Object, Text, Text, AverageRate_CommentCount_tuple>{
	
	private Text video_name=new Text();
	private float video_rate;
	private AverageRate_CommentCount_tuple outTuple=new AverageRate_CommentCount_tuple();
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] fields=value.toString().split(",");
		String videoIdD= (fields[0]);
		try {
			if(fields.length>6) {
				this.video_rate=Float.parseFloat(fields[6]);
			}
			else {
				this.video_rate=0;
			}
			video_name.set(videoIdD);
			outTuple.setCommentCount(1);
			outTuple.setVideoRate(this.video_rate);
			context.write(video_name, outTuple);
		}catch(Exception e){
			e.printStackTrace();
			
		}
		
		
	}

}
