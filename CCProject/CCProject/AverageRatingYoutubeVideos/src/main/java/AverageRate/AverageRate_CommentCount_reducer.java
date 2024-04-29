package AverageRate;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import AverageRate.AverageRate_CommentCount_tuple;

public class AverageRate_CommentCount_reducer extends Reducer<Text, AverageRate_CommentCount_tuple, Text, AverageRate_CommentCount_tuple> {
	
	private AverageRate_CommentCount_tuple result=new AverageRate_CommentCount_tuple();
	
	protected void reduce(Text key, Iterable<AverageRate_CommentCount_tuple> values, Context context) throws IOException, InterruptedException{
		
		float sum=0;
		int count=0;
		
		for(AverageRate_CommentCount_tuple val:values) {
			sum+= val.getCommentCount()*val.getVideoRate();
			count+= val.getCommentCount();
		}
		result.setVideoRate(sum/count);
		context.write(key, result);
			
	}
}
