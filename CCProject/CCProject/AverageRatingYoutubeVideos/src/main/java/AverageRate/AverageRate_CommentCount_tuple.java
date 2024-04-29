package AverageRate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class AverageRate_CommentCount_tuple implements Writable{
	private int comment_count=0;
	private double video_rate=0;
	
	public int getCommentCount() {
		return comment_count;
		
	}
	public void setCommentCount(int count) {
		this.comment_count=count;
	}
	
	public double getVideoRate() {
		return video_rate;
		
	}
	public void setVideoRate(double rate) {
		this.video_rate=rate;
	}
	
	public void write(DataOutput Out) throws IOException{
		Out.writeInt(comment_count);
		Out.writeDouble(video_rate);
		
	}
	
	
	@Override
	public String toString() {
		return Integer.toString(comment_count)+" "+Double.toString(video_rate);
	}
	@Override
	public void readFields(DataInput In) throws IOException {
		// TODO Auto-generated method stub
		comment_count=In.readInt();
		video_rate=In.readDouble();
		
	}

}
