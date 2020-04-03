package hu.bme.bigdata.homework.mapreduce.flight2.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OriginCounterReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text origin, Iterable<IntWritable> counts,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable count : counts) {
			sum += count.get();
		}
		context.write(origin, new IntWritable(sum));
	}
}