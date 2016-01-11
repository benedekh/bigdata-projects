package hu.bme.bigdata.homework.mapreduce.flight2.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RowToPairMapper extends
		Mapper<LongWritable, Text, Text, MapWritable> {

	public static final Text occurenceKey = new Text("occurence");
	public static final Text originKey = new Text("origin");

	private static final Text max = new Text("Max");

	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		String[] splitted = lineText.toString().split("	");

		String origin = splitted[0];
		Integer occurence = Integer.parseInt(splitted[1]);

		MapWritable occurenceMap = new MapWritable();
		occurenceMap.put(originKey, new Text(origin));
		occurenceMap.put(occurenceKey, new IntWritable(occurence));

		context.write(max, occurenceMap);
	}
}