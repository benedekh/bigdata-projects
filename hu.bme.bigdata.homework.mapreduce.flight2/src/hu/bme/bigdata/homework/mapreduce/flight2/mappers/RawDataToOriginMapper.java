package hu.bme.bigdata.homework.mapreduce.flight2.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RawDataToOriginMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		String[] splitted = lineText.toString().split(",");
		String cancelled = splitted[21];
		String origin = splitted[16];

		if ("0".equals(cancelled)) {
			Text originText = new Text(origin);
			context.write(originText, one);
		}
	}
}
