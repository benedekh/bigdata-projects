package hu.bme.bigdata.homework.mapreduce.flight2;

import hu.bme.bigdata.homework.mapreduce.flight2.mappers.RawDataToOriginMapper;
import hu.bme.bigdata.homework.mapreduce.flight2.mappers.RowToPairMapper;
import hu.bme.bigdata.homework.mapreduce.flight2.reducers.MaxOccurenceReducer;
import hu.bme.bigdata.homework.mapreduce.flight2.reducers.OriginCounterReducer;

import java.io.File;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class JobRunner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String partialPath = args[1] + File.separator + "partial";

		Job firstJob = Job.getInstance(getConf(), "Flight-2 - 1st phase");
		firstJob.setJarByClass(this.getClass());

		FileInputFormat.addInputPath(firstJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(firstJob, new Path(partialPath));

		firstJob.setMapperClass(RawDataToOriginMapper.class);
		firstJob.setReducerClass(OriginCounterReducer.class);

		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(IntWritable.class);

		firstJob.waitForCompletion(true);

		Job secondJob = Job.getInstance(getConf(), "Flight-2 - 2nd phase");
		secondJob.setJarByClass(this.getClass());

		FileInputFormat.addInputPath(secondJob, new Path(partialPath
				+ File.separator + "part-r-00000"));
		FileOutputFormat
				.setOutputPath(secondJob, new Path(args[1] + "-result"));

		secondJob.setMapperClass(RowToPairMapper.class);
		secondJob.setMapOutputValueClass(MapWritable.class);
		secondJob.setReducerClass(MaxOccurenceReducer.class);

		secondJob.setOutputKeyClass(Text.class);
		secondJob.setOutputValueClass(IntWritable.class);

		int completionCode = secondJob.waitForCompletion(true) ? 0 : 1;

		return completionCode;
	}
}
