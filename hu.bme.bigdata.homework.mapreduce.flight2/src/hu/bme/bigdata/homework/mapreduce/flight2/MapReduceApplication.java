package hu.bme.bigdata.homework.mapreduce.flight2;

import org.apache.hadoop.util.ToolRunner;

public class MapReduceApplication {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new JobRunner(), args);
		System.exit(res);
	}
}