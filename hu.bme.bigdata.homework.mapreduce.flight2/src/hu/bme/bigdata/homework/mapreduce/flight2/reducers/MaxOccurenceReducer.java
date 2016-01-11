package hu.bme.bigdata.homework.mapreduce.flight2.reducers;

import static hu.bme.bigdata.homework.mapreduce.flight2.mappers.RowToPairMapper.occurenceKey;
import static hu.bme.bigdata.homework.mapreduce.flight2.mappers.RowToPairMapper.originKey;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxOccurenceReducer extends
		Reducer<Text, MapWritable, Text, IntWritable> {

	@Override
	public void reduce(Text word, Iterable<MapWritable> records, Context context)
			throws IOException, InterruptedException {
		int maxOccurences = -1;
		String place = "";

		for (MapWritable occurenceMap : records) {
			int occurences = ((IntWritable) occurenceMap.get(occurenceKey))
					.get();
			if (occurences > maxOccurences) {
				maxOccurences = occurences;
				place = ((Text) occurenceMap.get(originKey)).toString();
			}
		}

		System.out.println("Most of the airplanes took off from " + place
				+ " (" + maxOccurences + " times).");

		Text maxPlaceName = new Text(place);
		IntWritable maxPlaceOccurences = new IntWritable(maxOccurences);

		context.write(maxPlaceName, maxPlaceOccurences);
	}
}
