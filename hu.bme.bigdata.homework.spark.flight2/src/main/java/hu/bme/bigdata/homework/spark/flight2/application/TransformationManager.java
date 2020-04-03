package hu.bme.bigdata.homework.spark.flight2.application;

import hu.bme.bigdata.homework.spark.flight2.transformation.OccurenceSummarizer;
import hu.bme.bigdata.homework.spark.flight2.transformation.RawToPairTransformer;
import hu.bme.bigdata.homework.spark.flight2.transformation.ValidRecordFilter;
import hu.bme.bigdata.homework.spark.flight2.utility.OriginTupleComparator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TransformationManager {

    public void calculateMaxTakeOff(JavaSparkContext ctx, String flightData, int partitions) {
        // transformers initialization
        ValidRecordFilter validRecordFilter = new ValidRecordFilter();
        RawToPairTransformer rawToPairTransformer = new RawToPairTransformer();
        OccurenceSummarizer occurenceSummarizer = new OccurenceSummarizer();
        OriginTupleComparator originTupleComparator = new OriginTupleComparator();

        // RDD transformations
        JavaRDD<String> lines = ctx.textFile(flightData, partitions);
        JavaRDD<String> validRecords = lines.filter(validRecordFilter);
        JavaPairRDD<String, Integer> flightOrigins = validRecords.mapToPair(rawToPairTransformer);
        JavaPairRDD<String, Integer> occurencesByKey = flightOrigins.reduceByKey(occurenceSummarizer);
        // workaround, otherwise .max function could not be invoked (SPARK-3266)
        Tuple2<String, Integer> max = ((JavaRDDLike<Tuple2<String, Integer>, ?>) occurencesByKey)
                .max(originTupleComparator);

        // print result
        System.out.println("\nMost of the airplanes took off from " + max._1 + ", " + max._2 + " times.");
    }
}
