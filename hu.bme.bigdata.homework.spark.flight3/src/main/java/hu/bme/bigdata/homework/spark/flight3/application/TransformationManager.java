package hu.bme.bigdata.homework.spark.flight3.application;

import hu.bme.bigdata.homework.spark.flight3.enums.Season;
import hu.bme.bigdata.homework.spark.flight3.transformation.ArrDelayToIsDelayedTransformer;
import hu.bme.bigdata.homework.spark.flight3.transformation.AverageCalculator;
import hu.bme.bigdata.homework.spark.flight3.transformation.CancelledExcluder;
import hu.bme.bigdata.homework.spark.flight3.transformation.DateToSeasonTransformer;
import hu.bme.bigdata.homework.spark.flight3.transformation.RawToPairTransformer;
import hu.bme.bigdata.homework.spark.flight3.transformation.SumCalculator;
import hu.bme.bigdata.homework.spark.flight3.utility.SeasonTupleComparator;
import hu.bme.bigdata.homework.spark.flight3.utility.SeasonTuplePrinter;

import java.time.LocalDateTime;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TransformationManager {

    public void calculateAvgDelays(JavaSparkContext ctx, String flightData, int partitions) {
        // transformers initialization
        CancelledExcluder cancelledExcluder = new CancelledExcluder();
        RawToPairTransformer rawToPairTransformer = new RawToPairTransformer();
        DateToSeasonTransformer dateToSeasonTransformer = new DateToSeasonTransformer();
        ArrDelayToIsDelayedTransformer arrDelayToIsDelyedTransformer = new ArrDelayToIsDelayedTransformer();

        SumCalculator sumCalculator = new SumCalculator();
        SumCalculator.InPartitionSumCalculator inpartitionSummarize = sumCalculator.new InPartitionSumCalculator();
        SumCalculator.CrossPartitionSumCalculator crosspartitionSummarize = sumCalculator.new CrossPartitionSumCalculator();

        AverageCalculator avgCalculator = new AverageCalculator();

        // RDD transformations
        JavaRDD<String> lines = ctx.textFile(flightData, partitions);
        JavaRDD<String> preparedInput = lines.filter(cancelledExcluder);
        JavaPairRDD<LocalDateTime, Integer> dates = preparedInput.mapToPair(rawToPairTransformer);
        JavaPairRDD<Season, Integer> seasonedDates = dates.mapToPair(dateToSeasonTransformer);
        JavaPairRDD<Season, Integer> seasonedIsDelayed = seasonedDates.mapToPair(arrDelayToIsDelyedTransformer);
        JavaPairRDD<Season, Tuple2<Long, Integer>> summarizedBySeason = seasonedIsDelayed.aggregateByKey(
                sumCalculator.initialValue(), inpartitionSummarize, crosspartitionSummarize);
        JavaPairRDD<Season, Double> averageBySeason = summarizedBySeason.mapValues(avgCalculator);

        // print the result
        printResult(summarizedBySeason, averageBySeason);
    }

    private void printResult(JavaPairRDD<Season, Tuple2<Long, Integer>> summarizedBySeason,
            JavaPairRDD<Season, Double> averageBySeason) {
        SeasonTuplePrinter printer = new SeasonTuplePrinter();
        summarizedBySeason.foreach(printer);

        SeasonTupleComparator seasonTupleComparator = new SeasonTupleComparator();

        // workaround, otherwise .max function could not be invoked (SPARK-3266)
        Tuple2<Season, Double> max = ((JavaRDDLike<Tuple2<Season, Double>, ?>) averageBySeason)
                .max(seasonTupleComparator);

        // workaround, otherwise .min function could not be invoked (SPARK-3266)
        Tuple2<Season, Double> min = ((JavaRDDLike<Tuple2<Season, Double>, ?>) averageBySeason)
                .min(seasonTupleComparator);

        Double percentages = Math.round((max._2 - min._2) * 100.0) / 100.0;
        System.out.println();
        System.out.println("In average, in " + max._1 + " " + percentages + "% more of the planes are delayed than in "
                + min._1 + ".");
    }
}
