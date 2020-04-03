package hu.bme.bigdata.homework.spark.flight3.utility;

import hu.bme.bigdata.homework.spark.flight3.enums.Season;

import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SeasonTuplePrinter implements VoidFunction<Tuple2<Season, Tuple2<Long, Integer>>> {

    private static final long serialVersionUID = -3624526999361038180L;

    private static boolean    isHeaderPrinted  = false;

    @Override
    public void call(Tuple2<Season, Tuple2<Long, Integer>> tuple) throws Exception {
        if (!isHeaderPrinted) {
            System.out.println();
            System.out.println("<SEASON>" + '\t' + "<NUMBER OF DELAYS>" + '\t' + "<NUMBER OF RECORDS>");
            isHeaderPrinted = true;
        }
        System.out.println(tuple._1.toString() + '\t' + tuple._2._1 + '\t' + tuple._2._2);
    }

}
