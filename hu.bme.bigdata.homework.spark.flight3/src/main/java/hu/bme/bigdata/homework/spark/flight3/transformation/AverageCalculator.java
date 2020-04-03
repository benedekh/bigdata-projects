package hu.bme.bigdata.homework.spark.flight3.transformation;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class AverageCalculator implements Function<Tuple2<Long, Integer>, Double> {

    private static final long serialVersionUID = -3602170028043630989L;

    @Override
    public Double call(Tuple2<Long, Integer> record) throws Exception {
        Double sum = Double.valueOf(record._1);
        Integer count = record._2;

        return sum / count;
    }

}
