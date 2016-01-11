package hu.bme.bigdata.homework.spark.flight2.transformation;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class RawToPairTransformer implements PairFunction<String, String, Integer> {

    private static final long serialVersionUID = 399006031280178508L;

    @Override
    public Tuple2<String, Integer> call(String record) throws Exception {
        String[] splitted = record.split(",");
        String origin = splitted[16];

        return new Tuple2<>(origin, Integer.valueOf(1));
    }

}
