package hu.bme.bigdata.homework.spark.flight3.transformation;

import hu.bme.bigdata.homework.spark.flight3.enums.Season;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ArrDelayToIsDelayedTransformer implements PairFunction<Tuple2<Season, Integer>, Season, Integer> {

    private static final long serialVersionUID = 1338211487290991471L;

    @Override
    public Tuple2<Season, Integer> call(Tuple2<Season, Integer> record) throws Exception {
        int arrDelay = record._2.intValue();
        int isDelayed = (arrDelay > 0) ? 1 : 0;

        return new Tuple2<>(record._1, isDelayed);
    }

}
