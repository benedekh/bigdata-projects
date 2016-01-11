package hu.bme.bigdata.homework.spark.flight3.utility;

import hu.bme.bigdata.homework.spark.flight3.enums.Season;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class SeasonTupleComparator implements Comparator<Tuple2<Season, Double>>, Serializable {

    private static final long serialVersionUID = 13826599364549827L;

    @Override
    public int compare(Tuple2<Season, Double> firstTuple, Tuple2<Season, Double> secondTuple) {
        return firstTuple._2.compareTo(secondTuple._2);
    }

}
