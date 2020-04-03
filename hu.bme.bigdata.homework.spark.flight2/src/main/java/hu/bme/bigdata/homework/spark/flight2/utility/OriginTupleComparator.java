package hu.bme.bigdata.homework.spark.flight2.utility;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class OriginTupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

    private static final long serialVersionUID = -2644667108949856177L;

    @Override
    public int compare(Tuple2<String, Integer> firstTuple, Tuple2<String, Integer> secondTuple) {
        return firstTuple._2.compareTo(secondTuple._2);
    }

}
