package hu.bme.bigdata.homework.spark.flight3.transformation;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class SumCalculator implements Serializable {

    private static final long serialVersionUID = -5458753512529207349L;

    public class InPartitionSumCalculator implements Function2<Tuple2<Long, Integer>, Integer, Tuple2<Long, Integer>> {

        private static final long serialVersionUID = -8607578313978918953L;

        /**
         * Summarizes the values in-partition.
         * 
         * @param accumulator
         *            ._1 stores the sum
         * @param accumulator
         *            ._2 stores the count
         * @param isDelayed
         *            is the next value in the sum
         */
        @Override
        public Tuple2<Long, Integer> call(Tuple2<Long, Integer> accumulator, Integer isDelayed) throws Exception {
            Long sum = accumulator._1 + isDelayed;
            Integer count = accumulator._2 + 1;
            return new Tuple2<>(sum, count);
        }
    }

    public class CrossPartitionSumCalculator implements
            Function2<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Tuple2<Long, Integer>> {

        private static final long serialVersionUID = 2198752266676874321L;

        /**
         * Summarizes the values cross-partitions.
         * 
         * @param accumulator
         *            ._1 stores the sum (cross-partition)
         * @param accumulator
         *            ._2 stores the count (cross-partition)
         * @param nextPartion
         *            ._1 stores the next partion's sum
         * @param nextPartition
         *            ._2 stores the next partition's count
         */
        @Override
        public Tuple2<Long, Integer> call(Tuple2<Long, Integer> accumulator, Tuple2<Long, Integer> nextPartition)
                throws Exception {
            Long crossSum = accumulator._1 + nextPartition._1;
            Integer crossCount = accumulator._2 + nextPartition._2;
            return new Tuple2<>(crossSum, crossCount);
        }
    }

    /**
     * @return initial value of the aggregation
     */
    public Tuple2<Long, Integer> initialValue() {
        return new Tuple2<>(Long.valueOf(0), Integer.valueOf(0));
    }

}
