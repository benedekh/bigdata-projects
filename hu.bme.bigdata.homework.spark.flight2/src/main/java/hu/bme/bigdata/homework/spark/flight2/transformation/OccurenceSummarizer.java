package hu.bme.bigdata.homework.spark.flight2.transformation;

import org.apache.spark.api.java.function.Function2;

public class OccurenceSummarizer implements Function2<Integer, Integer, Integer> {

    private static final long serialVersionUID = -4541162907421330727L;

    @Override
    public Integer call(Integer accumulator, Integer value) throws Exception {
        return accumulator + value;
    }

}
