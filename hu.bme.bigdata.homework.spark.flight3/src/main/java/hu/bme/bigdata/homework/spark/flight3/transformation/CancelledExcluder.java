package hu.bme.bigdata.homework.spark.flight3.transformation;

import org.apache.spark.api.java.function.Function;

public class CancelledExcluder implements Function<String, Boolean> {

    private static final long serialVersionUID = -4513475604270181839L;

    @Override
    public Boolean call(String line) throws Exception {
        String[] splitted = line.split(",");
        String cancelled = splitted[21];

        return "0".equals(cancelled);
    }
}
