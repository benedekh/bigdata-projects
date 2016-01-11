package hu.bme.bigdata.homework.spark.flight2.transformation;

import org.apache.spark.api.java.function.Function;

public class ValidRecordFilter implements Function<String, Boolean> {

    private static final long serialVersionUID = 3508379993559478037L;

    public Boolean call(String line) throws Exception {
        String[] splitted = line.split(",");
        String cancelled = splitted[21];

        return "0".equals(cancelled);
    }

}
