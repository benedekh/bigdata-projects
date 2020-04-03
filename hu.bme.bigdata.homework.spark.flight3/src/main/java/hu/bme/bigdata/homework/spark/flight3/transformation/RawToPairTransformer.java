package hu.bme.bigdata.homework.spark.flight3.transformation;

import java.time.LocalDateTime;
import java.time.Month;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class RawToPairTransformer implements PairFunction<String, LocalDateTime, Integer> {

    private static final long serialVersionUID = 1592716716673893751L;

    @Override
    public Tuple2<LocalDateTime, Integer> call(String line) throws Exception {
        String[] splitted = line.split(",");
        String arrDelayString = splitted[14];
        String yearString = splitted[0];
        String monthString = splitted[1];
        String dayString = splitted[2];

        Integer arrDelay = 0;
        try {
            arrDelay = Integer.valueOf(arrDelayString);
        } catch (NumberFormatException ex) {
        }

        Integer month = 5;
        try {
            month = Integer.valueOf(monthString);
        } catch (NumberFormatException ex) {
        }

        Integer day = 1;
        try {
            day = Integer.valueOf(dayString);
        } catch (NumberFormatException ex) {
        }

        Integer year = 0;
        try {
            year = Integer.valueOf(yearString);
        } catch (NumberFormatException ex) {
        }

        LocalDateTime time = LocalDateTime.of(year, Month.of(month), day, 0, 0);

        return new Tuple2<>(time, arrDelay);
    }

}
