package hu.bme.bigdata.homework.spark.flight3.transformation;

import hu.bme.bigdata.homework.spark.flight3.enums.Season;

import java.time.LocalDateTime;
import java.time.Month;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class DateToSeasonTransformer implements PairFunction<Tuple2<LocalDateTime, Integer>, Season, Integer> {

    private static final long serialVersionUID = -8940629807933769588L;

    @Override
    public Tuple2<Season, Integer> call(Tuple2<LocalDateTime, Integer> tuple) throws Exception {
        Season season = getSeasonFromDate(tuple._1);
        return new Tuple2<>(season, tuple._2);
    }

    private Season getSeasonFromDate(LocalDateTime date) {
        Month month = date.getMonth();
        int dayOfMonth = date.getDayOfMonth();

        Season season;
        switch (month) {
            case NOVEMBER:
                season = Season.WINTER;
                break;
            case DECEMBER:
                season = Season.WINTER;
                break;
            case JANUARY:
                season = Season.WINTER;
                break;
            case FEBRUARY:
                season = Season.WINTER;
                break;
            case MARCH:
                if (dayOfMonth <= 7) {
                    season = Season.WINTER;
                } else {
                    season = Season.SUMMER;
                }
                break;
            default:
                season = Season.SUMMER;
                break;
        }

        return season;
    }

}
