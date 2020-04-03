package hu.bme.bigdata.homework.spark.flight2.application;

import hu.bme.bigdata.homework.spark.flight2.application.jopt.ParameterManager;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkApplication {

    /**
     * 
     * @param args
     *            --home <SPARK_HOME>
     *            --data <FLIGHT.CSV PATH>
     *            --partitions <# PARTITIONS>
     */
    public static final void main(String[] args) {
        try {
            ParameterManager parameters = new ParameterManager();
            parameters.storeParameters(args, System.err);

            JavaSparkContext ctx = createJavaSparkContext(parameters);
            TransformationManager tm = new TransformationManager();

            tm.calculateMaxTakeOff(ctx, parameters.getFlightData(), parameters.getPartitions());
            ctx.stop();
        } catch (Exception ex) {
            Logger.getLogger(SparkApplication.class.getName()).error(
                    "Error while calculating the average of delays: " + ex.getMessage());
        }
    }

    private static JavaSparkContext createJavaSparkContext(ParameterManager parameters) {
        SparkConf sparkConf = new SparkConf().setAppName("Flight-3 ArrDelays");
        sparkConf.setSparkHome(parameters.getSparkHome());
        sparkConf.setMaster("local[" + parameters.getPartitions() + "]");

        return new JavaSparkContext(sparkConf);
    }

}
