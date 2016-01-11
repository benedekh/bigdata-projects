package hu.bme.bigdata.homework.spark.flight2.application.jopt;

import java.io.IOException;
import java.io.PrintStream;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class ParameterManager {

    private String sparkHome;
    private String flightData;
    private int    partitions;

    public void storeParameters(String[] args, PrintStream stdOut) throws IOException {
        OptionParser parser = new OptionParser();

        ArgumentAcceptingOptionSpec<String> sparkHomeArg = parser.accepts("home", "Spark home directory [mandatory]")
                .withRequiredArg().ofType(String.class);
        ArgumentAcceptingOptionSpec<String> flightDataArg = parser
                .accepts("data", "Flight data CSV location [mandatory]").withRequiredArg().ofType(String.class);
        ArgumentAcceptingOptionSpec<Integer> partitionsArg = parser
                .accepts("partitions", "Number of partitions [mandatory]").withRequiredArg().ofType(Integer.class);

        OptionSet parsed = parser.parse(args);

        if (!parsed.has(sparkHomeArg) || !parsed.has(flightDataArg) || !parsed.has(partitionsArg)) {
            parser.printHelpOn(stdOut);
        }

        sparkHome = parsed.valueOf(sparkHomeArg);
        flightData = parsed.valueOf(flightDataArg);
        partitions = parsed.valueOf(partitionsArg).intValue();
    }

    public String getSparkHome() {
        return sparkHome;
    }

    public String getFlightData() {
        return flightData;
    }

    public int getPartitions() {
        return partitions;
    }
    
    

}
