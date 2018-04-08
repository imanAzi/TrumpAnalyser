package be.kdg;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author Floris Van Tendeloo
 */
public class twitterStreamFunctions {
    SparkConf conf;
    JavaStreamingContext jsc;

    public twitterStreamFunctions(boolean forceLocal, int batchDuration) {
        conf = new SparkConf().setAppName("TrumpAnalyser");
        if (forceLocal) {
            conf.setMaster("local[2]");
        }
        jsc = new JavaStreamingContext(conf ,new Duration(batchDuration));
    }
}
