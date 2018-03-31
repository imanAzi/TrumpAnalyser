package be.kdg;

import jdk.nashorn.internal.runtime.regexp.joni.Regex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.User;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class StartMain {

    public static void main(String[] args) {
        final String consumerKey = "KZSQvBjJpVBWvb97QHeL2pbYV";
        final String consumerSecret = "6Auksnx9RNxON8XXldeiDnrTSgJOoGYjSW7R56sdyjRnJ8AotA";
        final String accessToken = "948994158479409159-zgeiikgTqDbwbZ7s7iv5otmjeyZJ20F";
        final String accessTokenSecret = "VluOhqkwvFUEpp7rwPehDV7IqWOdzETJ4wtxki7GZugqi";

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TrumpAnalyzer");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(30000));

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

        String[] filters = {"#Trump", "#trump"};

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, twitterAuth, filters);

        JavaDStream<String> statuses = twitterStream.map(
                new Function<Status, String>() {
                    public String call(Status status) {

                        StringBuilder sb = new StringBuilder();
                        //sb.append("Username: ");
                        sb.append(status.getUser().getScreenName());
                        sb.append("\n");
                        sb.append(status.getCreatedAt().toString());
                        sb.append("\n");
                        sb.append(status.getId());
                        sb.append("\n");
                        String text;
                        if (status.isRetweet()) {
                            text = status.getRetweetedStatus().getText();
                        }
                        else {
                            text = status.getText();
                        }

                        String str = text.replaceAll("[^a-zA-Z0-9 ]+","");
                        sb.append(str);
                        return sb.toString();
                    }
                }
        );

        statuses.dstream().saveAsTextFiles("file:///C:/BigDataStreaming/TrumpStream", "txt");

        //removeStopwords();

/*        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
    }


    }


