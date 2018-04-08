package be.kdg;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.tartarus.snowball.ext.PorterStemmer;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;



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

        Stemmer stemmer = new Stemmer();
        //PorterStemmer stemmer = new PorterStemmer();

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, twitterAuth, filters);

        JavaDStream<String> statuses = twitterStream.map(
                new Function<Status, String>() {
                    public String call(Status status) {
                        StringBuilder sb = new StringBuilder();
                        sb.append(status.getUser().getScreenName());
                        sb.append("\n");
                        sb.append(status.getCreatedAt().toString());
                        sb.append("\n");
                        sb.append(status.getId());
                        sb.append("\n");
                        String text;
                        if (status.isRetweet()) {
                            text = status.getRetweetedStatus().getText().toLowerCase();
                        }
                        else {
                            text = status.getText().toLowerCase();
                        }
                        //text = text.replaceAll("[\\p{Punct}&&[^#]]+", "");
                        text = text.replaceAll("[^\\w\\#\\s\\d]+", "");
                        String str = removeStopwords(text);
                        //List<String> words = IteratorUtils.
                        //str = str.replaceAll("[^a-zA-Z0-9 ]+", "");
                        sb.append(str);
                        return sb.toString();
                    }
                }
        );

        //JavaDStream<String> stemmed = statuses.map(str -> stemmer.stem(str));

        statuses.dstream().saveAsTextFiles("file:///C:/BigDataStreaming/TrumpStream", "txt");

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static String removeStopwords(String input) {
        List<String> stopwords = new ArrayList<>();
        String line = "";
        File file = new File("C:\\Users\\FLO\\Google Drive\\KdG\\ETL, NoSQL & Big Data 2\\Examen\\TrumpAnalyser\\src\\main\\java\\be\\kdg\\stopwords.txt");
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            while (line != null) {
                line = br.readLine();
                stopwords.add(line);
            }
        } catch (Exception e) {

        }

        List<String> words = Arrays.asList(input.split(" "));
        List<String> arrayList = new ArrayList<>(words);
        arrayList.removeAll(stopwords);
        StringBuilder sb = new StringBuilder();
        for (String word : arrayList) {
            sb.append(word);
            sb.append(" ");
        }
        return sb.toString();
    }
}


