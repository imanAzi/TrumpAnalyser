package be.kdg;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import java.util.ArrayList;
import java.util.List;

public class StartMain {

    public static void main(String[] args) throws Exception {
        if (args.length > 5 || args.length < 4) {
            System.err.println("Usage:\n " +
                    "1 - input naive bayes model \n" +
                    "2 - output_location keywords \n" +
                    "3 - output location sentiment \n" +
                    "4 - batchduration \n" +
                    "5 - (forceLocal)");
            System.exit(1);
        }

        int batchDuration = 30000;

        String input_naives_bayes = args[0];
        String output_keywords = args[1];
        String output_sentiment = args[2];

        if (args[3].matches("\\d+")) {
            batchDuration = Integer.parseInt(args[3]);
        }

        SparkConf conf = new SparkConf().setAppName("TrumpAnalyzer");
        if (args.length == 5 && args[4].toLowerCase().equals("forcelocal")) {
            conf.setMaster("local[2]");
        }

        final String consumerKey = "KZSQvBjJpVBWvb97QHeL2pbYV";
        final String consumerSecret = "6Auksnx9RNxON8XXldeiDnrTSgJOoGYjSW7R56sdyjRnJ8AotA";
        final String accessToken = "948994158479409159-zgeiikgTqDbwbZ7s7iv5otmjeyZJ20F";
        final String accessTokenSecret = "VluOhqkwvFUEpp7rwPehDV7IqWOdzETJ4wtxki7GZugqi";

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(batchDuration));

        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

        NaiveBayesModel naiveBayesModel = NaiveBayesModel.load(SparkContext.getOrCreate(), input_naives_bayes);
        if (naiveBayesModel == null) {
            System.err.println("No Naive Bayes Model found!");
            System.exit(1);
        }

        String[] filters = {"#Trump", "#trump"};
        Stemmer stemmer = new Stemmer();

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, twitterAuth, filters);

        JavaDStream<Status> removeRetweets = twitterStream.filter(status -> !status.isRetweet());

        JavaDStream<String> keywords = removeRetweets.map(
                (Function<Status, String>) status -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append(status.getUser().getScreenName());
                    sb.append("\n");
                    sb.append(status.getCreatedAt().toString());
                    sb.append("\n");
                    sb.append(status.getId());
                    sb.append("\n");
                    String text = status.getText().toLowerCase();
                    text = text.replaceAll("[^\\w\\#\\s\\d]+", "");
                    text = StopwordsRemover.removeStopwords(text);
                    String[] strArray = text.split(" ");
                    for (String s : strArray) {
                        s.trim();
                        sb.append(stemmer.stem(s));
                        sb.append(" ");
                    }
                    return sb.toString();
                }
        );

        HashingTF tf = new HashingTF();

        JavaDStream<String> sentimentStream = removeRetweets.map(
                (Function<Status, String>) status -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append(status.getUser().getScreenName());
                    sb.append("_");
                    sb.append(status.getCreatedAt().toString());
                    sb.append(";");
                    sb.append(status.getUser().getScreenName());
                    sb.append(";");
                    sb.append(status.getCreatedAt().toString());
                    sb.append(";");
                    String text = status.getText().toLowerCase();
                    text = text.replaceAll("[^\\w\\s\\d]+", "");
                    text = StopwordsRemover.removeStopwords(text);
                    String[] strArray = text.split(" ");
                    List<String> stemmed = new ArrayList<>();
                    for (String s : strArray) {
                        s.trim();
                        if (!s.startsWith("http")) {
                            stemmed.add(stemmer.stem(s));
                        }
                    }
                    Double sentiment = naiveBayesModel.predict(tf.transform(stemmed));
                    System.out.println(sentiment);
                    sb.append(sentiment.intValue());
                    return sb.toString();
                }
        );

        JavaDStream<String> stemmedWords = keywords.flatMap(
                (FlatMapFunction<String, String>) input -> {
                    String[] strArray = input.split("\n");
                    List<String> words = new ArrayList<>();
                    for (String s : strArray[3].split(" ")) {
                        String isHashtag = "";
                        if (s.startsWith("#")) {
                            isHashtag = "Yes";
                            s = s.replaceAll("#", "");
                        } else {
                            isHashtag = "No";
                        }
                        if (!s.startsWith("http")) {
                            words.add(s + ";" + strArray[0] + ";" + strArray[1] + ";" +
                                    strArray[2] + ";" + isHashtag);
                        }
                    }
                    return words.iterator();
                });

        stemmedWords.dstream().saveAsTextFiles(output_keywords, "");

        sentimentStream.dstream().saveAsTextFiles(output_sentiment, "");


        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


