package be.kdg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Floris Van Tendeloo
 */
public class StopwordsRemover {
    public static String removeStopwords(String input) {
        String[] arrStopWords = {"i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves","he","him","his","himself","she","her","hers","herself","it","its","itself","they","them","their","theirs","themselves","what","which","who","whom","this","that","these","those","am","is","are","was","were","be","been","being","have","has","had","having","do","does","did","doing","a","an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against","between","into","through","during","before","after","above","below","to","from","up","down","in","out","on","off","over","under","again","further","then","once","here","there","when","where","why","how","all","any","both","each","few","more","most","other","some","such","no","nor","not","only","own","same","so","than","too","very","s","t","can","will","just","don","should","now","null"};
        List<String> stopwords = new ArrayList<>(Arrays.asList(arrStopWords));
        List<String> words = Arrays.asList(input.split(" "));
        List<String> arrayList = new ArrayList<>(words);
        arrayList.removeAll(stopwords);
        StringBuilder sb = new StringBuilder();
        for (String word : arrayList) {
            word.trim();
            sb.append(word);
            sb.append(" ");
        }
        return sb.toString();
    }
}
