package be.kdg;

/**
 * @author Floris Van Tendeloo
 */
public class StemTest {
    public static void main(String[] args) {
        Stemmer stemmer = new Stemmer();
        String[] strings = {"cats", "allies", "congratulations", "says"};

        for (String string : strings) {
            String stemmed = stemmer.stem(string);
            System.out.println(stemmed);
        }

        // allies -> alli ???
        // says -> sai ????
        // congratul -> congratulations
    }
}
