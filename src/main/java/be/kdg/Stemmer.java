package be.kdg;

import org.tartarus.snowball.ext.PorterStemmer;

import java.io.Serializable;

/**
 * @author Floris Van Tendeloo
 */
public class Stemmer implements Serializable {
    public String stem(String input) {
        PorterStemmer state = new PorterStemmer();
        state.setCurrent(input);
        state.stem();
        return state.getCurrent();
    }
}
