package com.sentiment.worker;

import info.debatty.java.stringsimilarity.JaroWinkler;
import org.junit.jupiter.api.Test;

public class StringSimilarity {

    @Test
    public void testNormalizedStringSimilarity() {

        JaroWinkler jw = new JaroWinkler();


        System.out.println(jw.similarity("Michigan U.S.A", "USA"));
        System.out.println(jw.similarity("Michigan USA", "USA"));
        System.out.println(jw.similarity("United State Of America", "USA"));




    }
}
