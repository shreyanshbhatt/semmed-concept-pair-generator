package org.knoesis.semmed.concept.pairfilter;

public interface PairFilter {

    String FILTER_DIR = "org.knoesis.semmed.concept.FILTER_DIR";

    boolean accept(String semanticType1, String semanticType2);
    void close();

}
