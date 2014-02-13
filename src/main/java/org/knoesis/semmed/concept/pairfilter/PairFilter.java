package org.knoesis.semmed.concept.pairfilter;

public interface PairFilter {

    String FILTER_DIR = "org.knoesis.semmed.concept.FILTER_DIR";

    boolean accept(String semType1, String semType2);
    void close();

}
