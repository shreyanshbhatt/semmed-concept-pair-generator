package org.knoesis.semmed.concept.pairfilter;

public class AcceptAllFilter implements PairFilter {

    public boolean accept(String semType1, String semType2) {
        return true;
    }

    public void close(){}

}
