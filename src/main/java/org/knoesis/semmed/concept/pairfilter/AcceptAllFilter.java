package org.knoesis.semmed.concept.pairfilter;

public class AcceptAllFilter implements PairFilter {

    public boolean accept(String concept1, String concept2) {
        return true;
    }

    public void close(){};

}
