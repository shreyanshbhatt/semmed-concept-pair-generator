package org.knoesis.semmed.concept.pairfilter;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SetFile;
import org.apache.hadoop.io.Text;

public class UMLSPairFilter implements PairFilter {

    private static final Logger LOG = Logger.getLogger(UMLSPairFilter.class.getName());

    private final SetFile.Reader set;
    private final Text value = new Text();

    protected UMLSPairFilter(Configuration conf) throws IOException {
        set = new SetFile.Reader(FileSystem.get(conf), conf.get(FILTER_DIR), conf);
    }

    public boolean accept(String concept1, String concept2) {
        concept1 = concept1.toLowerCase();
        concept2 = concept2.toLowerCase();
        if (concept1.compareTo(concept2) < 0) {
            value.set(concept1 + "|" + concept2);
        } else {
            value.set(concept2 + "|" + concept1);
        }
        try {
            return set.get(value) != null;
        } catch (IOException ex) {
            throw new RuntimeException("Exception reading from pair filter", ex);
        }
    }

    public void close() {
        IOUtils.closeStream(set);
    }

    public static PairFilter get(Configuration conf) {
        String setFilePath = conf.get(FILTER_DIR);
        if (setFilePath != null && !"".equals(setFilePath)) {
            try {
                return new UMLSPairFilter(conf);
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "Could not read specified pair filter path: accepting all concept pairs", ex);
                return new AcceptAllFilter();
            }
        } else {
            LOG.warning("Pair filter path not specified in config XML: accepting all concept pairs");
            return new AcceptAllFilter();
        }
    }

}
