package org.knoesis.semmed.concept;

import java.io.IOException;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SetFile;
import org.apache.hadoop.io.Text;

public class PairFilterSetFileGenerator {

    private static void main(String[] args) throws IOException {

        SetFile.Writer writer = new SetFile.Writer(null, null, null, Text.class, SequenceFile.CompressionType.NONE);

    }

}
