package org.knoesis.semmed.concept.pairfilter;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SetFile;
import org.apache.hadoop.io.Text;

public class UMLSPairFilterSetFileGenerator {

    private static void main(String[] args) throws IOException {

        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Text item = new Text();
        SetFile.Writer writer = null;
        try {
            writer = new SetFile.Writer(conf, fs, uri, item.getClass(), SequenceFile.CompressionType.NONE);
            
        } finally {
            IOUtils.closeStream(writer);
        }


    }

}
