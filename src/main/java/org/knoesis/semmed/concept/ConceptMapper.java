package org.knoesis.semmed.concept;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SetFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConceptMapper extends Mapper<NullWritable, Text, Text, ConceptCoocurrence> {

    public static final String FILTER_DIR = "org.knoesis.semmed.concept.FILTER_DIR";

    private static final Logger LOG = Logger.getLogger(ConceptMapper.class.getName());

    private final Text pmid = new Text();
    private final Text pairKey = new Text();
    private SetFile.Reader filter = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        String filterDirPath = conf.get(FILTER_DIR);
        if (filterDirPath != null && !"".equals(filterDirPath)) {
            try {
                filter = new SetFile.Reader(FileSystem.get(conf), filterDirPath, conf);
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "Could not read specified pair filter path: accepting all concept pairs", ex);
            }
        } else {
            LOG.warning("Pair filter path not specified in config XML: accepting all concept pairs");
        }
    }

    @Override
    protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        StringTokenizer st = new StringTokenizer(input, "\n");
        String[] lines = new String[st.countTokens()];
        int i = 0;

        while (st.hasMoreTokens()) {
            lines[i++] = st.nextToken();
        }

        for (i = 0; i < lines.length; i++) {
            String[] splits1 = lines[i].split("\\|");
            if (splits1.length < 6) {
                System.out.println("SHORT LINE: " + lines[i]);
                continue;
            }
            if (!splits1[5].equals("entity")) {
                continue;
            }
            for (int j = i + 1; j < lines.length; j++) {

                String[] splits2 = lines[j].split("\\|");
                if (splits2.length < 6) {
                    System.out.println("SHORT LINE: " + lines[j]);
                    continue;
                }
                if (!splits2[5].equals("entity")) {
                    continue;
                }

                pmid.set(splits1[1]);
                String geneid1 = splits1[9];
                String geneid2 = splits2[9];
                String cui1 = geneid1.isEmpty() ? splits1[6] : geneid1;
                String cui2 = geneid2.isEmpty() ? splits2[6] : geneid2;
                int sentenceid = Integer.parseInt(splits1[4]);

                if (filter != null) {
                    pairKey.set(cui1.toLowerCase() + "|" + cui2.toLowerCase());
                    if (filter.get(pairKey) == null) {
                        continue;
                    }
                }
                context.write(pmid, new ConceptCoocurrence(pmid.toString(), sentenceid, cui1, cui2));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (filter != null) {
            try {
                filter.close();
            } catch(IOException ex) {
                LOG.log(Level.WARNING, "Unable to close path filter reader", ex);
            }
        }
        super.cleanup(context);
    }



}
