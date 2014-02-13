package org.knoesis.semmed.concept.pairfilter.generator.output;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SetFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.getCompressOutput;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.getOutputPath;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SetFileOutputFormat
        extends FileOutputFormat<WritableComparable<?>, Writable> {

  public RecordWriter<WritableComparable<?>, Writable> getRecordWriter(
      TaskAttemptContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.NONE;
    if (getCompressOutput(context)) {
      // find the kind of compression to do
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(context);
    }

    Path file = getOutputPath(context);
    FileSystem fs = file.getFileSystem(conf);
    // ignore the progress parameter, since MapFile is local
    final SetFile.Writer out =
      new SetFile.Writer(conf, fs, file.toString(),
        context.getOutputKeyClass().asSubclass(WritableComparable.class),
        compressionType);

    return new RecordWriter<WritableComparable<?>, Writable>() {
        public void write(WritableComparable<?> key, Writable value)
            throws IOException {
          out.append(key, value);
        }

        public void close(TaskAttemptContext context) throws IOException {
          out.close();
        }
      };
  }

}
