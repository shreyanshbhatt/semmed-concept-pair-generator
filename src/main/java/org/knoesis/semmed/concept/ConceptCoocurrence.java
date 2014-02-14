package org.knoesis.semmed.concept;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class ConceptCoocurrence implements DBWritable, WritableComparable<ConceptCoocurrence> {

    private static final String SEPARATOR = "\t";

    private String pmid;
    private String firstConcept;
    private int firstSentenceId;
    private String secondConcept;
    private int secondSentenceId;

    public ConceptCoocurrence() {}

    public ConceptCoocurrence(String pmid, String firstConcept, int firstSentenceId, String secondConcept, int secondSentenceId) {
        this.pmid = pmid;
        this.firstConcept = firstConcept;
        this.firstSentenceId = firstSentenceId;
        this.secondConcept = secondConcept;
        this.secondSentenceId = secondSentenceId;
    }

    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, pmid);
        statement.setString(2, firstConcept);
        statement.setInt(3, firstSentenceId);
        statement.setString(4, secondConcept);
        statement.setInt(5, secondSentenceId);
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        pmid = resultSet.getString(1);
        firstConcept = resultSet.getString(2);
        firstSentenceId = resultSet.getInt(3);
        secondConcept = resultSet.getString(4);
        secondSentenceId = resultSet.getInt(5);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(pmid);
        out.writeUTF(firstConcept);
        out.writeInt(firstSentenceId);
        out.writeUTF(secondConcept);
        out.writeInt(secondSentenceId);
    }

    public void readFields(DataInput in) throws IOException {
        pmid = in.readUTF();
        firstConcept = in.readUTF();
        firstSentenceId = in.readInt();
        secondConcept = in.readUTF();
        secondSentenceId = in.readInt();
    }

    public int compareTo(ConceptCoocurrence o) {
        int c = firstConcept.compareTo(o.firstConcept);
        if (c != 0) {
            return c;
        }
        c = secondConcept.compareTo(o.secondConcept);
        if (c != 0) {
            return c;
        }
        c = pmid.compareTo(o.pmid);
        if (c != 0) {
            return c;
        }
        if (firstSentenceId < o.firstSentenceId) {
            return -1;
        } else if (firstSentenceId > o.firstSentenceId) {
            return 1;
        }
        if (secondSentenceId < o.secondSentenceId) {
            return -1;
        } else if (secondSentenceId > o.secondSentenceId) {
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return new StringBuilder(pmid).append(SEPARATOR)
                .append(firstConcept).append(SEPARATOR)
                .append(firstSentenceId).append(SEPARATOR)
                .append(secondConcept).append(SEPARATOR)
                .append(secondSentenceId)
                .toString();
    }

}
