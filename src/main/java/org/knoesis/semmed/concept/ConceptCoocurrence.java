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

    private String pmid;
    private int firstSentenceId;
    private int secondSentenceId;
    private String firstConcept;
    private String secondConcept;

    public ConceptCoocurrence() {
    }

    public ConceptCoocurrence(String pmid, int firstSentenceId, int secondSentenceId, String firstConcept, String secondConcept) {
        this.pmid = pmid;
        this.firstSentenceId = firstSentenceId;
        this.secondSentenceId = secondSentenceId;
        this.firstConcept = firstConcept;
        this.secondConcept = secondConcept;
    }

    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, pmid);
        statement.setInt(2, firstSentenceId);
        statement.setInt(3, secondSentenceId);
        statement.setString(4, firstConcept);
        statement.setString(5, secondConcept);
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        pmid = resultSet.getString(1);
        firstSentenceId = resultSet.getInt(2);
        secondSentenceId = resultSet.getInt(3);
        firstConcept = resultSet.getString(4);
        secondConcept = resultSet.getString(5);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(pmid);
        out.writeInt(firstSentenceId);
        out.writeInt(secondSentenceId);
        out.writeUTF(firstConcept);
        out.writeUTF(secondConcept);
    }

    public void readFields(DataInput in) throws IOException {
        pmid = in.readUTF();
        firstSentenceId = in.readInt();
        secondSentenceId = in.readInt();
        firstConcept = in.readUTF();
        secondConcept = in.readUTF();
    }

    public int compareTo(ConceptCoocurrence o) {
        int c = pmid.compareTo(o.pmid);
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
        c = firstConcept.compareTo(o.firstConcept);
        if (c != 0) {
            return c;
        }
        return secondConcept.compareTo(o.secondConcept);
    }

}
