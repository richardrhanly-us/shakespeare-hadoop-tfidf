import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * DFMapper
 *
 * This mapper reads the output from Job 1:
 *
 *     document@word    tf
 *
 * It separates the document name and word, then emits:
 *
 *     word    document
 *
 * Example:
 *     Hamlet.txt@king  5
 *
 * becomes:
 *     king    Hamlet.txt
 *
 * This prepares the data so Job 2 can count how many documents
 * each word appears in.
 */
public class DFMapper extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text();
    private Text document = new Text();

    /*
     * map
     *
     * Reads one line from the TF output, splits the combined key into
     * document name and word, and emits (word, document).
     *
     * key      - byte offset of the line (not used here)
     * value    - one line from Job 1 output
     * context  - Hadoop context used to write mapper output
     */
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // Split the line into the left side (document@word) and right side (tf).
        String[] parts = value.toString().split("\\s+");

        // Skip malformed lines.
        if (parts.length < 1) {
            return;
        }

        // Split document@word into two pieces.
        String[] docWord = parts[0].split("@");

        // Skip malformed combined keys.
        if (docWord.length != 2) {
            return;
        }

        document.set(docWord[0]);
        word.set(docWord[1]);

        // Emit (word, document) so reducer can count unique documents per word.
        context.write(word, document);
    }
}