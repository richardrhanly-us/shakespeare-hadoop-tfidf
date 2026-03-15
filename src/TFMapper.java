import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
 * TFMapper
 *
 * This mapper reads each line of text from each input file.
 * It cleans each token, converts it to lowercase, and emits:
 *
 *     documentName@word    1
 *
 * Example:
 *     Hamlet.txt@king      1
 *
 * This is the first stage of the TF-IDF pipeline, where we begin
 * counting term frequency for each word inside each document.
 */
public class TFMapper extends Mapper<Object, Text, Text, IntWritable> {

    // Constant value 1 used for counting occurrences of each word.
    private final static IntWritable one = new IntWritable(1);

    // Reusable Text object to reduce object creation overhead.
    private Text wordDocKey = new Text();

    /*
     * map
     *
     * Reads one line of input text at a time, extracts the current file name,
     * tokenizes the line into words, removes punctuation, converts to lowercase,
     * and emits (document@word, 1) for each valid token.
     *
     * key      - byte offset of the line (not used here)
     * value    - the line of text being processed
     * context  - Hadoop context used to write mapper output
     */
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // Get the current file name so each word can be associated with its document.
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        // Break the line into tokens using whitespace.
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            // Normalize the token:
            // - convert to lowercase
            // - remove all non-letter characters
            String token = itr.nextToken().toLowerCase().replaceAll("[^a-z]", "");

            // Skip empty tokens after cleaning.
            if (!token.isEmpty()) {
                // Build a combined key in the format: document@word
                wordDocKey.set(fileName + "@" + token);

                // Emit one occurrence of this word in this document.
                context.write(wordDocKey, one);
            }
        }
    }
}
