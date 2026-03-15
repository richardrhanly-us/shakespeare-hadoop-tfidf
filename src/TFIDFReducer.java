import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * TFIDFReducer
 *
 * This reducer joins term frequency (TF) and document frequency (DF)
 * for each word, computes the TF-IDF score, and keeps the top 50 terms
 * for each document.
 *
 * TF-IDF formula:
 *     tfidf = tf * log(N / df)
 *
 * where:
 *     tf = term frequency in the document
 *     df = number of documents containing the word
 *     N  = total number of documents in the corpus
 *
 * For this Shakespeare project, N = 3.
 *
 * The reducer stores the top 50 highest-scoring terms for each document
 * and writes them during cleanup.
 */
public class TFIDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    // Total number of Shakespeare documents in the corpus.
    private static final int TOTAL_DOCUMENTS = 3;

    /*
     * Holds top terms for each document.
     *
     * Outer map key   = document name
     * Inner TreeMap key = TF-IDF score
     * Inner TreeMap value = word
     *
     * TreeMap keeps scores sorted so we can easily remove the smallest
     * when more than 50 terms are stored.
     */
    private Map<String, TreeMap<Double, String>> topTermsByDoc = new HashMap<String, TreeMap<Double, String>>();

    /*
     * reduce
     *
     * For each word, this reducer receives:
     * - one DF value
     * - one or more TF values for different documents
     *
     * It computes TF-IDF for each (document, word) pair and stores
     * only the top 50 terms per document.
     *
     * key      - word
     * values   - tagged TF and DF records
     * context  - Hadoop context used later during cleanup
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int df = 0;

        // Holds TF per document for the current word.
        Map<String, Integer> tfByDoc = new HashMap<String, Integer>();

        for (Text val : values) {
            String record = val.toString();

            if (record.startsWith("DF@")) {
                // Example: DF@2
                df = Integer.parseInt(record.substring(3));
            } else if (record.startsWith("TF@")) {
                // Example: TF@Hamlet.txt=18
                String tfPart = record.substring(3);
                String[] pieces = tfPart.split("=");

                if (pieces.length != 2) {
                    continue;
                }

                String document = pieces[0];
                int tf = Integer.parseInt(pieces[1]);

                tfByDoc.put(document, tf);
            }
        }

        // Skip if DF was not found or is zero.
        if (df == 0) {
            return;
        }

        // Compute TF-IDF for each document containing this word.
        for (Map.Entry<String, Integer> entry : tfByDoc.entrySet()) {
            String document = entry.getKey();
            int tf = entry.getValue();

            double tfidf = tf * Math.log((double) TOTAL_DOCUMENTS / df);

            // Create a TreeMap for this document if it does not already exist.
            if (!topTermsByDoc.containsKey(document)) {
                topTermsByDoc.put(document, new TreeMap<Double, String>());
            }

            TreeMap<Double, String> topTerms = topTermsByDoc.get(document);

            /*
             * Handle duplicate scores by making the key slightly larger
             * until it is unique. This avoids overwriting words that
             * happen to have the same TF-IDF score.
             */
            while (topTerms.containsKey(tfidf)) {
                tfidf += 0.0000001;
            }

            topTerms.put(tfidf, key.toString());

            // Keep only the top 50 terms for this document.
            if (topTerms.size() > 50) {
                topTerms.remove(topTerms.firstKey());
            }
        }
    }

    /*
     * cleanup
     *
     * After all reduce calls are complete, this method outputs the stored
     * top 50 terms for each document in descending TF-IDF order.
     *
     * Output format:
     *     document -> word    tfidf
     *
     * Example key:
     *     Hamlet.txt -> revenge
     */
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        for (Map.Entry<String, TreeMap<Double, String>> docEntry : topTermsByDoc.entrySet()) {
            String document = docEntry.getKey();
            TreeMap<Double, String> topTerms = docEntry.getValue();

            for (Map.Entry<Double, String> termEntry : topTerms.descendingMap().entrySet()) {
                String outputKey = document + " -> " + termEntry.getValue();
                context.write(new Text(outputKey), new DoubleWritable(termEntry.getKey()));
            }
        }
    }
}