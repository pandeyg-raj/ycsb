package site.ycsb.generator;

import site.ycsb.ByteIterator;
import site.ycsb.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;

public class WikipediaByteIterator extends ByteIterator {

    // Static corpus loaded once in memory
    private static byte[] corpus;
    private static int corpusLen;

    private final byte[] value;
    private int index = 0;

    // Load corpus once
    static {
        try {
            corpus = Files.readAllBytes(Paths.get("wiki_corpus.txt"));
            corpusLen = corpus.length;
            if (corpusLen < 10_240) {
                throw new RuntimeException("Corpus too small for 10KB objects");
            }
            System.out.println("Loaded corpus: " + corpusLen + " bytes");
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Wikipedia corpus", e);
        }
    }

    /**
     * Constructor
     * @param key Unique YCSB key (String)
     * @param size Object size in bytes (e.g., 10 KB)
     */
    public WikipediaByteIterator(String key, int size) {
        if (size >= corpusLen) {
            throw new IllegalArgumentException(
                "Object size must be smaller than corpus size"
            );
        }

        // Deterministic offset based on key hash
        long hash = Utils.hash(key.hashCode());
        int offset = (int) (Math.abs(hash) % (corpusLen - size));

        // Optional: 95% corpus text + 5% random
        int textSize = (int) (size * 0.95);
        int randSize = size - textSize;

        value = new byte[size];
        System.arraycopy(corpus, offset, value, 0, textSize);

        byte[] rand = new byte[randSize];
        // Use hash as seed for deterministic randomness
        new Random(hash).nextBytes(rand);
        System.arraycopy(rand, 0, value, textSize, randSize);
    }

    @Override
    public boolean hasNext() {
        return index < value.length;
    }

    @Override
    public byte nextByte() {
        return value[index++];
    }

    @Override
    public long bytesLeft() {
        return value.length - index;
    }
}
