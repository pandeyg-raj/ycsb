package site.ycsb.generator;

import site.ycsb.ByteIterator;
import site.ycsb.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

public class WikipediaByteIterator extends ByteIterator {

    // Shared memory-mapped corpus
    private static ByteBuffer corpusBuf;
    private static long corpusLen;

    private final byte[] value;
    private int index = 0;

    static {
        try {
            FileChannel fc = FileChannel.open(Paths.get("wiki_corpus.txt"), StandardOpenOption.READ);
            corpusBuf = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            corpusLen = fc.size();
            System.out.println("Loaded Wikipedia corpus from wiki_corpus.txt (" + corpusLen + " bytes)");
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Wikipedia corpus", e);
        }
    }

    /**
     * Constructor
     * @param key unique YCSB key + field
     * @param size object size in bytes
     */
    public WikipediaByteIterator(String key, int size) {
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Object size too large: " + size);
        }
        if (size >= corpusLen) {
            throw new IllegalArgumentException("Object size must be smaller than corpus size");
        }

        // Deterministic offset
        long hash = Utils.hash(key.hashCode());
        int offset = (int)(Math.abs(hash) % (corpusLen - size));

        // 95% corpus text + 5% random
        int textSize = (int)(size * 0.95);
        int randSize = size - textSize;

        value = new byte[size];

        // Copy from memory-mapped buffer
        synchronized (corpusBuf) {
            // Use temporary buffer to avoid modifying shared buffer position
            MappedByteBuffer slice = corpusBuf.duplicate();
            slice.position(offset);
            slice.get(value, 0, textSize);
        }

        // Deterministic random suffix
        byte[] rand = new byte[randSize];
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
