package site.ycsb.generator;

import site.ycsb.ByteIterator;
import java.nio.CharBuffer;

import site.ycsb.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.*;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class WikipediaByteIterator extends ByteIterator {

    private static MappedByteBuffer corpus;
    private static long corpusSize;

    private final byte[] value;
    private int index = 0;

    static {
        try {
            FileChannel ch = FileChannel.open(
                Path.of("wiki_corpus.txt"),
                StandardOpenOption.READ
            );
            corpusSize = ch.size();
            corpus = ch.map(FileChannel.MapMode.READ_ONLY, 0, corpusSize);

            if (corpusSize < 10240) {
                throw new RuntimeException("Corpus too small");
            }

            System.out.println("Mapped UTF-8 corpus: " + corpusSize + " bytes");
        } catch (IOException e) {
            throw new RuntimeException("Failed to mmap Wikipedia corpus", e);
        }
    }

    public WikipediaByteIterator(String key, int size) {
        value = new byte[size];

        long hash = Utils.hash(key.hashCode());
        long offset = (hash & Long.MAX_VALUE) % (corpusSize - size * 4L);

        CharsetDecoder decoder = StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);

        CharBuffer chars;
        try {
            chars = decoder.decode(slice);
        } catch (CharacterCodingException e) {
            // Fallback: valid UTF-8 padding
            for (int i = 0; i < size; i++) {
                value[i] = (byte) ' ';
            }
            return;
        }


        byte[] encoded = chars.toString().getBytes(StandardCharsets.UTF_8);
        int copy = Math.min(encoded.length, size);
        System.arraycopy(encoded, 0, value, 0, copy);

        // Pad with spaces (valid UTF-8)
        for (int i = copy; i < size; i++) {
            value[i] = ' ';
        }
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
