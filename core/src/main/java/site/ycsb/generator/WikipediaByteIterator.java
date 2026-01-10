package site.ycsb.generator;

import site.ycsb.ByteIterator;
import site.ycsb.Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class WikipediaByteIterator extends ByteIterator {

    // UTF-8 corpus as characters
    private static String corpus;
    private static int corpusChars;

    private final byte[] value;
    private int index = 0;

    static {
      try {
          corpus = Files.readString(
              Paths.get("wiki_corpus.txt"),
              StandardCharsets.UTF_8
          );
          corpusChars = corpus.length();
  
          if (corpusChars < 4096) {
              throw new RuntimeException("Corpus too small");
          }
  
          System.out.println("Loaded UTF-8 corpus, chars=" + corpusChars);
      } catch (IOException e) {
          throw new RuntimeException("Failed to load UTF-8 Wikipedia corpus", e);
      }
    }


    public WikipediaByteIterator(String key, int size) {
        long hash = Utils.hash(key.hashCode());
        int charOffset = (int) ((hash & Long.MAX_VALUE) % corpusChars);

        StringBuilder sb = new StringBuilder(size);

        // Build UTF-8-safe text
        while (sb.length() < size) {
            sb.append(corpus.charAt(charOffset));
            charOffset++;
            if (charOffset == corpusChars) {
                charOffset = 0;
            }
        }

        // Encode to UTF-8
        byte[] encoded = sb.toString().getBytes(StandardCharsets.UTF_8);

        // Enforce exact byte size
        value = new byte[size];
        int copyLen = Math.min(encoded.length, size);
        System.arraycopy(encoded, 0, value, 0, copyLen);

        // Pad with spaces if needed (valid UTF-8)
        for (int i = copyLen; i < size; i++) {
            value[i] = (byte) ' ';
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
