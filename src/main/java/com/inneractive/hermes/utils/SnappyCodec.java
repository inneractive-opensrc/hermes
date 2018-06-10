package com.inneractive.hermes.utils;

import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;

public class SnappyCodec {
    public static byte[] readResourceFile(String fileName)
            throws IOException
    {
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(fileName));
        return readFully(input);
    }

    public static byte[] readFully(InputStream input)
            throws IOException
    {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            for (int readBytes = 0; (readBytes = input.read(buf)) != -1; ) {
                out.write(buf, 0, readBytes);
            }
            out.flush();
            return out.toByteArray();
        }
        finally {
            input.close();
        }
    }

    public static byte[] byteWiseReadFully(InputStream input)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        for (int readData = 0; (readData = input.read()) != -1; ) {
            out.write(readData);
        }
        out.flush();
        return out.toByteArray();
    }

    public byte[] read(String fileName) throws Exception
    {
        ByteArrayOutputStream compressedBuf = new ByteArrayOutputStream();
        SnappyOutputStream snappyOut = new SnappyOutputStream(compressedBuf);
        byte[] orig = readResourceFile(fileName);
        snappyOut.write(orig);
        snappyOut.close();
        byte[] compressed = compressedBuf.toByteArray();

        SnappyInputStream in = new SnappyInputStream(new ByteArrayInputStream(compressed));
        byte[] uncompressed = readFully(in);

        return uncompressed;
    }

    public byte[] readBlockCompressedData(String fileName)  throws Exception
    {
        byte[] orig = readResourceFile(fileName);
        byte[] compressed = Snappy.compress(orig);

        SnappyInputStream in = new SnappyInputStream(new ByteArrayInputStream(compressed));
        byte[] uncompressed = readFully(in);

        return uncompressed;
    }

    public byte[] biteWiseRead() throws Exception
    {
        byte[] orig = readResourceFile("testdata/calgary/paper6");
        byte[] compressed = Snappy.compress(orig);

        SnappyInputStream in = new SnappyInputStream(new ByteArrayInputStream(compressed));
        byte[] uncompressed = byteWiseReadFully(in);

        return uncompressed;
    }

}
