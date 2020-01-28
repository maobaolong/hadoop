package org.apache.hadoop.util;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class ByteUtil {

    /**
     * Converts a byte array to a string using UTF8 encoding.
     */
    public static String bytes2String(byte[] bytes) {
        return bytes2String(bytes, 0, bytes.length);
    }

    // Using the charset canonical name for String/byte[] conversions is much
    // more efficient due to use of cached encoders/decoders.
    private static final String UTF8_CSN = StandardCharsets.UTF_8.name();

    /**
     * Converts a string to a byte array using UTF8 encoding.
     */
    public static byte[] string2Bytes(String str) {
        try {
            return str.getBytes(UTF8_CSN);
        } catch (UnsupportedEncodingException e) {
            // should never happen!
            throw new IllegalArgumentException("UTF8 decoding is not supported", e);
        }
    }

    /**
     * Decode a specific range of bytes of the given byte array to a string
     * using UTF8.
     *
     * @param bytes The bytes to be decoded into characters
     * @param offset The index of the first byte to decode
     * @param length The number of bytes to decode
     * @return The decoded string
     */
    static String bytes2String(byte[] bytes, int offset, int length) {
        try {
            return new String(bytes, offset, length, UTF8_CSN);
        } catch (UnsupportedEncodingException e) {
            // should never happen!
            throw new IllegalArgumentException("UTF8 encoding is not supported", e);
        }
    }
}
