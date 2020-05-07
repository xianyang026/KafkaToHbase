package MD5;

import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Md5Hash {
    public static String md5HashMethod(String str, Integer length) {
        String res = null;
        try {
            res = getMD5AsHex(str.getBytes("UTF-8")).substring(0, length);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    public static String md5HashMethod(String str) {
        String res = null;
        try {
            res = getMD5AsHex(str.getBytes("UTF-8")).substring(0, 8);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }


    private static String getMD5AsHex(byte[] key) {
        return getMD5AsHex(key, 0, key.length);
    }




    private static String getMD5AsHex(byte[] key, int offset, int length) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(key, offset, length);
            byte[] digest = md.digest();
            return new String(Hex.encodeHex(digest));
        } catch (NoSuchAlgorithmException var5) {
            throw new RuntimeException("Error computing MD5 hash", var5);
        }
    }

}
