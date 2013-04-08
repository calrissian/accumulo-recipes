package org.calrissian.accumlorecipes.changelog.support;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtils {

    public static String hashString(String itemToHash) throws NoSuchAlgorithmException, UnsupportedEncodingException {

        byte[] bytesOfMessage = new byte[0];
        bytesOfMessage = itemToHash.getBytes("UTF-8");
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] thedigest = md.digest(bytesOfMessage);
        StringBuffer hexString = new StringBuffer();
        for (int i=0; i < thedigest.length; i++) {
            String hex=Integer.toHexString(0xff & thedigest[i]);
            if(hex.length()==1) hexString.append('0');
            hexString.append(hex);
        }

        return hexString.toString();
    }
}
