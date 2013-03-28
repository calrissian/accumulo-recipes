package org.calrissian.recipes.accumulo.blobstore.support;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class BlobUtils {

    public static Logger logger = LoggerFactory.getLogger(BlobUtils.class);

    public static String hashBytes(byte[] bytes) {

        try {

            final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.reset();
            messageDigest.update(bytes);
            final byte[] resultByte = messageDigest.digest();

            return new String(Hex.encodeHex(resultByte));

        } catch (NoSuchAlgorithmException e) {
            logger.error("There was an exception trying to initialize the MD5 digest");
        }

        return null;

    }
}