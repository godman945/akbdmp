package com.pchome.soft.depot.utils;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;


@Component
public class AESAlgorithm {
    private static final Log log = LogFactory.getLog(AESAlgorithm.class);

    private String pchomeConstantKey = "uOY7HEXaH8SylKMSOwZ43g==";
    private Cipher cipherE;
    private Cipher cipherD;
    private SecretKey pchomeKey;

    /**
     * @deprecated
     */
    public static final int KEY_SIZE_128 = 0;

    public static AESAlgorithm getInstance() {
        return SingletonHolder.instance;
    }
    
    /**
     * 新舊相容
     * @param size
     * @return instance
     * @deprecated
     */
    public static AESAlgorithm getInstance(int size) {
        return getInstance();
    }

    private static class SingletonHolder {
        private static final AESAlgorithm instance = new AESAlgorithm();
    }
    
    private AESAlgorithm() {
        try {
            pchomeKey = new SecretKeySpec(Base64.decodeBase64(pchomeConstantKey.getBytes("UTF-8")), "AES");
            cipherE = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipherE.init(Cipher.ENCRYPT_MODE, pchomeKey);
            cipherD = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipherD.init(Cipher.DECRYPT_MODE, pchomeKey);
        } catch (NoSuchAlgorithmException e) {
            log.error(e.getMessage(), e);
        } catch (NoSuchPaddingException e) {
            log.error(e.getMessage(), e);
        } catch (InvalidKeyException e) {
            log.error(e.getMessage(), e);
        } catch (UnsupportedEncodingException e) {
            log.error(e.getMessage(), e);
        }
    }

    public String simpleEncode(String word) {
        if (StringUtils.isBlank(word)) {
            return null;
        }

        String str = word;
        try {
            Cipher cipher = getCipherE();
            StringBuilder sb = new StringBuilder();
            sb.append(RandomStringUtils.randomAlphanumeric(1));
            sb.append(word);
            sb.append(RandomStringUtils.randomAlphanumeric(1));
            byte[] ptext = sb.toString().getBytes("UTF-8");
            byte[] ctext = cipher.doFinal(ptext);
            str = new String(Base64.encodeBase64(ctext), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error(e.getMessage(), e);
        } catch (IllegalBlockSizeException e) {
            log.error(e.getMessage(), e);
        } catch (BadPaddingException e) {
            log.info("BadPaddingException");
        }
        return str;
    }

    
    
    
    public String decodeAppkey(String appkey) {
    	 if (StringUtils.isEmpty(appkey)) {
             return "";
         }
    	 
    	 try {
             Cipher cipher = getCipherD();
             byte[] ptext = cipher.doFinal(Base64.decodeBase64(appkey.getBytes("UTF-8")));
             appkey = new String(ptext, "UTF-8");
             if (appkey.length() > 2) {
            	 appkey = appkey.substring(1, appkey.length() - 1);
             }
         } catch (IllegalBlockSizeException e) {
             log.error(e.getMessage(), e);
         } catch (BadPaddingException e) {
             log.info("BadPaddingException");
         } catch (UnsupportedEncodingException e) {
             log.error(e.getMessage(), e);
         }
         return appkey;
    }
    
    public String simpleDecode(String word) {
        String str = word;
        if (StringUtils.isEmpty(word)) {
            return "";
        }

        if (word.length() < 24) {
            return word;
        }
        if (word.length() != 172) {
            return word;
        }

        try {
            Cipher cipher = getCipherD();
            byte[] ptext = cipher.doFinal(Base64.decodeBase64(word.getBytes("UTF-8")));
            str = new String(ptext, "UTF-8");
            if (str.length() > 2) {
                str = str.substring(1, str.length() - 1);
            }
        } catch (IllegalBlockSizeException e) {
            log.error(e.getMessage(), e);
        } catch (BadPaddingException e) {
            log.info("BadPaddingException");
        } catch (UnsupportedEncodingException e) {
            log.error(e.getMessage(), e);
        }
        return str;
    }

    public Cipher getCipherE() {
        return cipherE;
    }

    public void setCipherE(Cipher cipherE) {
        this.cipherE = cipherE;
    }

    public Cipher getCipherD() {
        return cipherD;
    }

    public void setCipherD(Cipher cipherD) {
        this.cipherD = cipherD;
    }

    /**
     * @param srcStr original string
     * @return array[0]:private key <br/> array[1]:public key
     */
    public String[] getEncrypted(String srcStr) {

        String[] result = null;
        try {
            result = new String[2];
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(128);
            
            SecretKey privateKey = keyGenerator.generateKey();
            byte[] bpKey = privateKey.getEncoded();
            result[0] = new String(Base64.encodeBase64(bpKey)).replace('+', '@');
            
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, privateKey);
            byte[] ptext = srcStr.getBytes("UTF-8");
            byte[] ctext = cipher.doFinal(ptext);
            result[1] = new String(Base64.encodeBase64(ctext)).replace('+', '@');
            return result;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        log.info("Encrypt: return null");
        return result;
    }
    
    /**
     * @param privateKey
     * @param publicKey 
     * @return original string
     */
    public String getDecrypted(String privateKey, String publicKey) {
        publicKey = publicKey.replace('@', '+');
        privateKey = privateKey.replace('@', '+');
        try {
            SecretKey pKey = new SecretKeySpec(Base64.decodeBase64(privateKey.getBytes("UTF-8")), "AES");
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, pKey);
            byte[] ptext = cipher.doFinal(Base64.decodeBase64(publicKey.getBytes("UTF-8")));
            String str = new String(ptext, "UTF-8");
            return str;
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage(), e);
        }

        log.info("Decrypt: return null");
        return "";
    }
    
    public static void main(String[] args) throws Exception {
        String str = "Alex Online";
        
        String a = IOUtils.toString(Base64.encodeBase64(str.getBytes()),"UTF-8");
        System.out.println(a);
        String seencode = AESAlgorithm.getInstance().simpleEncode(a);
        System.out.println(seencode);
        String sedecode = AESAlgorithm.getInstance().simpleDecode(seencode);
        System.out.println(sedecode);
        String b = IOUtils.toString(Base64.decodeBase64(sedecode.getBytes()),"UTF-8");
        System.out.println(b);
    }
}