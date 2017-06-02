package com.pchome.soft.depot.utils;

import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.Cipher;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

@Component
public class EncrypUtil {

	public static MessageDigest messageDigest = null;

	public static StringBuffer stringBuffer = new StringBuffer();
	
	public EncrypUtil() throws Exception {
		messageDigest = MessageDigest.getInstance("MD5");
	}

	Log log = LogFactory.getLog(EncrypUtil.class);

	/**
	 * 產生公用、私用鑰
	 */
	public KeyPair getRSAKeyPair() throws Exception {
		KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
		keyPairGen.initialize(1024);
		KeyPair keyPair = keyPairGen.generateKeyPair();
		return keyPair;
	}

	/**
	 * apache Base64加密
	 */
	public String base64Encode(byte[] key) throws Exception {
		return new String(Base64.encodeBase64(key), "UTF-8");
	}

	/**
	 * apache Base64解密
	 */
	public byte[] base64decode(String key) throws Exception {
		return Base64.decodeBase64(key);
	}

	/**
	 * 轉換RSA公鑰
	 */
	public PublicKey getPublicKey(byte[] key) throws Exception {
		X509EncodedKeySpec pkcs8KeySpec = new X509EncodedKeySpec(key);
		KeyFactory keyFactory = KeyFactory.getInstance("RSA");
		return keyFactory.generatePublic(pkcs8KeySpec);
	}

	/**
	 * 透過公鑰解密私鑰
	 */
	public String decodeRSA(PublicKey publicKey, byte[] privateKey) throws Exception {
		Cipher cipher = Cipher.getInstance("RSA");
		cipher.init(Cipher.DECRYPT_MODE, publicKey);
		byte[] result = cipher.doFinal(privateKey);
		return new String(result, "UTF-8");
	}

	/**
	 * 透過私鑰加密內容
	 */
	public byte[] encodePrivateKeyRSA(Key privateKey) throws Exception {
		Cipher cipher = Cipher.getInstance("RSA");
		cipher.init(Cipher.ENCRYPT_MODE, privateKey);
		return cipher.doFinal("PCHOME".getBytes());
	}

	/**
	 * privateKey:私鑰 publicKey:公鑰
	 */
	public boolean decodeContentRSA(String privateKey, String publicKey) throws Exception {
		try {
			PublicKey publicKeyObj = getPublicKey(base64decode(publicKey));
			String result = decodeRSA(publicKeyObj, base64decode(privateKey));
			return result.equals("PCHOME");
		} catch (Exception e) {
			log.error("EMAIL VERIFY:" + e.getMessage());
			return false;
		}
	}

	public String stringToMd5By32byte(String message) throws Exception {
		stringBuffer.setLength(0);
		messageDigest.reset();
		messageDigest.update(message.getBytes("UTF-8"));
		byte[] byteArray = messageDigest.digest();
		for (int i = 0; i < byteArray.length; i++) {
			if (Integer.toHexString(0xFF & byteArray[i]).length() == 1) {
				stringBuffer.append("0").append(Integer.toHexString(0xFF & byteArray[i]));
			} else {
				stringBuffer.append(Integer.toHexString(0xFF & byteArray[i]));
			}

		}
		return stringBuffer.toString().toUpperCase();
	}


	public static void main(String args[]) throws Exception {
		// EncrypUtil encrypUtil = new EncrypUtil();
		//
		// //1.base64加密
		// String loginAccount = "陳天天 123";
		// String code = encrypUtil.base64Encode(loginAccount.getBytes());
		// System.out.println(">>>>"+code);
		//
		//
		//
		// KeyPair keyPair = encrypUtil.getRSAKeyPair();
		// byte[] data = encrypUtil.encodePrivateKeyRSA(keyPair.getPrivate());
		// String privateKeyBase64 = encrypUtil.base64Encode(data);
		// String publicKeyBase64 =
		// encrypUtil.base64Encode(encrypUtil.base64Encode(keyPair.getPublic().getEncoded()).getBytes());
		//
		// System.out.println("privateKey:"+privateKeyBase64);
		//// 解譯

		int code = (int) (Math.random() * (9999 - 1000 + 1)) + 1000; // 隨機生成4位整數

		System.out.println(code);

		// String b =
		// a.getMd5By32byte("4d7ce41eab09f432d5b867de0a9afbdbe013aa72578fc991207cf0287b45ebca"+pcsell);
		// System.out.println(b);
		// String url =
		// "http://pcsapistg.pchome.com.tw?pcode="+"UENTVTIwMTYwMzExMDAyMyZrZXk9Yk1LMTArUVNDemxFSHo4R3N3UE83dGdZZGFEQmZwT1RrTDlwTGtSOXZRNGdpV2VQNlJ1VkgxQi9rNWtuMFN4Skd6ZlY4TXErL0N5VEVqYU0yUy9Yelp5Zms0bjR5TDhGTVdNWlJZMWN3bEt1dDNLb3NmZnNodStkUm1QeTFYSXN0QUZqaEwzQkhXU3FjUnVsT0s4eStlZGFjNlA5RmFVSTdyZTc4b2VTZTBrPQ==";
		// String data =
		// a.base64Encode("PCSU201603110023&key=bMK10+QSCzlEHz8GswPO7tgYdaDBfpOTkL9pLkR9vQ4giWeP6RuVH1B/k5kn0SxJGzfV8Mq+/CyTEjaM2S/XzZyfk4n4yL8FMWMZRY1cwlKut3Kosffshu+dRmPy1XIstAFjhL3BHWSqcRulOK8y+edac6P9FaUI7re78oeSe0k=".getBytes());
		// System.out.println(data);
		// System.out.println(data.length());
		// KeyPair keyPair = a.getRSAKeyPair();
		// byte[] data = a.encodePrivateKeyRSA(keyPair.getPrivate());
		// System.out.println("私鑰:" + a.base64Encode(data));
		// System.out.println("公鑰:" +
		// a.base64Encode(keyPair.getPublic().getEncoded()));
		// String p1 =
		// "MhOGtohchaOJf4qrhmpzAXIFYMx61oWZwq//TgBIA/NCYK6TqYGNZJovZCCz+2EvAFsXYI7VnkHYMxG4bLJYWsRdM4T8Li6P0zXgCGt4Dkn286vZiHMkviD8hfcls9b2bBwqhoFpWuVbx/IDowpfn0Kafe7uekyCIQEArZUCvdk=";
		// String p2 =
		// "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCF5TR7TaaNTabZxhAUhfwg6jLbM2ffkk+SfFsHonrOyQ3bklnPfcSNlpXWyMKnmhUWMZDgmKiXdAZiAKe4DO/HEb2olzqG/iVL0yQmb0Xob1HOkAULi1AQ0TlvH+FV5sCYqrLbEN4Iu1vVOHANUASGInyTW+HDt426tx1RvXbZaQIDAQAB";
		// System.out.println(a.base64Encode(p2.getBytes()));
		// String
		// P3="MhOGtohchaOJf4qrhmpzAXIFYMx61oWZwq//TgBIA/NCYK6TqYGNZJovZCCz+2EvAFsXYI7VnkHYMxG4bLJYWsRdM4T8Li6P0zXgCGt4Dkn286vZiHMkviD8hfcls9b2bBwqhoFpWuVbx/IDowpfn0Kafe7uekyCIQEArZUCvdk=";
		// String
		// P4="MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCF5TR7TaaNTabZxhAUhfwg6jLbM2ffkk+SfFsHonrOyQ3bklnPfcSNlpXWyMKnmhUWMZDgmKiXdAZiAKe4DO/HEb2olzqG/iVL0yQmb0Xob1HOkAULi1AQ0TlvH+FV5sCYqrLbEN4Iu1vVOHANUASGInyTW+HDt426tx1RvXbZaQIDAQAB";
		// System.out.println(p1.equals(P3));
		// System.out.println(p2.equals(P4));
		// System.out.println(a.decodeContentRSA(p1, p2));

		// PublicKey publicKey = a.getPublicKey(a.base64decode(p2));
		// String result = a.decodeRSA(publicKey, a.base64decode(p1));
		// System.out.println(result);
	}
}