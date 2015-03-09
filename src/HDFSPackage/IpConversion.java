package HDFSPackage;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import com.google.protobuf.*;
public class IpConversion {
	public int packIP(byte[] bytes) {
		int val = 0;
		for (int i = 0; i < bytes.length; i++) {
			val <<= 8;
			val |= bytes[i] & 0xff;
		}
		
		return val;
	}

	public byte[] unpackIP(int bytes){
		return new byte[]{ (byte) ((bytes >>> 24) & 0xff),
				(byte) ((bytes >>> 16) & 0xff), (byte) ((bytes >>> 8) & 0xff),
				(byte) ((bytes) & 0xff) };
	}

	public String intToIP(int ip) throws UnknownHostException {

		return Inet4Address.getByAddress(unpackIP(ip)).getHostAddress();
	}
}
