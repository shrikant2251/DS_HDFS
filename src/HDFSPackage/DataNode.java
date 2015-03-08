package HDFSPackage;

import java.io.*;
import java.util.ArrayList;

import HDFSPackage.RequestResponse.*;
import java.rmi.Remote;
import java.rmi.RemoteException;
public class DataNode implements IDataNode{

	@Override
	public byte[] readBlock(byte[] input) {
		// TODO Auto-generated method stub
		ReadBlockRequest readBlockRequest = new ReadBlockRequest(input);
		ReadBlockResponse readBlockResponse = new ReadBlockResponse();
		try{
			File file = new File(""+readBlockRequest.blockNumber);
			if(file.exists()){
				FileInputStream fis = new FileInputStream(file);
				byte[] data = new byte[(int) file.length()];
				fis.read(data);
				readBlockResponse.data = data;
				readBlockResponse.status = 1;
				fis.close();
			}
			else{
				readBlockResponse.status = -1;
			}
		}
		catch(IOException e){
			e.printStackTrace();
		}
		//readBlockRequest.blockNumber;
		return readBlockResponse.toProto();
	}

	@Override
	public byte[] writeBlock(byte[] input) {
		// TODO Auto-generated method stub
		// status successful = 1 unsuccessful = -1
		int status=1;
		WriteBlockRequest writeBlockRequest = new WriteBlockRequest(input);
		WriteBlockResponse writeBlockResponse = new WriteBlockResponse();
		
		// write into file 
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(""+writeBlockRequest.blockInfo.blockNumber));
			bw.append(writeBlockRequest.data.toString());
			bw.flush();
			bw.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			status=-1;
			e.printStackTrace();
		}
		/*Take data Node information from blockInfo Chaining call*/
		writeBlockResponse.status=status;
		return writeBlockResponse.toProto();
	}

}
