package HDFSPackage;
import com.google.protobuf.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import HDFSPackage.RequestResponse.*;

public class GeneralClient {
	public static String nameNodeIP;
	public static int nameNodePort,blockSize;
	public static void config(String filePath) throws FileNotFoundException, IOException
	{
		File file= new File(filePath);
		if(file.exists())
		{
			try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			    String line;
			    while ((line = br.readLine()) != null) {
			     String [] array = line.split("=");
			     if(array.length !=2)
			     {
			    	 System.out.println("Error processing dataNodeConfigFile");
			    	 return;
			     }
			     switch(array[0])
			     {
			     case "nameNodeIP":
 	 								nameNodeIP= new String(array[1]);
 	 								break;
			     case "nameNodePort":
 	 								nameNodePort=Integer.parseInt(array[1]);
 	 								break;
			     case "blockSize":
 	 								blockSize=Integer.parseInt(array[1]);
 	 								break;
 
			     }
			    }
			}	
		}else {
			System.out.println("Config file does not exists please check the location");
			System.exit(0);
		}
	}
	/* openFile will return the response of openFileResponse*/
	public byte[] open(String fileName, boolean forRead) {
		byte[] response = new byte[1];
		int status = 1;
		try {
			Registry myreg = LocateRegistry.getRegistry(nameNodeIP,nameNodePort);
			INameNode in = (INameNode) myreg.lookup("NameNode");
			OpenFileRequest openFileRequest = new OpenFileRequest(fileName,forRead);
			response = in.openFile(openFileRequest.toProto());
		} catch (Exception e) {
			status = -1;
			e.printStackTrace();
		}
		if (status == -1) {
			OpenFileRespose openFileResponse = new OpenFileRespose(-1, -1, new ArrayList<Integer>());
			response = openFileResponse.toProto();
		}

		return response;
	}

	// close File
	int close(int fileHandle) {
		int status = 1;
		INameNode in = null;
		CloseFileRequest closeFileRequest = new CloseFileRequest(fileHandle);

		Registry myreg;
		try {
			myreg = LocateRegistry.getRegistry(nameNodeIP,nameNodePort);
			in = (INameNode) myreg.lookup("NameNode");

			byte[] closeFileRes;
			closeFileRes = in.closeFile(closeFileRequest.toProto());
			CloseFileResponse closeFileResponse = new CloseFileResponse(
					closeFileRes);
			status = closeFileResponse.status;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("GeneralClient :: Error ,  couldnot close File handle :: " + closeFileRequest.handle);
			status = -1;
		}

		return status;
	}

	// read file
	public int read(String fileName) {

		int status = 1;

		// Open File for read
		byte[] openResponse;
		openResponse = open(fileName, true);
		OpenFileRespose openFileResponse = new OpenFileRespose(openResponse);

		// check status of openFile
		if (openFileResponse.status == -1) {
			// openFile Unsuccessful
			status = -1;
			return status;
		}

		INameNode in = null;
		IDataNode dataNode = null;
		IpConversion ipObj = new IpConversion();
		try {
			Registry myreg = LocateRegistry.getRegistry(nameNodeIP,nameNodePort);
			in = (INameNode) myreg.lookup("NameNode");
		} catch (Exception e) {
			status = -1;
			System.out.println("GeneralClient :: RMI Error locating NameNode Registry.");
			e.printStackTrace();
			return status;
		}

		// get locations for list of blocks returned by openFile
		BlockLocationRequest blockLocationRequest = new BlockLocationRequest(openFileResponse.blockNums);

		byte[] getBlockLocationResponse;
		/*
		 * for(int i:openFileResponse.blockNums){
			System.out.println("GenClient read method openFileResponse block :" + i);
		}
		*/
		try {
			byte [] temp = blockLocationRequest.toProto();
			//System.out.println("GenClient temp byte getlocations input" + temp.length);
			getBlockLocationResponse = in.getBlockLocations(temp);
		} 
		catch (Exception e1) {
			// TODO Auto-generated catch block
			status = -1;
			System.out.println("GeneralClient :: Error retrieving block locations.");
			e1.printStackTrace();
			return status;
		}
		BlockLocationResponse blockLocationResponse = new BlockLocationResponse(getBlockLocationResponse);
		
		/*
		 * System.out.println(" GenClient read method BlockLocationResponse status = " + blockLocationResponse.status + " blockNum size = " + blockLocationResponse.blockLocations.size());
		Iterator<BlockLocations> it = blockLocationResponse.blockLocations.iterator();
		while(it.hasNext()){
			BlockLocations b = it.next();
			Iterator<DataNodeLocation> it1 = b.locations.iterator();
			System.out.print("Genclient read method get block " + b.blockNumber + " -->");
			while(it1.hasNext()){
				DataNodeLocation d = it1.next();
				System.out.println("GenClient read method " + d.ip + " " + d.port);
			}
		}
		*/
		
		// check status of getBlockLocations
		if (blockLocationResponse.status == -1) {
			status = -1;
			return status;
		}

		StringBuilder buffer = new StringBuilder();
		// repeat for each block
		for (int i = 0; i < blockLocationResponse.blockLocations.size(); i++) {
			BlockLocations blockLocation = blockLocationResponse.blockLocations
					.get(i);
			int blockNum = blockLocation.blockNumber;

			// repeat for each dataNodeLocation for that block
			int j;
			for (j = 0; j < blockLocation.locations.size(); j++) {
				DataNodeLocation dataNodeAddress = blockLocation.locations.get(j);
					try {
						Registry myreg = LocateRegistry.getRegistry(ipObj.intToIP(dataNodeAddress.ip),dataNodeAddress.port);
						dataNode = (IDataNode) myreg.lookup("DataNode");
						
						ReadBlockRequest readBlockRequest = new ReadBlockRequest(blockNum);
						byte[] temp;
						temp = dataNode.readBlock(readBlockRequest.toProto());
						ReadBlockResponse readBlockResponse = new ReadBlockResponse(temp);

						if (readBlockResponse.status == 1) {
							String str = new String(readBlockResponse.data);
							buffer.append(str);
							break;
						}
					} catch (Exception e) {
						continue;
					}
			}
			if (j > blockLocation.locations.size()) {
				status = -1;
				return status;
			}
		}

		if(status == 1)
		{
			System.out.println("Data Read Successful \n Content Of data is :: \n" + buffer.toString());
		}
		
		// close file
		int status1 = close(openFileResponse.handle);
		//System.out.println("GenClient Final statuses "+status1 + " " + status);		
		//System.out.println("Close file status = " + status1);

		if(status == -1 || status1 == -1)
			return -1;
		else return 1;
	}

	public int write(String fileName,/*byte [] data , */ String sourceFilePath) throws NotBoundException, UnknownHostException {
		
		int status = 1;
		float dataSize = 0;
		//float dataSize = data.length
		int numOfBlocksReq =0;
		// int numOfBlocksReq = (int) Math.ceil(dataSize / blockSize);
		byte[] openResponse;
		openResponse = open(fileName, false);
		OpenFileRespose openFileResponse = new OpenFileRespose(openResponse);

		// check status of openFile
		if (openFileResponse.status == -1) {
			// openFile Unsuccessful
			System.out.println("GeneralCLient :: Error , Cannot open File : " + fileName);
			status = -1;
			return status;
		}

		INameNode in = null;
		IDataNode dataNode = null;
		IpConversion ipObj = new IpConversion();
		try {
			Registry myreg = LocateRegistry.getRegistry(nameNodeIP,nameNodePort);
			in = (INameNode) myreg.lookup("NameNode");
		} catch (Exception e) {
			status = -1;
			System.out.println("GeneralClient :: RMI Error locating NameNode registry");
			e.printStackTrace();
			return status;
		}

		if (openFileResponse.status == 1){
			System.out.println("GeneralClient :: Error , rewrinting of existing file not allowed");
			status = -1;
			return status;
		}
		else if( openFileResponse.status == 2){
			// create new file in write mode
			//System.out.println("GenClient Write Method status=2");
			int i;
			int offset = 0;
			/**************************************************************************************************/
			RandomAccessFile sourceFile;
			try {
					sourceFile = new RandomAccessFile(sourceFilePath, "r");
					dataSize=sourceFile.length();
					numOfBlocksReq = (int) Math.ceil(dataSize / blockSize);
				
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				status = -1;
				System.out.println("GeneralClient :: Error , cannot open source file");
				e1.printStackTrace();
				return status;
			}
			/****************************************************************************************/
			for (i = 0; i < numOfBlocksReq; i++) {
				try {
					AssignBlockRequest assignBlockRequest = new AssignBlockRequest(openFileResponse.handle);
					byte[] assignBlockRes = in.assignBlock(assignBlockRequest.toProto());

					AssignBlockResponse assignBlockResponse = new AssignBlockResponse(assignBlockRes);

					// assign block fails
					if (assignBlockResponse.status == -1) {
						status = -1;
						System.out.println("GeneralClient :: Error getting new block from NameNode");
						break;
					}
					/*
					 * System.out.println("GenClient assignBlock = " + blockLocations.blockNumber);
					Iterator<DataNodeLocation> it = blockLocations.locations.iterator();
					while(it.hasNext()){
						DataNodeLocation d = it.next();
						System.out.println(d.ip + " " + d.port + " " + d.tstamp);
					}
					*/
					int endOffset;
					endOffset=offset+blockSize;
					if(endOffset>dataSize)
						endOffset=(int) dataSize;
					
					/*************************************************************************************************/
					byte[] buffer = new byte[endOffset];
					try {
						sourceFile.seek(offset);
						sourceFile.read(buffer);
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						status = -1;
						e1.printStackTrace();
						break;
					}
					//byte[] buffer = Arrays.copyOfRange(data,offset,endOffset);
					/*************************************************************************************************/
					
					offset += blockSize;
					
					BlockLocations blockLocations = assignBlockResponse.newBlock;
					WriteBlockRequest writeBlockRequest = new WriteBlockRequest(blockLocations,buffer);
					DataNodeLocation dataNodeAddress = blockLocations.locations.get(0);
					try{
						Registry myreg = LocateRegistry.getRegistry(ipObj.intToIP(dataNodeAddress.ip),dataNodeAddress.port);
						dataNode = (IDataNode) myreg.lookup("DataNode");
						byte[] writeResponse = dataNode.writeBlock(writeBlockRequest.toProto());
						WriteBlockResponse wr = new WriteBlockResponse(writeResponse);
						if(wr.status == -1){
							status = -1;
						}
					}catch(Exception e){
						status = -1;
						System.out.println("GeneralClient :: Error , Writing data block to DataNode.");
						e.printStackTrace();
					}
				} catch (RemoteException e) {
					status = -1;
					System.out.println("GeneralClient :: Error , Writing data block to dataNode.");
					e.printStackTrace();
				}
				if(status == -1){
					break;
				}
			}
		}
		// close file
		int status1 = close(openFileResponse.handle);
		//System.out.println("GenClient Final statuses "+status1 + " " + status);
		
		//System.out.println("Close file status = " + status1);
		
		if(status == -1 || status1 == -1)
			return -1;
		else return 1;
				
	}
	int list(){
		ListFilesRequest listFilesRequest = new ListFilesRequest(".");
		int status = 1;
		INameNode in = null;
		try {
			Registry myreg = LocateRegistry.getRegistry(nameNodeIP,nameNodePort);
			in = (INameNode) myreg.lookup("NameNode");
			byte []fileNames = in.list(listFilesRequest.toProto());
			ListFilesResponse listFilesResponse = new ListFilesResponse(fileNames);
			System.out.println("List of files");
			for(String file:listFilesResponse.fileNames){
				System.out.println(file);
			}
		} catch (Exception e) {
			status = -1;
			e.printStackTrace();
			return status;
		}
		return status;
	}
	public static void main(String[] args) {
		if(args.length != 1)
		{
			System.out.println("Invalid number of parameters");
			System.out.println("Usage java <GeneralClient> <config File Path>");
			System.exit(-1);
		}
		try {
			config(args[0]);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
			
		try {
			GeneralClient client = new GeneralClient();
			String sourceFilePath="/home/shrikant/trim_path.py";
			//String data = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZKedar";
			//int st = client.write("tmp2.txt",data.getBytes() );
			int st = client.write("tmp1.txt",sourceFilePath);
			int st1 = client.read("tmp1.txt");
			System.out.println("Status of read = " + st1  + "status of write = " + st);
			//client.list();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
