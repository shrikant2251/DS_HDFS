package HDFSPackage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import HDFSPackage.RequestResponse.AssignBlockRequest;
import HDFSPackage.RequestResponse.AssignBlockResponse;
import HDFSPackage.RequestResponse.BlockLocationRequest;
import HDFSPackage.RequestResponse.BlockLocations;
import HDFSPackage.RequestResponse.BlockReportRequest;
import HDFSPackage.RequestResponse.BlockReportResponse;
import HDFSPackage.RequestResponse.CloseFileRequest;
import HDFSPackage.RequestResponse.CloseFileResponse;
import HDFSPackage.RequestResponse.DataNodeLocation;
import HDFSPackage.RequestResponse.HeartBeatRequest;
import HDFSPackage.RequestResponse.HeartBeatResponse;
import HDFSPackage.RequestResponse.ListFilesRequest;
import HDFSPackage.RequestResponse.ListFilesResponse;
import HDFSPackage.RequestResponse.OpenFileRequest;
import HDFSPackage.RequestResponse.OpenFileRespose;
import com.google.protobuf.*;
public class NameNode extends UnicastRemoteObject implements INameNode {
	
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
 	 								AllDataStructures.nameNodeIP= new String(array[1]);
 	 								break;
			     case "nameNodePort":
 	 								AllDataStructures.nameNodePort=Integer.parseInt(array[1]);
 	 								break;
			     case "threshold":
 	 								AllDataStructures.thresholdTime=Integer.parseInt(array[1]);
 	 								break;
			     case "blockSize":
									AllDataStructures.blockSize=Integer.parseInt(array[1]);
									break;
			     case "replicationFactor":
									AllDataStructures.replicationFactor=Integer.parseInt(array[1]);
									break;
	
			     }
			    }
			}	
		}else {
			System.out.println("Config file does not exists please check the location");
			System.exit(0);
		}
	}

	protected NameNode() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}

	/* Store the fileName and corresponding list of blocks */
	// HashMap<String,ArrayList<Integer> > fileNameToBlockNum = new
	// HashMap<String,ArrayList<Integer>>();
	// HashMap<Integer, String> fileHandleToFileName = new HashMap<Integer,
	// String>();
	// static HashMap<Integer, ArrayList<RequestResponse.DataNodeLocation>>
	// blocNumToDataNodeLoc = new
	// HashMap<Integer,ArrayList<RequestResponse.DataNodeLocation>>();
	// HashMap<Integer,DataNodeLocation> idToDataNode = new HashMap<Integer,
	// DataNodeLocation>();
	// static int fileHande = 0;
	@Override
	public byte[] openFile(byte[] input) throws RemoteException {
		// TODO Auto-generated method stub
		OpenFileRequest openFileRequest = new OpenFileRequest(input);
		OpenFileRespose openFileResponse = new OpenFileRespose();
		if (openFileRequest.forRead) {/*
									 * read request for File(check proto file
									 * for more details)
									 */
			System.out.println("NameNode :: request to open file (for read) :: " + openFileRequest.fileName);
			if (AllDataStructures.fileNameToBlockNum
					.containsKey(openFileRequest.fileName)) {
				/*
				 * File exists write operations done and file contains some
				 * blocks
				 */
				AllDataStructures.fileHandel++;
				openFileResponse.status = 1;// read success
				openFileResponse.handle = AllDataStructures.fileHandel; // used to close the file;
				openFileResponse.blockNums = (ArrayList<Integer>) AllDataStructures.fileNameToBlockNum
						.get(openFileRequest.fileName);
			} else {/* File does not exist */
				openFileResponse.status = -1; // file does not exist error in
												// opening file
				openFileResponse.handle = -1;
				//openFileResponse.blockNums = new ArrayList<Integer>();
				//openFileResponse.blockNums.add(-1);
			}
		} else {/* write request for File(check proto file for more details) */
			
			System.out.println("NameNode :: request to open file (for write) :: " + openFileRequest.fileName);
			AllDataStructures.fileHandel++;
			AllDataStructures.fileHandleToFileName.put(AllDataStructures.fileHandel, openFileRequest.fileName);
			openFileResponse.handle = AllDataStructures.fileHandel;
			/* Write operation NameNode will */
			if (AllDataStructures.fileNameToBlockNum.containsKey(openFileRequest.fileName)) {
				openFileResponse.status = 1;
				openFileResponse.blockNums = (ArrayList<Integer>) AllDataStructures.fileNameToBlockNum
						.get(openFileRequest.fileName);
			} else {
				//System.out.println("NameNode openFile method write new file created");
				ArrayList<Integer> blocks = new ArrayList<Integer>();
				AllDataStructures.fileNameToBlockNum.put(openFileRequest.fileName, blocks);
				openFileResponse.status = 2;
				//openFileResponse.blockNums = new ArrayList<Integer>();
				//openFileResponse.blockNums.add(-1);
			}
		}
		return openFileResponse.toProto();
	}

	@Override
	public byte[] closeFile(byte[] input) throws RemoteException {
		// TODO Auto-generated method stub
		CloseFileRequest closeFileRequest = new CloseFileRequest(input);
		CloseFileResponse closeFileResponse = new CloseFileResponse();
		
		System.out.println("NameNode :: request to close file handle :: " + closeFileRequest.handle);
		if (AllDataStructures.fileHandleToFileName.containsKey(closeFileRequest.handle)) {
			AllDataStructures.fileHandleToFileName.remove(closeFileRequest.handle);
			closeFileResponse.status = 1;
		} else {
			closeFileResponse.status = -1;
		}
		return closeFileResponse.toProto();
	}

	@Override
	public byte[] getBlockLocations(byte[] input) throws RemoteException {
		// TODO Auto-generated method stub
		// status successful = 1 unsuccessful = -1;
		int status = 1;
		BlockLocationRequest blockLocationRequest = new BlockLocationRequest(input); // parse the request
		System.out.println("NameNode :: request to get location of DataNode for blocks ( " + blockLocationRequest.blockNums + " )");
		/*
		 * for(int i=0;i<blockLocationRequest.blockNums.size();i++)
			System.out.println("NameNode getBlockLocs ####### "  + " " + blockLocationRequest.blockNums.get(i));
		System.out.println("NameNode getBlockLocation method size of blockNums = " + blockLocationRequest.blockNums.size());
		*/
		
		// list of block location which contain block number and list of DataNodeLocation
		ArrayList<RequestResponse.BlockLocations> locationList = new ArrayList<RequestResponse.BlockLocations>();
		
		// iterate through each block number and add DataNodeLocation to list
		for (int i = 0; i < blockLocationRequest.blockNums.size(); i++) {
			int blknm = blockLocationRequest.blockNums.get(i);
			//System.out.println("NameNode getBlockLocation Method block no " + blknm);
			RequestResponse.BlockLocations blockLocation;

			// check block number is available in hashmap or not
			if (AllDataStructures.blocNumToDataNodeLoc.containsKey(blknm))
				blockLocation = new RequestResponse.BlockLocations(blknm,AllDataStructures.blocNumToDataNodeLoc.get(blknm));
			else {
				status = -1;
				blockLocation = new RequestResponse.BlockLocations(blknm,new ArrayList<DataNodeLocation>());
				break;
			}
			// add current block number location list to final list
			locationList.add(blockLocation);
		}
		RequestResponse.BlockLocationResponse listLoc = new RequestResponse.BlockLocationResponse(status, locationList);
		return listLoc.toProto();
	}

	@Override
	public byte[] assignBlock(byte[] blockLocations) throws RemoteException {
		// TODO Auto-generated method stub
		// status successful = 1 unsuccessful =-1
		int status = 1, count = 0;
		
		AssignBlockRequest assignBlockRequest = new AssignBlockRequest(blockLocations);
		AssignBlockResponse assignBlockResponse = new AssignBlockResponse();
		ArrayList<DataNodeLocation> node = new ArrayList<DataNodeLocation>();

		System.out.println("NameNode :: Assign block ( blockNumber , DataNodeList) for file handle :: " + assignBlockRequest.handle);
		int size = AllDataStructures.idToDataNode.size();
		Random randomGen = new Random();
		// randomly add replicationFactor DataNodeLocations to node list
		Set<Integer> idSet = AllDataStructures.idToDataNode.keySet();
		int [] idArray = new int[idSet.size()];
		int index = 0;
		for( int id : idSet){
			idArray[index++]=id;
		}
		while (node.size() < AllDataStructures.replicationFactor && count < size) {
			int randomIndex = randomGen.nextInt(size);
			int random=idArray[randomIndex];
			//System.out.println("NameNode AssignBlock Method random = " + random);
			//System.out.println(AllDataStructures.idToDataNode.get(random));
			if (!node.contains(AllDataStructures.idToDataNode.get(random))&& AllDataStructures.idToDataNode.get(random).tstamp >= System.currentTimeMillis() - AllDataStructures.thresholdTime) {
				node.add(AllDataStructures.idToDataNode.get(random));
			}
			count++;
			if (count > size) {
				status = -1;
				break;
			}
		}

		if (status == 1	&& AllDataStructures.fileHandleToFileName.containsKey(assignBlockRequest.handle)) {

			// increase overall block number
			AllDataStructures.blockNumber++;

			ArrayList<Integer> blockList = new ArrayList<Integer>();
			String file = AllDataStructures.fileHandleToFileName.get(assignBlockRequest.handle);

			// if that file already present means it already has blocks
			if (AllDataStructures.fileNameToBlockNum.containsKey(file)) {
				blockList = AllDataStructures.fileNameToBlockNum.get(file);
			}
			blockList.add(AllDataStructures.blockNumber);
			AllDataStructures.fileNameToBlockNum.put(file, blockList);
			AllDataStructures.blocNumToDataNodeLoc.put(AllDataStructures.blockNumber,node);
			try {
				FileWriter fw = new FileWriter(file, true);
				BufferedWriter bw = new BufferedWriter(fw);
				String data = Integer.toString(AllDataStructures.blockNumber)+ ",";
				bw.append(data);
				bw.flush();
				bw.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		assignBlockResponse.newBlock.blockNumber = AllDataStructures.blockNumber;
		assignBlockResponse.newBlock.locations = node;
		assignBlockResponse.status = status;

		return assignBlockResponse.toProto();
	}

	@Override
	public byte[] list(byte[] input) throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("NameNode :: Request for list all files");
		ListFilesRequest listFileRequest = new ListFilesRequest(input);
		ListFilesResponse listFilesResponse = new ListFilesResponse();
		if (!AllDataStructures.fileNameToBlockNum.isEmpty()) {// There are some
																// files in HDFS
			Iterator<String> it = AllDataStructures.fileNameToBlockNum.keySet().iterator();
			while (it.hasNext()) {
				listFilesResponse.fileNames.add(it.next());
			}
			listFilesResponse.status = 1;
		} else {// There are no files in HDFS
			listFilesResponse.fileNames.add("-1");
			listFilesResponse.status = -1;
		}
		return listFilesResponse.toProto();
	}

	@Override
	public byte[] blockReport(byte[] input) throws RemoteException {
		// TODO Auto-generated method stub
		BlockReportRequest blockReportRequest = new BlockReportRequest(input);
		BlockReportResponse blockReportResponse = new BlockReportResponse();
		
		System.out.println("NameNode :: block report receive from DataNode ID " + blockReportRequest.id);
	//	System.out.println("NameNode blockReport id = " + blockReportRequest.id);
		if(!AllDataStructures.idToDataNode.containsKey(blockReportRequest.id)){
			AllDataStructures.idToDataNode.put(blockReportRequest.id, blockReportRequest.location);
		}
		ArrayList<DataNodeLocation> dataNode;
		for (int block : blockReportRequest.blockNumbers) {
			if (!AllDataStructures.blocNumToDataNodeLoc.containsKey(block)) {
				dataNode = new ArrayList<DataNodeLocation>();
				dataNode.add(blockReportRequest.location);
				AllDataStructures.blocNumToDataNodeLoc.put(block, dataNode);
			} else {
				dataNode = AllDataStructures.blocNumToDataNodeLoc.get(block);
				if (!dataNode.contains(blockReportRequest.location)) {
					dataNode.add(blockReportRequest.location);
				}
			}
			blockReportResponse.status.add(1);
		}
		return blockReportResponse.toProto();
	}

	@Override
	public byte[] heartBeat(byte[] input) throws RemoteException {
		// TODO Auto-generated method stub
		HeartBeatRequest heartBeatRequest = new HeartBeatRequest(input);
		HeartBeatResponse hearBeatResponse = new HeartBeatResponse();
		
		System.out.println("NameNode :: Heartbeat received from DataNode ID :: " + heartBeatRequest.id);

		if (AllDataStructures.idToDataNode.containsKey(heartBeatRequest.id)) {
	//		Date date = new Date();
			//System.out.println("NameNode connected to Datanode id : " + heartBeatRequest.id);
			AllDataStructures.idToDataNode.get(heartBeatRequest.id).tstamp = System.currentTimeMillis();
			hearBeatResponse.status = 1;
		} else {
			hearBeatResponse.status = -1;
		}
		return hearBeatResponse.toProto();
	}

	public long getTimeStamp(int id) throws RemoteException {
		if(AllDataStructures.idToDataNode.containsKey(id))
			return AllDataStructures.idToDataNode.get(id).tstamp;
		else
			return 0;
	}
	public void test() throws RemoteException {
		System.out.println("test() Method NameNode success");
	}

	public static void main(String[] args) {
		if(args.length != 1)
		{
			System.out.println("Invalid number of parameters");
			System.out.println("Usage java <NameNode> <config File Path>");
			System.exit(-1);
		}
		try {
			config(args[0]);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			//System.setProperty( "java.rmi.server.hostname", AllDataStructures.nameNodeIP ) ;
			Registry reg = LocateRegistry.createRegistry(AllDataStructures.nameNodePort);
			NameNode obj = new NameNode();
			reg.rebind("NameNode", obj);
			System.out.println("NameNode server is running");
		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println("Hello");
	}
}
