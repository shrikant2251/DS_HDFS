package HDFSPackage;
import com.google.protobuf.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import HDFSPackage.RequestResponse.ReadBlockRequest;
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
import HDFSPackage.RequestResponse.ReadBlockResponse;
import HDFSPackage.RequestResponse.WriteBlockRequest;
import HDFSPackage.RequestResponse.WriteBlockResponse;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class DataNode extends UnicastRemoteObject implements IDataNode {

	protected DataNode() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}

	public static int dataNodeID = 1,NameNodeport = 1099,DataNodePort=5000;
	public static String nameNodeIP = "127.0.0.1",dataNodeIP="127.0.0.1";
	public static int heartBeatRate = 5000;
	String directoryName = "blockDirectory";

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
			     case "dataNodeID":
			    	 				DataNode.dataNodeID=Integer.parseInt(array[1]);
			    	 				break;
			     case "dataNodeIP":
 	 					DataNode.dataNodeIP = new  String(array[1]);
 	 					break;
			     case "dataNodePort":
 	 								DataNode.DataNodePort=Integer.parseInt(array[1]);
 	 								break;
			     case "nameNodeIP":
 	 								DataNode.nameNodeIP= new String(array[1]);
 	 								break;
			     case "nameNodePort":
 	 								DataNode.NameNodeport=Integer.parseInt(array[1]);
 	 								break;
			     case "heartBeatRate":
 	 								DataNode.heartBeatRate=Integer.parseInt(array[1]);
 	 								break;
 
			     }
			    }
			}	
		}
		else {
			System.out.println("Config file does not exists please check the location");
			System.exit(0);
		}
	}
	@Override
	public byte[] readBlock(byte[] input) throws RemoteException {
		// TODO Auto-generated method stub
		ReadBlockRequest readBlockRequest = new ReadBlockRequest(input);
		ReadBlockResponse readBlockResponse = new ReadBlockResponse();
		System.out.println("DataNode :: Request for reading block number # " + readBlockRequest.blockNumber);
		try {
			File file = new File(directoryName + "/"
					+ readBlockRequest.blockNumber);
			if (file.exists()) {
				FileInputStream fis = new FileInputStream(file);
				byte[] data = new byte[(int) file.length()];
				fis.read(data);
				readBlockResponse.data = data;
				readBlockResponse.status = 1;
				fis.close();
			} else {
				readBlockResponse.status = -1;
			}
		} catch (IOException e) {
			readBlockResponse.status=-1;
			e.printStackTrace();
		}
		//System.out.println("DataNode :: Response status for reading block # " + readBlockRequest.blockNumber + " is : " +readBlockResponse.status);
		return readBlockResponse.toProto();
	}

	@Override
	public byte[] writeBlock(byte[] input) throws RemoteException {
		// TODO Auto-generated method stub
		// status successful = 1 unsuccessful = -1
		int status = 1;
		WriteBlockRequest writeBlockRequest = new WriteBlockRequest(input);
		WriteBlockResponse writeBlockResponse = new WriteBlockResponse();
		System.out.println("DataNode :: Request for writing block number # " + writeBlockRequest.blockInfo.blockNumber);
		File dir = new File(directoryName);
		if (!dir.exists()) {
			dir.mkdir();
		}
		// write into file
		try {
			FileOutputStream bw = new FileOutputStream(directoryName + "/" + writeBlockRequest.blockInfo.blockNumber);
			bw.write(writeBlockRequest.data);
			bw.flush();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			status = -1;
			e.printStackTrace();
		}
		// make entry of block number in datanode
		try {
			String meta = "metadata";
			File fis = new File(meta);
			FileWriter fw = null;
			boolean flag = true;
			if (!fis.exists()) {
				flag = false;
				fw = new FileWriter(meta, false);
			} else {
				fw = new FileWriter(meta, true);
			}
			BufferedWriter bw = new BufferedWriter(fw);
			if (flag) {
				String data = "," + writeBlockRequest.blockInfo.blockNumber;
				bw.write(data);
			} else {
				String data = "" + writeBlockRequest.blockInfo.blockNumber;
				bw.append(data);
			}
			bw.flush();
			bw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/* Take data Node information from blockInfo Chaining call */
		writeBlockResponse.status = status;
		writeBlockRequest.blockInfo.locations.remove(0);
	
		if(writeBlockRequest.blockInfo.locations.size()>0){
			DataNodeLocation dataNodeAddress = writeBlockRequest.blockInfo.locations.get(0);
			IDataNode dataNode = null;
			IpConversion ipObj = new IpConversion();
			
			try{
				Registry myreg = LocateRegistry.getRegistry(ipObj.intToIP(dataNodeAddress.ip),dataNodeAddress.port);
				dataNode = (IDataNode) myreg.lookup("DataNode");
				byte []wr = dataNode.writeBlock(writeBlockRequest.toProto());
				WriteBlockResponse res = new WriteBlockResponse(wr);
				
				if(res.status ==-1 && writeBlockResponse.status == -1){
					writeBlockResponse.status = -1;
				}
				else
				{
					writeBlockResponse.status = 1;
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		//System.out.println("DataNode :: Response status for reading block # " + writeBlockRequest.blockInfo.blockNumber + " is : " + writeBlockResponse.status);
		return writeBlockResponse.toProto();
	}

	public static void main(String[] args) {
		if(args.length != 1)
		{
			System.out.println("Invalid number of parameters");
			System.out.println("Usage java <DataNode> <config File Path>");
			System.exit(-1);
		}
		try {
			config(args[0]);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			
			Registry reg = LocateRegistry.createRegistry(DataNodePort);
			DataNode obj = new DataNode();
			reg.rebind("DataNode", obj);
			System.out.println("DataNode server is running");
		} catch (Exception e) {
			System.out.println("DataNode :: Error While creating RMI registry.");
			e.printStackTrace();
		}
		Registry myreg;
		INameNode in = null;
		try {
			//System.out.println("NameNode IP : "+nameNodeIP);
			myreg = LocateRegistry.getRegistry(nameNodeIP, NameNodeport);
			in = (INameNode) myreg.lookup("NameNode");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("DataNode :: Error While looking up for NameNode RMI registry. NameNode IP is :: " + nameNodeIP);
			e.printStackTrace();
		}
		TimerTask perodicService = new PerodicService(in);
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(perodicService, 0, heartBeatRate);
	}

}

class PerodicService extends TimerTask {
	INameNode in;

	public PerodicService(INameNode in) {
		super();
		this.in = in;
	}
	IpConversion ipObj = new IpConversion();
	byte[] generateBlockReport() throws UnknownHostException {
		ArrayList<Integer> blockList = new ArrayList<Integer>();
		int ip = ipObj.packIP(Inet4Address.getByName(DataNode.dataNodeIP).getAddress());
		//int ip = ipObj.packIP(DataNode.dataNodeIP.getBytes());
		
		File file = new File("metadata");
		if (file.exists()) {
			FileInputStream fis;
			try {
				fis = new FileInputStream(file);
				BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
				String line = reader.readLine();
				fis.close();
				String[] blockNums = line.split(",");

				for (String num : blockNums) {
					blockList.add(Integer.parseInt(num));
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		DataNodeLocation dataNodeLocation = new DataNodeLocation(ip,DataNode.DataNodePort);
		BlockReportRequest blockReportRequest = new BlockReportRequest(	DataNode.dataNodeID, dataNodeLocation, blockList);
		return blockReportRequest.toProto();
	}

	byte[] generateDataNodeAddress() {
		HeartBeatRequest heartBeatRequest = new HeartBeatRequest(
				DataNode.dataNodeID);
		return heartBeatRequest.toProto();
	}

	@Override
	public void run() {
		try {
			//Remember to remove this comment
			//System.out.println("DataNode :: Generate Heart beat and block report.");
			in.blockReport(generateBlockReport());
			in.heartBeat(generateDataNodeAddress());
		} catch (RemoteException | UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
