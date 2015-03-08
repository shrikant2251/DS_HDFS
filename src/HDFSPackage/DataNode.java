package HDFSPackage;

import java.io.*;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

import HDFSPackage.RequestResponse.*;

import java.net.Inet4Address;
import java.net.UnknownHostException;
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
	public static String nameNodeIP = "127.0.0.1";
	public static int heartBeatRate = 5000;
	String directoryName = "blockDirectory";

	@Override
	public byte[] readBlock(byte[] input) throws RemoteException {
		// TODO Auto-generated method stub
		ReadBlockRequest readBlockRequest = new ReadBlockRequest(input);
		ReadBlockResponse readBlockResponse = new ReadBlockResponse();
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
			e.printStackTrace();
		}
		// readBlockRequest.blockNumber;
		return readBlockResponse.toProto();
	}

	@Override
	public byte[] writeBlock(byte[] input) throws RemoteException {
		// TODO Auto-generated method stub
		// status successful = 1 unsuccessful = -1
		System.out.println("DataNode writeBlock");
		int status = 1;
		WriteBlockRequest writeBlockRequest = new WriteBlockRequest(input);
		WriteBlockResponse writeBlockResponse = new WriteBlockResponse();
		File dir = new File(directoryName);
		if (!dir.exists()) {
			dir.mkdir();
		}
		// write into file
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(directoryName
					+ "/" + writeBlockRequest.blockInfo.blockNumber));
			bw.append(writeBlockRequest.data.toString());
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
			//
			bw.flush();
			bw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/* Take data Node information from blockInfo Chaining call */
		writeBlockResponse.status = status;
		return writeBlockResponse.toProto();
	}

	public static void main(String[] args) {
		try {
			Registry reg = LocateRegistry.createRegistry(DataNodePort);
			NameNode obj = new NameNode();
			reg.rebind("DataNode", obj);
			System.out.println("DataNode server is running");
		} catch (Exception e) {
			e.printStackTrace();
		}
		Registry myreg;
		INameNode in = null;
		try {
			myreg = LocateRegistry.getRegistry(nameNodeIP, NameNodeport);
			in = (INameNode) myreg.lookup("NameNode");
		} catch (Exception e) {
			// TODO Auto-generated catch block
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
		int ip = ipObj.packIP(Inet4Address.getLocalHost().getHostAddress().getBytes());
		File file = new File("metadata");
		if (file.exists()) {
			FileInputStream fis;
			try {
				fis = new FileInputStream(file);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(fis));
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
			in.blockReport(generateBlockReport());
			in.heartBeat(generateDataNodeAddress());
			//System.out.println("DataNode alive !!!");
		} catch (RemoteException | UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
