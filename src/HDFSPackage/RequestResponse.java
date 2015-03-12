package HDFSPackage;
import java.sql.Timestamp;
import java.util.ArrayList;


import HDFSPackage.Hdfs.DataNodeLocation;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.*;
public interface RequestResponse {
/****************************************************************/
	public class BlockReportRequest{
		public int id; //identity of the DataNode. NameNode uses this ID for getting blocks
		public DataNodeLocation location;
		ArrayList<Integer> blockNumbers;
		public BlockReportRequest(int _id,DataNodeLocation loc,ArrayList<Integer> blocks){
			id = _id;
			location = loc;
			blockNumbers = new ArrayList<Integer>();
			for(int i:blocks)
			blockNumbers.add(i);// = blocks;
			//System.out.println("ReqResp blockReportRequest Method id=" + _id + " " + loc + " " + blocks);
		}
		public BlockReportRequest(byte []input){
			Hdfs.BlockReportRequest builder = null;
			try{
				builder = Hdfs.BlockReportRequest.parseFrom(input);
			}
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			id = builder.getId();
			blockNumbers = new ArrayList<Integer>();
			for(int i: builder.getBlockNumbersList())
				blockNumbers.add(i);
			location = new DataNodeLocation(builder.getLocation());
		}
		public byte[] toProto(){
			Hdfs.BlockReportRequest.Builder builder = Hdfs.BlockReportRequest.newBuilder();
		//	System.out.println("BlockReportRequest toProto method id = " + id + " " + location + " " + blockNumbers);
			builder.setId(id);
			builder.setLocation(location.toProtoObject());
			for(int i:blockNumbers)
				builder.addBlockNumbers(i);
			return builder.build().toByteArray();
		}
	}
/****************************************************************/	
	public class BlockReportResponse{
		ArrayList<Integer> status;
		public BlockReportResponse(){
			status = new ArrayList<Integer>();
		}
		public BlockReportResponse(ArrayList<Integer> s){
			status = s;	
		}
		public BlockReportResponse(byte []input){
			Hdfs.BlockReportResponse builder = null;
			try{
				builder = Hdfs.BlockReportResponse.parseFrom(input);
			}
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			status = new ArrayList<Integer>();
			for(int i:builder.getStatusList())
				status.add(i);
		}
		public byte[] toProto(){
			Hdfs.BlockReportResponse.Builder builder = Hdfs.BlockReportResponse.newBuilder();
			for(int i:status)
				builder.addStatus(i);
			return builder.build().toByteArray();
		}
		
	}
/****************************************************************/
	public class DataNodeLocation {
		public int ip;
		public int port;
		public long tstamp;
		public DataNodeLocation(byte[] input) {
			try {
				 Hdfs.DataNodeLocation location = Hdfs.DataNodeLocation.parseFrom(input);
				 ip = location.getIp();
				 port = location.getPort();
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
		}
		public Hdfs.DataNodeLocation.Builder toProtoObject() {
			Hdfs.DataNodeLocation.Builder locationBuilder = Hdfs.DataNodeLocation.newBuilder();
			locationBuilder.setIp(ip);
			locationBuilder.setPort(port);
			return locationBuilder;
		}
		public DataNodeLocation(Hdfs.DataNodeLocation location) {
			ip = location.getIp();
			port = location.getPort();
		}
		public DataNodeLocation(int _ip, int _port) {
			ip=_ip;
			port = _port;
		}
		public byte[] toProto() {
			Hdfs.DataNodeLocation.Builder builder = Hdfs.DataNodeLocation.newBuilder();
			builder.setIp(ip);	
			builder.setPort(port);			
			return builder.build().toByteArray();
		}
	}
/****************************************************************/
	public class HeartBeatRequest {
		public int id;
		public HeartBeatRequest(byte[] input) {
			Hdfs.HeartBeatRequest builder = null;
			try {
				builder = Hdfs.HeartBeatRequest.parseFrom(input);
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			id =  builder.getId();
		}
		public HeartBeatRequest(int i) {
			id = i;
		}
		public byte[] toProto() {
			Hdfs.HeartBeatRequest.Builder builder = Hdfs.HeartBeatRequest.newBuilder();
			builder.setId(id);	
			return builder.build().toByteArray();
		}
	}
/****************************************************************/
	public class HeartBeatResponse {
		public int status;
		public HeartBeatResponse(int s){
			status = s;
		}
		public HeartBeatResponse(byte[] input) {
			Hdfs.HeartBeatResponse builder = null;
			try {
				builder= Hdfs.HeartBeatResponse.parseFrom(input);
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			status =  builder.getStatus();
		}
		public HeartBeatResponse() {

		}
		public byte[] toProto() {
			Hdfs.HeartBeatResponse.Builder builder = Hdfs.HeartBeatResponse.newBuilder();
			builder.setStatus(status);	
			return builder.build().toByteArray();
		}
	}
/*****************************************************************/
	public class WriteBlockRequest {
		public BlockLocations blockInfo;
		public byte data[];
		
		public WriteBlockRequest(byte[] input) {
			Hdfs.WriteBlockRequest builder = null;
			try {
				builder= Hdfs.WriteBlockRequest.parseFrom(input);
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			blockInfo = new BlockLocations(builder.getBlockInfo());
			int indx = 0;
			data = new byte[builder.getDataList().get(0).size()];
			for(ByteString bs : builder.getDataList()){
				byte[] tmp = bs.toByteArray();
				for(byte b : tmp){
					data[indx++] = b;
				}
			}
		}

		public WriteBlockRequest(BlockLocations _blockLocations,byte _data[]) {
			blockInfo =_blockLocations;
			data = _data;
		}

		public byte[] toProto() {
			Hdfs.WriteBlockRequest.Builder builder = Hdfs.WriteBlockRequest.newBuilder();
			builder.setBlockInfo(blockInfo.toProtoObject());
			builder.addData(ByteString.copyFrom(data));
			return builder.build().toByteArray();
		}
	}
/*******************************************************************************************/
	public class WriteBlockResponse {
		public int status;
		public WriteBlockResponse(){
		}
		public WriteBlockResponse(int s){
			status = s;
		}
		public WriteBlockResponse(byte[] input) {
			Hdfs.WriteBlockResponse builder = null;
			try{
				builder = Hdfs.WriteBlockResponse.parseFrom(input);
			} 
			catch(InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			status = builder.getStatus();
		}
		public byte[] toProto(){
			Hdfs.WriteBlockResponse.Builder builder = Hdfs.WriteBlockResponse.newBuilder();
			builder.setStatus(status);	
			return builder.build().toByteArray();
		}
	}
/*******************************************************************************************/
	public class CloseFileRequest {
		public int handle;

		public CloseFileRequest(int h){
			handle = h;
		}
		public CloseFileRequest(byte[] input){
			Hdfs.CloseFileRequest builder = null;
			try{
				builder = Hdfs.CloseFileRequest.parseFrom(input);
			} 
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			handle = builder.getHandle();
		}
		public byte[] toProto() {
			Hdfs.CloseFileRequest.Builder builder = Hdfs.CloseFileRequest.newBuilder();
			builder.setHandle(handle);	
			return builder.build().toByteArray();
		}
	}
/*******************************************************************************************/
	public class CloseFileResponse {
		public int status;
		
		public CloseFileResponse(int s){
			status = s;
		}
		public CloseFileResponse(byte[] input){
			Hdfs.CloseFileResponse builder = null;
			try{
				builder= Hdfs.CloseFileResponse.parseFrom(input);
			} 
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			status = builder.getStatus();
		}

		public CloseFileResponse(){
		}
		public byte[] toProto(){
			Hdfs.CloseFileResponse.Builder builder = Hdfs.CloseFileResponse.newBuilder();
			builder.setStatus(status);
			return builder.build().toByteArray();
		}
	}
/*******************************************************************************************/
	public class BlockLocations {
		public int blockNumber;
		public ArrayList<DataNodeLocation> locations;

		public BlockLocations(int blockNum, ArrayList<DataNodeLocation> loc){
			blockNumber = blockNum;
			locations = new ArrayList<RequestResponse.DataNodeLocation>();
			for(DataNodeLocation dnl : loc)
			{
				locations.add(dnl);
			}
		}
		public BlockLocations(byte[] input) {
			Hdfs.BlockLocations builder = null;
			try {
				builder= Hdfs.BlockLocations.parseFrom(input);
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			blockNumber = builder.getBlockNumber();
			locations = new ArrayList<RequestResponse.DataNodeLocation>();
			for(Hdfs.DataNodeLocation bl : builder.getLocationsList()){
				locations.add(new DataNodeLocation(bl));
			}
		}

		public BlockLocations(Hdfs.BlockLocations builder){
			blockNumber = builder.getBlockNumber();
			locations = new ArrayList<RequestResponse.DataNodeLocation>();
			for(Hdfs.DataNodeLocation bl : builder.getLocationsList()){
				locations.add(new DataNodeLocation(bl));
			}
		}
		
		public BlockLocations() {
			locations = new ArrayList<RequestResponse.DataNodeLocation>();
			// TODO Auto-generated constructor stub
		}
		public Hdfs.BlockLocations.Builder toProtoObject() {
			Hdfs.BlockLocations.Builder builder = Hdfs.BlockLocations.newBuilder();
			builder.setBlockNumber(blockNumber);
			for(DataNodeLocation location : locations){
				builder.addLocations(location.toProtoObject());
			}
			return builder;
		}

		public byte[] toProto() {
			Hdfs.BlockLocations.Builder builder = Hdfs.BlockLocations.newBuilder();
			builder.setBlockNumber(blockNumber);
			for(DataNodeLocation location : locations){
				builder.addLocations(location.toProtoObject());
			}
			return builder.build().toByteArray();
		}
	}
/*******************************************************************************/
	public class BlockLocationRequest {
		ArrayList<Integer> blockNums;

		public BlockLocationRequest(ArrayList<Integer> blocks) {
			blockNums = new ArrayList<Integer>();
			//System.out.println(" RequestResponse BlockLocationRequest Block size = " + blocks.size());
			for( int i: blocks)
			{
				//System.out.println(" RequestResponse BlockLocationRequest num = " + i);
				blockNums.add(i);
			}
		}

		public BlockLocationRequest(byte[] input){
			Hdfs.BlockLocationRequest builder = null;
			try {
				//builder = Hdfs.BlockReportRequest.parseFrom(input);
				builder = Hdfs.BlockLocationRequest.parseFrom(input);
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			blockNums = new ArrayList<Integer>();
			//System.out.println("RequestResponse BlockLocationRequest(byte [] input) blocknum size = " + builder.getBlockNumsCount());
			for(int i:builder.getBlockNumsList())
			{
				//System.out.println("RequestResponse BlockLocationRequest(byte [] input) block num = " + i);
				blockNums.add(i);
			}
		}

		public byte[] toProto(){
			Hdfs.BlockLocationRequest.Builder builder = Hdfs.BlockLocationRequest.newBuilder();
			/*for(int i=0;i<blockNums.size();i++){
				System.out.println("%%%%%% BlockLocationRequest block num = " + blockNums.get(i));
			}*/
			for (int i:blockNums)
				builder.addBlockNums(i);
			//System.out.println(" RequestResponse BlockLocationRequest toProto() **** addblock size " + builder.getBlockNumsCount() );
			//System.out.println("RequestResponse BlockLocationRequest toProto() blocknum size = " + builder.getBlockNumsCount());
			return builder.build().toByteArray();
		}
	}
/****************************************************************/
	public class BlockLocationResponse {
		public int status;
		public ArrayList<BlockLocations> blockLocations;
		public BlockLocationResponse() {
			blockLocations = new ArrayList<BlockLocations>();
		}
		public BlockLocationResponse(int st, ArrayList<BlockLocations> blocks){
			blockLocations = new ArrayList<BlockLocations>();
			for(BlockLocations b: blocks){
				blockLocations.add(b);
			}
			status = st;
		}
		public BlockLocationResponse(byte[] input){
			Hdfs.BlockLocationResponse builder = null;
			try{
				builder= Hdfs.BlockLocationResponse.parseFrom(input);
			} 
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			status = builder.getStatus();
			blockLocations = new ArrayList<RequestResponse.BlockLocations>();
			for(Hdfs.BlockLocations b: builder.getBlockLocationsList()){
				blockLocations.add(new BlockLocations(b));
			}
		}
		public byte[] toProto() {
			Hdfs.BlockLocationResponse.Builder builder = Hdfs.BlockLocationResponse.newBuilder();
			builder.setStatus(status);
			for(BlockLocations block_Locations : blockLocations){
				builder.addBlockLocations(block_Locations.toProtoObject());
			}
			return builder.build().toByteArray();
		}
	}
/****************************************************************/
	public class AssignBlockRequest {
		public int handle;
		public AssignBlockRequest(int hand){
			handle = hand;
		}
		public AssignBlockRequest(byte[] input) {
			Hdfs.AssignBlockRequest builder = null;
			try {
				builder = Hdfs.AssignBlockRequest.parseFrom(input);
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			handle = builder.getHandle();
		}
		public byte[] toProto() {
			Hdfs.AssignBlockRequest.Builder builder = Hdfs.AssignBlockRequest.newBuilder();
			builder.setHandle(handle);	
			return builder.build().toByteArray();
		}
	}
/****************************************************************/
	public class AssignBlockResponse {
		public int status;
		public BlockLocations newBlock;
		public AssignBlockResponse() {
			newBlock = new BlockLocations();
		}
		public AssignBlockResponse(int st, BlockLocations blockLoc){
			status = st;
			newBlock = blockLoc;
		}
		public AssignBlockResponse(byte[] input) {
			Hdfs.AssignBlockResponse builder = null;
			try {
				builder= Hdfs.AssignBlockResponse.parseFrom(input);
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			status = builder.getStatus();
			newBlock = new BlockLocations(builder.getNewBlock());
		}
		public byte[] toProto(){
			Hdfs.AssignBlockResponse.Builder builder = Hdfs.AssignBlockResponse.newBuilder();
			builder.setStatus(status);
			builder.setNewBlock(newBlock.toProtoObject());
			return builder.build().toByteArray();
		}
	}

/****************************************************************/
	public class ListFilesRequest {
		public String dirName;
		public ListFilesRequest(String dir){
			dirName = dir;
		}
		public ListFilesRequest(byte[] input) {
			Hdfs.ListFilesRequest builder = null;
			try {
				builder = Hdfs.ListFilesRequest.parseFrom(input);
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			dirName = builder.getDirName();
		}
		public byte[] toProto() {
			Hdfs.ListFilesRequest.Builder builder = Hdfs.ListFilesRequest.newBuilder();
			builder.setDirName(dirName);	
			return builder.build().toByteArray();
		}
	}
/****************************************************************/
	public class ListFilesResponse {
		public int status;
		public ArrayList<String> fileNames;
		public ListFilesResponse() {
			fileNames = new ArrayList<String>();
		}
		public ListFilesResponse(int st, ArrayList<String> files){
			status = st;
			fileNames = new ArrayList<String>();
			for(String i:files)
				fileNames.add(i);
		}		
		public ListFilesResponse(byte[] input){
			Hdfs.ListFilesResponse builder = null;
			try{
				builder = Hdfs.ListFilesResponse.parseFrom(input);
			} 
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			status = builder.getStatus();
			fileNames = new ArrayList<String>();
			for(String s : builder.getFileNamesList())
				fileNames.add(s);
		}
		public byte[] toProto() {
			Hdfs.ListFilesResponse.Builder builder = Hdfs.ListFilesResponse.newBuilder();
			builder.setStatus(status);	
			for (String fileName : fileNames)
				builder.addFileNames(fileName);
			return builder.build().toByteArray();
		}
	}

/****************************************************************/
	public class OpenFileRequest{
			public String fileName;
			public boolean forRead;
			public OpenFileRequest(String fName,boolean fRead){
				fileName = fName;
				forRead = fRead;
			}
			
			/*Constructor byte array input unmarshall*/
			public OpenFileRequest(byte[] input){
				Hdfs.OpenFileRequest builder = null;
				try{
					builder = Hdfs.OpenFileRequest.parseFrom(input);
				}
				catch(InvalidProtocolBufferException e){
					e.printStackTrace();
				}
				fileName = builder.getFileName();
				forRead = builder.getForRead();
			}
			
			/*return byte array marshalling*/
			public byte[] toProto(){
				Hdfs.OpenFileRequest.Builder builder = Hdfs.OpenFileRequest.newBuilder();
				builder.setFileName(fileName);
				builder.setForRead(forRead);
				return builder.build().toByteArray();
			}
		}
/****************************************************************/
		public class OpenFileRespose{
			public int status,handle;
			public ArrayList<Integer> blockNums;
			public OpenFileRespose(){
				blockNums = new ArrayList<Integer>();
			}
			public OpenFileRespose(int s,int h,ArrayList<Integer> blcks){
				status = s;
				handle = h;
				blockNums = blcks;
			}
			public OpenFileRespose(byte[] input){
				Hdfs.OpenFileResponse builder = null;
				try{
					builder = Hdfs.OpenFileResponse.parseFrom(input);
					status = builder.getStatus();
					handle = builder.getHandle();
					blockNums = new ArrayList<Integer>();
					for(int item:builder.getBlockNumsList()){
						blockNums.add(item);
					}
				}
				catch(InvalidProtocolBufferException e){
					e.printStackTrace();
				}
			}
			public byte[] toProto(){
				Hdfs.OpenFileResponse.Builder builder = Hdfs.OpenFileResponse.newBuilder();
				builder.setStatus(status);
				builder.setHandle(handle);
				if(!blockNums.isEmpty()){
					for(int blocks:blockNums)
						builder.addBlockNums(blocks);
				}
				return builder.build().toByteArray();
			}
		}
/****************************************************************/
		public class ReadBlockRequest {
			public int blockNumber;

			public ReadBlockRequest(int block){
				blockNumber = block;
			}
			
			public ReadBlockRequest(byte[] input) {
				Hdfs.ReadBlockRequest builder = null;
				try {
					builder = Hdfs.ReadBlockRequest.parseFrom(input);
				} catch (InvalidProtocolBufferException e) {
					e.printStackTrace();
				}
				blockNumber = builder.getBlockNumber();
			}

			public byte[] toProto() {
				Hdfs.ReadBlockRequest.Builder builder = Hdfs.ReadBlockRequest.newBuilder();
				builder.setBlockNumber(blockNumber);
				return builder.build().toByteArray();
			}
		}
/*****************************************************/
		public class ReadBlockResponse {
			public int status;
			public byte data[];
			public ReadBlockResponse(){
				
			}
			public ReadBlockResponse(int st, byte[] dt){
				status = st;
				data = dt;
			}
			public ReadBlockResponse(byte[] input){
				Hdfs.ReadBlockResponse builder = null;
				try{	
					builder= Hdfs.ReadBlockResponse.parseFrom(input);
				}
				catch(InvalidProtocolBufferException e){
					e.printStackTrace();
				}
				status = builder.getStatus();
				int indx = 0;
				data = new byte[builder.getDataList().get(0).size()];
				for(ByteString bs : builder.getDataList()){
					byte[] tmp = bs.toByteArray();
					for(byte b : tmp){
						data[indx++] = b;
					}
				}
			}
			public byte[] toProto() {
				Hdfs.ReadBlockResponse.Builder builder = Hdfs.ReadBlockResponse.newBuilder();
				builder.setStatus(status);
				builder.addData(ByteString.copyFrom(data));
				return builder.build().toByteArray();
			}
		}
/****************************************************************/
}
