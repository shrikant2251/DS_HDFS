package HDFSPackage;
//import 
import java.rmi.Remote;
import java.rmi.RemoteException;
import com.google.protobuf.*;
public interface IDataNode extends Remote{

	/* ReadBlockResponse readBlock(ReadBlockRequest)) */
	/* Method to read data from any block given block-number */
	byte[] readBlock(byte dataBlock[]) throws RemoteException;
	
	/* WriteBlockResponse writeBlock(WriteBlockRequest) */
	/* Method to write data to a specific block */
	byte[] writeBlock(byte dataBlock[]) throws RemoteException;
}