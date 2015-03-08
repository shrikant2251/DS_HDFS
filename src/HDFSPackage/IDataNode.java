package HDFSPackage;
import 
public interface IDataNode {

	/* ReadBlockResponse readBlock(ReadBlockRequest)) */
	/* Method to read data from any block given block-number */
	byte[] readBlock(byte dataBlock[]);
	
	/* WriteBlockResponse writeBlock(WriteBlockRequest) */
	/* Method to write data to a specific block */
	byte[] writeBlock(byte dataBlock[]);
}