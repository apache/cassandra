package org.apache.cassandra.db;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.xml.bind.annotation.XmlElement;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;


public class TouchMessage
{

private static ICompactSerializer<TouchMessage> serializer_;	
	
    static
    {
        serializer_ = new TouchMessageSerializer();
    }

    static ICompactSerializer<TouchMessage> serializer()
    {
        return serializer_;
    }
    
    public static Message makeTouchMessage(TouchMessage touchMessage) throws IOException
    {
    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        TouchMessage.serializer().serialize(touchMessage, dos);
        Message message = new Message(StorageService.getLocalStorageEndPoint(), StorageService.readStage_, StorageService.touchVerbHandler_, new Object[]{bos.toByteArray()});         
        return message;
    }
    
    @XmlElement(name="Table")
    private String table_;
    
    @XmlElement(name="Key")
    private String key_;
    
    @XmlElement(name="fData")
    private boolean fData_ = true;
        
    private TouchMessage()
    {
    }
    
    public TouchMessage(String table, String key)
    {
        table_ = table;
        key_ = key;
    }

    public TouchMessage(String table, String key, boolean fData)
    {
        table_ = table;
        key_ = key;
        fData_ = fData;
    }
    

    String table()
    {
        return table_;
    }
    
    String key()
    {
        return key_;
    }

    public boolean isData()
    {
    	return fData_;
    }
}

class TouchMessageSerializer implements ICompactSerializer<TouchMessage>
{
	public void serialize(TouchMessage tm, DataOutputStream dos) throws IOException
	{
		dos.writeUTF(tm.table());
		dos.writeUTF(tm.key());
		dos.writeBoolean(tm.isData());
	}
	
    public TouchMessage deserialize(DataInputStream dis) throws IOException
    {
		String table = dis.readUTF();
		String key = dis.readUTF();
		boolean fData = dis.readBoolean();
		TouchMessage tm = new TouchMessage( table, key, fData);
    	return tm;
    }
	
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		// TODO Auto-generated method stub

	}

}
