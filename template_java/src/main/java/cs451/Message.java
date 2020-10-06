package cs451;

import java.io.*;
import java.util.Objects;

public class Message implements Serializable {

    private int seqNum;
    private boolean isAck;

    public Message(int seqNum, boolean isAck) {
        this.seqNum = seqNum;
        this.isAck = isAck;
    }

    // Simpler constructor for non-ACK messages
    public Message(int seqNum) {
        this.seqNum = seqNum;
        this.isAck = false;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public boolean isAck() {
        return isAck;
    }

    // Generate equivalent message but ACK
    public Message generateAck() {
        return new Message(seqNum, true);
    }

    public byte[] toData() throws IOException {
        ByteArrayOutputStream byteArrOutStream = new ByteArrayOutputStream();
        ObjectOutputStream objOutStr = new ObjectOutputStream(byteArrOutStream);
        objOutStr.writeObject(this);
        return byteArrOutStream.toByteArray();
    }

    public static Message fromData(byte[] data) {
       try {
           ByteArrayInputStream byteArrInStr = new ByteArrayInputStream(data);
           ObjectInputStream objInStr =  new ObjectInputStream(byteArrInStr);
           return (Message) objInStr.readObject();
       }
       catch (IOException | ClassNotFoundException e) {
            return null;
       }
    }

    @Override
    public int hashCode() {
        return Objects.hash(seqNum, isAck);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;

        return seqNum == message.seqNum && isAck == message.isAck;
    }

    @Override
    public String toString() {
        String msgtype;
        if (isAck) msgtype = "ACK";
        else msgtype = "DATA";
        return "Message(" + msgtype
                + ", seqNum: " + seqNum +")";
    }
}
