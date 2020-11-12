package cs451;

import java.io.*;
import java.util.Objects;

public class Message implements Serializable {

    // Most importantly, distinguish between sender and first sender for URB
    // Rebroadcast messages change senderId but retain firstSenderId
    private int senderId;
    private int firstSenderId;
    private int seqNum;
    private boolean isAck;

    public Message(int sendId, int firstSenderId, int seqNum, boolean isAck) {
        this.senderId = sendId;
        this.firstSenderId = firstSenderId;
        this.seqNum = seqNum;
        this.isAck = isAck;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getFirstSenderId() { return firstSenderId; }

    public int getSeqNum() {
        return seqNum;
    }

    public boolean isAck() {
        return isAck;
    }

    // Transforms a Message into a byte array for sending via UDP
    public byte[] toData() throws IOException {
        ByteArrayOutputStream byteArrOutStream = new ByteArrayOutputStream();
        ObjectOutputStream objOutStr = new ObjectOutputStream(byteArrOutStream);
        objOutStr.writeObject(this);
        return byteArrOutStream.toByteArray();
    }

    // Transforms a byte array into a Message for processing
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
        return Objects.hash(senderId, firstSenderId, seqNum, isAck);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;

        return senderId == message.senderId && firstSenderId == message.firstSenderId
                && seqNum == message.seqNum && isAck == message.isAck;
    }

    @Override
    public String toString() {
        String msgType;
        if (isAck) msgType = "ACK";
        else msgType = "DATA";
        return "Message(" + msgType
                + ", senderId: " + senderId
                + ", firstSenderId: " + firstSenderId
                + ", seqNum: " + seqNum
                + ")";
    }
}
