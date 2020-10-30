package cs451;

import java.io.*;
import java.util.Objects;

public class Message implements Serializable {

    private int senderId;
    private int seqNum;
    private boolean isAck;

    public Message(int sendId, int seqNum, boolean isAck) {
        this.senderId = sendId;
        this.seqNum = seqNum;
        this.isAck = isAck;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public boolean isAck() {
        return isAck;
    }

    // Generate ACK message (note that we switch source and destination)
    public Message generateAck(int pid) {
        return new Message(pid, seqNum, true);
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
        return Objects.hash(senderId, seqNum, isAck);
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
        String msgType;
        if (isAck) msgType = "ACK";
        else msgType = "DATA";
        return "Message(" + msgType
                + ", sendId: " + senderId
                + ", seqNum: " + seqNum
                + ")";
    }
}
