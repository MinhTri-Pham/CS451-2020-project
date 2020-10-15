package cs451;

import java.io.*;
import java.net.InetAddress;
import java.util.Objects;

public class Message implements Serializable {

    private int senderId;
    private int seqNum;
    private boolean isAck;
    private int sourcePort;
    private InetAddress sourceIp;
    private int destPort;
    private InetAddress destIp;

    public Message(int sendId, int seqNum, boolean isAck, int sourcePort, InetAddress sourceIp, int destPort, InetAddress destIp) {
        this.senderId = sendId;
        this.seqNum = seqNum;
        this.isAck = isAck;
        this.sourcePort = sourcePort;
        this.sourceIp = sourceIp;
        this.destPort = destPort;
        this.destIp = destIp;
    }

    public Message(int sendId, int seqNum, int sourcePort, InetAddress sourceIp, int destPort, InetAddress destIp) {
        this.senderId = sendId;
        this.seqNum = seqNum;
        this.isAck = false;
        this.sourcePort = sourcePort;
        this.sourceIp = sourceIp;
        this.destPort = destPort;
        this.destIp = destIp;
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

    public int getSourcePort() {
        return sourcePort;
    }

    public InetAddress getSourceIp() {
        return sourceIp;
    }

    // Generate ACK message (note that we switch source and destination)
    public Message generateAck(int pid) {
        return new Message(pid, seqNum, true, destPort, destIp, sourcePort, sourceIp);
    }

    // Message with given sequence number
    public Message withSeqNum(int sn) {
        return new Message(senderId, sn, isAck, destPort, destIp, sourcePort, sourceIp);
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
        return Objects.hash(senderId, seqNum, isAck, sourcePort, sourceIp, destPort, destIp);
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
                + ", srcPort: " + sourcePort
                + ", srcAddr: " + sourceIp
                + ", destPort: " + destPort
                + ", destAddr: " + destIp
                + ")";
    }
}
