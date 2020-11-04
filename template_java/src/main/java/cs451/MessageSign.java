package cs451;

import java.util.Objects;

// Similar to message but compressed form that doesn't change for rebroadcast messages in URB
public class MessageSign {
    private int firstSenderId;
    private int seqNum;

    public MessageSign(int firstSenderId, int seqNum) {
        this.firstSenderId = firstSenderId;
        this.seqNum = seqNum;
    }

    public int getFirstSenderId() {
        return firstSenderId;
    }

    public int getSeqNum() {
        return seqNum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstSenderId, seqNum);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageSign messageSign = (MessageSign) o;

        return firstSenderId == messageSign.firstSenderId && seqNum == messageSign.seqNum;
    }

    @Override
    public String toString() {
        return "MessageSign(firstSenderId: "  + firstSenderId
                + ", seqNum: " + seqNum
                + ")";
    }
}
