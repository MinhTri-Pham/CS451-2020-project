package cs451;

import java.util.Objects;

// Similar to message but compressed form that doesn't change for rebroadcast messages in URB
public class MessageFirst {
    private int firstSenderId;
    private int seqNum;

    public MessageFirst(int firstSenderId, int seqNum) {
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
        MessageFirst messageFirst = (MessageFirst) o;

        return firstSenderId == messageFirst.firstSenderId && seqNum == messageFirst.seqNum;
    }

    @Override
    public String toString() {
        return "MessageFirst(firstSenderId: "  + firstSenderId
                + ", seqNum: " + seqNum
                + ")";
    }
}
