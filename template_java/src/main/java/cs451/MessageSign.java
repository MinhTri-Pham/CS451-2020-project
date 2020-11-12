package cs451;

import java.util.Objects;

// Compressed form of a message that stays the same for rebroadcast messages in URB
public class MessageSign {
    private int firstSenderId;
    private int seqNum;

    public MessageSign(int firstSenderId, int seqNum) {
        this.firstSenderId = firstSenderId;
        this.seqNum = seqNum;
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
