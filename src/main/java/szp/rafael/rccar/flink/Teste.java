package szp.rafael.rccar.flink;

import org.apache.commons.collections4.queue.CircularFifoQueue;

public class Teste  {
    public static void main(String[] args) {
        CircularFifoQueue q = new CircularFifoQueue<>(3);
        q.add("A");
        q.add("B");
        q.add("C");
        System.out.println(q);
        q.add("d");
        System.out.println(q);
        q.add("e");
        System.out.println(q);
    }
}
