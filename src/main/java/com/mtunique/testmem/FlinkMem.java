package com.mtunique.testmem;

import org.apache.flink.api.common.typeutils.base.array.StringArraySerializer;
import org.apache.flink.core.memory.*;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.ListMemorySegmentSource;
import org.apache.flink.runtime.memory.MemoryManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

/**
 * Created by taomeng on 2016/12/29.
 */
@RunWith(Parameterized.class)
public class FlinkMem {
    private final AbstractInvokable parentTask = new DummyInvokable();

    private static final int DEFAULT_MEMORY_PAGE_SIZE = 1024*32;
    private int count;
    private int memSize;
    private int times;

    private WriteView outView;
    private ReadView inView;
    private MemoryManager mm;
    private List<MemorySegment> memory;

    String chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    Random random = new Random(0);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { 1024 * 8, 1024 * 8 * 4, 1024*40},
//                { 134217728, 134217728*8, 10}
        });
    }

    public FlinkMem(int count, int memSize, int times) throws Exception {
        this.count = count;
        this.memSize = memSize;
        this.times = times;
        this.mm = new MemoryManager(this.memSize, 1);
        this.memory = mm.allocatePages(this.parentTask, this.memSize / DEFAULT_MEMORY_PAGE_SIZE);
        ListMemorySegmentSource memSource = new ListMemorySegmentSource(memory);
        this.outView = new WriteView(memory, memSource, DEFAULT_MEMORY_PAGE_SIZE);
        this.inView = new ReadView(memory, DEFAULT_MEMORY_PAGE_SIZE);
    }

    @Test
    public void writeAndReadInt() throws Exception {

        long pre = System.currentTimeMillis();
        long sum = 0L;
        for (int t = 0; t < times; t++) {
            this.outView.resetTo(0L);
            int i = 0;
            while (i < this.count) {
                this.outView.writeInt(i);
                i++;
            }

            this.inView.setReadPosition(0L);
            i = 0;
            while (i < this.count) {
                this.inView.readInt();
                i ++;
            }
        }
        System.out.println(System.currentTimeMillis() - pre);
    }

    private String randomString(int min, int max) {
        int len = random.nextInt(max - min) + min;
        StringBuilder sb = new StringBuilder(len);
        int i = 0;
        while (i < len) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
            i += 1;
        }
        return sb.toString();
    }

    @Test
    public void writeAndReadString() throws Exception {
        int minString = 3;
        int maxString = 32;
        int count = 4 * 1000;

        String[] data = new String [count];
        for (int i = 0; i < count; i++) {
            data[i] = randomString(minString, maxString);
        }

        StringArraySerializer serializer = new StringArraySerializer();

        long pre = System.currentTimeMillis();
        long sum = 0L;
        for (int t = 0; t < times; t++) {
            this.outView.resetTo(0L);
            int i = 0;
            while (i < this.count) {
                serializer.serialize(data, this.outView);
                i++;
            }

            this.inView.setReadPosition(0L);
            i = 0;
            while (i < this.count) {
                serializer.deserialize(data, this.inView);
            }
        }
        System.out.println(System.currentTimeMillis() - pre);
    }

}
