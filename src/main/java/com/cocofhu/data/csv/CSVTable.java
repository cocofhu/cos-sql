package com.cocofhu.data.csv;

import com.cocofhu.data.FieldType;
import org.apache.calcite.DataContext;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CSVTable extends AbstractTable implements FilterableTable {
    private RelDataType relDataType;
    private final List<Pair<String, FieldType>> fields;
    private final String splitter;
    private final ArrayBlockingQueue<Object[]> queue;

    public CSVTable(List<Pair<String, FieldType>> fields,
                    final int cacheSize,
                    final String splitter
    ) {
        this.fields = fields;
        this.queue = new ArrayBlockingQueue<>(cacheSize);
        this.splitter = splitter;
    }

    public class LineEnumerator implements Enumerator<Object[]> {
        private Object[] current;
        volatile boolean hasNext = true;
        private final FieldType[] types;
        private final Condition condition;

        class Worker implements Runnable {
            private final List<String> files;

            public Worker(List<String> files) {
                this.files = files;
            }

            @Override
            public void run() {
                for (String file : files) {
                    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                            String[] row = line.split(splitter);
                            Object[] typedRow = new Object[fields.size()];
                            for (int i = 0, len = types.length; i < len; ++i) {
                                if (i < row.length) typedRow[i] = types[i].convert(row[i]);
                                else typedRow[i] = null;
                            }
                            if (condition.satisfy(typedRow)) {
                                queue.put(typedRow);
                            }
                        }
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        public LineEnumerator(Condition condition) {
            types = new FieldType[fields.size()];
            for (int i = 0, len = fields.size(); i < len; ++i) {
                types[i] = fields.get(i).right;
            }
            this.condition = condition;
            run();
        }

        void run() {
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < 6; ++i) {
                List<String> p = new ArrayList<>();
                p.add("C:\\Users\\Pouee\\Desktop\\111.txt");
                threads.add(new Thread(new Worker(p)));
            }
            hasNext = true;
            new Thread(() -> {
                for (Thread t : threads) t.start();
                for (Thread t : threads) {
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                hasNext = false;
            }).start();
        }

        @Override
        public Object[] current() {
            return current;
        }

        @Override
        public boolean moveNext() {
            try {
                Object[] next = null;
                do {
                    next = queue.poll(1L, TimeUnit.SECONDS);
                } while (next == null && hasNext);
                if (next == null) return false;
                current = next;
                return true;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException("cannot reset!");
        }

        @Override
        public void close() {
            hasNext = false;
        }
    }


    @Override
    public Enumerable<Object[]> scan(DataContext dataContext, List<RexNode> list) {
        Condition condition = Condition.TRUE;
        if (!list.isEmpty()) {
            Condition c = dfs(list.get(0));
            if (c != null) condition = c;
        }
        // 匿名函数闭包变量
        Condition finalCondition = condition;
        System.out.println("FILTER OF PUSH DOWN : " + (condition == Condition.TRUE ? "Nothing" : condition));

        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new LineEnumerator(finalCondition);
            }
        };
    }

    /**
     * 处理关系表达式：减少主节点对数据的处理压力
     * 注意：我们将谓词下推至数据源当并未消除该谓词
     */
    Condition dfs(RexNode filter) {
        if (filter.isA(SqlKind.AND)) {
            List<RexNode> operands = ((RexCall) filter).getOperands();
            List<Condition> list = new ArrayList<>(operands.size());
            for (RexNode o : operands) {
                Condition c = dfs(o);
                if (c != null) list.add(c);
            }
            return new Condition.And(list);
        }

        if (filter.isA(SqlKind.OR)) {
            List<RexNode> operands = ((RexCall) filter).getOperands();
            List<Condition> list = new ArrayList<>(operands.size());
            for (RexNode o : operands) {
                Condition c = dfs(o);
                if (c != null) list.add(c);
            }
            return new Condition.Or(list);
        }

        if (filter.isA(SqlKind.EQUALS)) {
            final RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            final RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef
                    && right instanceof RexLiteral) {
                final int index = ((RexInputRef) left).getIndex();
                return new Condition.Eq(fields.get(index).right, index,
                        Objects.requireNonNull(((RexLiteral) right).getValue2()).toString());
            }
        }
        // 这是一个很简单的实现，在某些情况可能会出现不可预测的结果
        if (filter.isA(SqlKind.LIKE)) {
            final RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            final RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef
                    && right instanceof RexLiteral) {
                final int index = ((RexInputRef) left).getIndex();
                return new Condition.Like(fields.get(index).right, index,
                        Objects.requireNonNull(((RexLiteral) right).getValue2()).toString());
            }
        }

        return null;
    }


    @Override
    public synchronized RelDataType getRowType(RelDataTypeFactory factory) {
        if (relDataType == null) {
            relDataType = factory.createStructType(
                    fields.stream().map(a -> new Pair<>(a.left, Objects.requireNonNull(a.right).toType(factory))).collect(Collectors.toList()));
        }
        return relDataType;
    }
}
