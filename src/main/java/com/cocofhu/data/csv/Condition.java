package com.cocofhu.data.csv;

import com.cocofhu.data.FieldType;

import java.util.List;
import java.util.function.Function;


public interface Condition {
    Condition TRUE = row -> true;
    boolean satisfy(Object[] row);

    abstract class BiOp implements Condition{
        final FieldType type;
        final int index;
        final String literalVal;

        protected BiOp(FieldType type, int index, String literalVal) {
            this.type = type;
            this.index = index;
            this.literalVal = literalVal;
        }
    }
    class Eq extends BiOp{
        protected Eq(FieldType type, int index, String literalVal) {
            super(type, index, literalVal);
        }

        @Override
        public boolean satisfy(Object[] row) {
            try {
                return row[index].equals(type.convert(literalVal));
            }catch (Exception e){
                return false;
            }
        }

        @Override
        public String toString() {
            return "Eq{" +
                    "type=" + type +
                    ", index=" + index +
                    ", literalVal='" + literalVal + '\'' +
                    '}';
        }
    }

    // 使用正则表达式
    class Like extends BiOp {
        Function<Object, Boolean> check = o -> true;
        private String expr = "always ture";
        protected Like(FieldType type, int index, String literalVal) {
            super(type, index, literalVal);
            char a = literalVal.charAt(0), b = literalVal.charAt(literalVal.length() - 1);
            if(a == '%' && b == '%') {
                String pattern = literalVal.substring(1, literalVal.length() - 1);
                check = o -> o.toString().contains(pattern);
                expr = "contains(" + pattern + ")";
            } else if(a == '%') {
                String pattern = literalVal.substring(1);
                check = o -> o.toString().startsWith(pattern);
                expr = "startsWith(" + pattern + ")";
            } else if(b == '%') {
                String pattern = literalVal.substring(0, literalVal.length() - 1);
                check = o -> o.toString().endsWith(pattern);
                expr = "endsWith(" + pattern + ")";
            }
        }

        @Override
        public boolean satisfy(Object[] row) {
            try {
                return check.apply(row[index]);
            }catch (Exception e){
                return false;
            }
        }

        @Override
        public String toString() {
            return "Like{" +
                    "expr='" + expr + '\'' +
                    ", type=" + type +
                    ", index=" + index +
                    ", literalVal='" + literalVal + '\'' +
                    '}';
        }
    }

    class Or implements Condition{
        final List<Condition> children;
        public Or(List<Condition> children) {
            this.children = children;
        }
        @Override
        public boolean satisfy(Object[] row) {
            for(Condition c: children) {
                if (c.satisfy(row)) return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return "Or{" +
                    "children=" + children +
                    '}';
        }
    }
    class And implements Condition{
        final List<Condition> children;
        public And(List<Condition> children) {
            this.children = children;
        }
        @Override
        public boolean satisfy(Object[] row) {
            for(Condition c: children) {
                if (!c.satisfy(row)) return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "And{" +
                    "children=" + children +
                    '}';
        }
    }
}
