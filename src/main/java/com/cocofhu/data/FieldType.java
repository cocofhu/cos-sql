package com.cocofhu.data;

import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.HashMap;
import java.util.Map;

public enum FieldType {
    STRING(String.class, "string"),
    BOOLEAN(Primitive.BOOLEAN),
    BYTE(Primitive.BYTE),
    CHAR(Primitive.CHAR),
    SHORT(Primitive.SHORT),
    INT(Primitive.INT),
    LONG(Primitive.LONG),
    FLOAT(Primitive.FLOAT),
    DOUBLE(Primitive.DOUBLE);

    private final Class<?> clazz;
    private final String simpleName;

    private static final Map<String, FieldType> MAP = new HashMap<>();

    static {
        for (FieldType value : values()) {
            MAP.put(value.simpleName, value);
        }
    }

    FieldType(Primitive primitive) {
        this(primitive.boxClass, primitive.primitiveName);
    }

    FieldType(Class<?> clazz, String simpleName) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }

    public RelDataType toType(RelDataTypeFactory typeFactory) {
        RelDataType javaType = typeFactory.createJavaType(clazz);
        RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
        return typeFactory.createTypeWithNullability(sqlType, true);
    }

    public static FieldType of(String typeString) {
        return MAP.get(typeString);
    }

    public Object convert(String string) {
        switch (this) {
            case BOOLEAN:
                if (string.isEmpty()) return null;
                return Boolean.parseBoolean(string);
            case BYTE:
                if (string.isEmpty()) return null;
                return Byte.parseByte(string);
            case SHORT:
                if (string.isEmpty()) return null;
                return Short.parseShort(string);
            case INT:
                if (string.isEmpty()) return null;
                return Integer.parseInt(string);
            case LONG:
                if (string.isEmpty()) return null;
                return Long.parseLong(string);
            case FLOAT:
                if (string.isEmpty()) return null;
                return Float.parseFloat(string);
            case DOUBLE:
                if (string.isEmpty()) return null;
                return Double.parseDouble(string);
            case STRING:
            default:
                return string;
        }
    }
}
