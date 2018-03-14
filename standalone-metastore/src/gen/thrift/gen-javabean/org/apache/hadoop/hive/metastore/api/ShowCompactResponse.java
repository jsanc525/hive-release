/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class ShowCompactResponse implements org.apache.thrift.TBase<ShowCompactResponse, ShowCompactResponse._Fields>, java.io.Serializable, Cloneable, Comparable<ShowCompactResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ShowCompactResponse");

  private static final org.apache.thrift.protocol.TField COMPACTS_FIELD_DESC = new org.apache.thrift.protocol.TField("compacts", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ShowCompactResponseStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ShowCompactResponseTupleSchemeFactory());
  }

  private List<ShowCompactResponseElement> compacts; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    COMPACTS((short)1, "compacts");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // COMPACTS
          return COMPACTS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.COMPACTS, new org.apache.thrift.meta_data.FieldMetaData("compacts", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ShowCompactResponseElement.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ShowCompactResponse.class, metaDataMap);
  }

  public ShowCompactResponse() {
  }

  public ShowCompactResponse(
    List<ShowCompactResponseElement> compacts)
  {
    this();
    this.compacts = compacts;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ShowCompactResponse(ShowCompactResponse other) {
    if (other.isSetCompacts()) {
      List<ShowCompactResponseElement> __this__compacts = new ArrayList<ShowCompactResponseElement>(other.compacts.size());
      for (ShowCompactResponseElement other_element : other.compacts) {
        __this__compacts.add(new ShowCompactResponseElement(other_element));
      }
      this.compacts = __this__compacts;
    }
  }

  public ShowCompactResponse deepCopy() {
    return new ShowCompactResponse(this);
  }

  @Override
  public void clear() {
    this.compacts = null;
  }

  public int getCompactsSize() {
    return (this.compacts == null) ? 0 : this.compacts.size();
  }

  public java.util.Iterator<ShowCompactResponseElement> getCompactsIterator() {
    return (this.compacts == null) ? null : this.compacts.iterator();
  }

  public void addToCompacts(ShowCompactResponseElement elem) {
    if (this.compacts == null) {
      this.compacts = new ArrayList<ShowCompactResponseElement>();
    }
    this.compacts.add(elem);
  }

  public List<ShowCompactResponseElement> getCompacts() {
    return this.compacts;
  }

  public void setCompacts(List<ShowCompactResponseElement> compacts) {
    this.compacts = compacts;
  }

  public void unsetCompacts() {
    this.compacts = null;
  }

  /** Returns true if field compacts is set (has been assigned a value) and false otherwise */
  public boolean isSetCompacts() {
    return this.compacts != null;
  }

  public void setCompactsIsSet(boolean value) {
    if (!value) {
      this.compacts = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case COMPACTS:
      if (value == null) {
        unsetCompacts();
      } else {
        setCompacts((List<ShowCompactResponseElement>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case COMPACTS:
      return getCompacts();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case COMPACTS:
      return isSetCompacts();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ShowCompactResponse)
      return this.equals((ShowCompactResponse)that);
    return false;
  }

  public boolean equals(ShowCompactResponse that) {
    if (that == null)
      return false;

    boolean this_present_compacts = true && this.isSetCompacts();
    boolean that_present_compacts = true && that.isSetCompacts();
    if (this_present_compacts || that_present_compacts) {
      if (!(this_present_compacts && that_present_compacts))
        return false;
      if (!this.compacts.equals(that.compacts))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_compacts = true && (isSetCompacts());
    list.add(present_compacts);
    if (present_compacts)
      list.add(compacts);

    return list.hashCode();
  }

  @Override
  public int compareTo(ShowCompactResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetCompacts()).compareTo(other.isSetCompacts());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCompacts()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.compacts, other.compacts);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ShowCompactResponse(");
    boolean first = true;

    sb.append("compacts:");
    if (this.compacts == null) {
      sb.append("null");
    } else {
      sb.append(this.compacts);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetCompacts()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'compacts' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ShowCompactResponseStandardSchemeFactory implements SchemeFactory {
    public ShowCompactResponseStandardScheme getScheme() {
      return new ShowCompactResponseStandardScheme();
    }
  }

  private static class ShowCompactResponseStandardScheme extends StandardScheme<ShowCompactResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ShowCompactResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // COMPACTS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list660 = iprot.readListBegin();
                struct.compacts = new ArrayList<ShowCompactResponseElement>(_list660.size);
                ShowCompactResponseElement _elem661;
                for (int _i662 = 0; _i662 < _list660.size; ++_i662)
                {
                  _elem661 = new ShowCompactResponseElement();
                  _elem661.read(iprot);
                  struct.compacts.add(_elem661);
                }
                iprot.readListEnd();
              }
              struct.setCompactsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ShowCompactResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.compacts != null) {
        oprot.writeFieldBegin(COMPACTS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.compacts.size()));
          for (ShowCompactResponseElement _iter663 : struct.compacts)
          {
            _iter663.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ShowCompactResponseTupleSchemeFactory implements SchemeFactory {
    public ShowCompactResponseTupleScheme getScheme() {
      return new ShowCompactResponseTupleScheme();
    }
  }

  private static class ShowCompactResponseTupleScheme extends TupleScheme<ShowCompactResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ShowCompactResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.compacts.size());
        for (ShowCompactResponseElement _iter664 : struct.compacts)
        {
          _iter664.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ShowCompactResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list665 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.compacts = new ArrayList<ShowCompactResponseElement>(_list665.size);
        ShowCompactResponseElement _elem666;
        for (int _i667 = 0; _i667 < _list665.size; ++_i667)
        {
          _elem666 = new ShowCompactResponseElement();
          _elem666.read(iprot);
          struct.compacts.add(_elem666);
        }
      }
      struct.setCompactsIsSet(true);
    }
  }

}

