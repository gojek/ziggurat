// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: proto/person.proto

package com.gojek.test.proto;

public final class PersonTestProto {
  private PersonTestProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface PersonOrBuilder extends
      // @@protoc_insertion_point(interface_extends:gojek.test.proto.Person)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>int32 id = 1;</code>
     */
    int getId();

    /**
     * <code>string name = 2;</code>
     */
    java.lang.String getName();
    /**
     * <code>string name = 2;</code>
     */
    com.google.protobuf.ByteString
        getNameBytes();

    /**
     * <code>string email = 3;</code>
     */
    java.lang.String getEmail();
    /**
     * <code>string email = 3;</code>
     */
    com.google.protobuf.ByteString
        getEmailBytes();

    /**
     * <code>string likes = 4;</code>
     */
    java.lang.String getLikes();
    /**
     * <code>string likes = 4;</code>
     */
    com.google.protobuf.ByteString
        getLikesBytes();

    /**
     * <code>.google.protobuf.Struct characters = 5;</code>
     */
    boolean hasCharacters();
    /**
     * <code>.google.protobuf.Struct characters = 5;</code>
     */
    com.google.protobuf.Struct getCharacters();
    /**
     * <code>.google.protobuf.Struct characters = 5;</code>
     */
    com.google.protobuf.StructOrBuilder getCharactersOrBuilder();
  }
  /**
   * Protobuf type {@code gojek.test.proto.Person}
   */
  public  static final class Person extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:gojek.test.proto.Person)
      PersonOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use Person.newBuilder() to construct.
    private Person(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private Person() {
      id_ = 0;
      name_ = "";
      email_ = "";
      likes_ = "";
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private Person(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownFieldProto3(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {

              id_ = input.readInt32();
              break;
            }
            case 18: {
              java.lang.String s = input.readStringRequireUtf8();

              name_ = s;
              break;
            }
            case 26: {
              java.lang.String s = input.readStringRequireUtf8();

              email_ = s;
              break;
            }
            case 34: {
              java.lang.String s = input.readStringRequireUtf8();

              likes_ = s;
              break;
            }
            case 42: {
              com.google.protobuf.Struct.Builder subBuilder = null;
              if (characters_ != null) {
                subBuilder = characters_.toBuilder();
              }
              characters_ = input.readMessage(com.google.protobuf.Struct.parser(), extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(characters_);
                characters_ = subBuilder.buildPartial();
              }

              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.gojek.test.proto.PersonTestProto.internal_static_gojek_test_proto_Person_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.gojek.test.proto.PersonTestProto.internal_static_gojek_test_proto_Person_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.gojek.test.proto.PersonTestProto.Person.class, com.gojek.test.proto.PersonTestProto.Person.Builder.class);
    }

    public static final int ID_FIELD_NUMBER = 1;
    private int id_;
    /**
     * <code>int32 id = 1;</code>
     */
    public int getId() {
      return id_;
    }

    public static final int NAME_FIELD_NUMBER = 2;
    private volatile java.lang.Object name_;
    /**
     * <code>string name = 2;</code>
     */
    public java.lang.String getName() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        name_ = s;
        return s;
      }
    }
    /**
     * <code>string name = 2;</code>
     */
    public com.google.protobuf.ByteString
        getNameBytes() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        name_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int EMAIL_FIELD_NUMBER = 3;
    private volatile java.lang.Object email_;
    /**
     * <code>string email = 3;</code>
     */
    public java.lang.String getEmail() {
      java.lang.Object ref = email_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        email_ = s;
        return s;
      }
    }
    /**
     * <code>string email = 3;</code>
     */
    public com.google.protobuf.ByteString
        getEmailBytes() {
      java.lang.Object ref = email_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        email_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int LIKES_FIELD_NUMBER = 4;
    private volatile java.lang.Object likes_;
    /**
     * <code>string likes = 4;</code>
     */
    public java.lang.String getLikes() {
      java.lang.Object ref = likes_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        likes_ = s;
        return s;
      }
    }
    /**
     * <code>string likes = 4;</code>
     */
    public com.google.protobuf.ByteString
        getLikesBytes() {
      java.lang.Object ref = likes_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        likes_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int CHARACTERS_FIELD_NUMBER = 5;
    private com.google.protobuf.Struct characters_;
    /**
     * <code>.google.protobuf.Struct characters = 5;</code>
     */
    public boolean hasCharacters() {
      return characters_ != null;
    }
    /**
     * <code>.google.protobuf.Struct characters = 5;</code>
     */
    public com.google.protobuf.Struct getCharacters() {
      return characters_ == null ? com.google.protobuf.Struct.getDefaultInstance() : characters_;
    }
    /**
     * <code>.google.protobuf.Struct characters = 5;</code>
     */
    public com.google.protobuf.StructOrBuilder getCharactersOrBuilder() {
      return getCharacters();
    }

    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (id_ != 0) {
        output.writeInt32(1, id_);
      }
      if (!getNameBytes().isEmpty()) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 2, name_);
      }
      if (!getEmailBytes().isEmpty()) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 3, email_);
      }
      if (!getLikesBytes().isEmpty()) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 4, likes_);
      }
      if (characters_ != null) {
        output.writeMessage(5, getCharacters());
      }
      unknownFields.writeTo(output);
    }

    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (id_ != 0) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(1, id_);
      }
      if (!getNameBytes().isEmpty()) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, name_);
      }
      if (!getEmailBytes().isEmpty()) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, email_);
      }
      if (!getLikesBytes().isEmpty()) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(4, likes_);
      }
      if (characters_ != null) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(5, getCharacters());
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.gojek.test.proto.PersonTestProto.Person)) {
        return super.equals(obj);
      }
      com.gojek.test.proto.PersonTestProto.Person other = (com.gojek.test.proto.PersonTestProto.Person) obj;

      boolean result = true;
      result = result && (getId()
          == other.getId());
      result = result && getName()
          .equals(other.getName());
      result = result && getEmail()
          .equals(other.getEmail());
      result = result && getLikes()
          .equals(other.getLikes());
      result = result && (hasCharacters() == other.hasCharacters());
      if (hasCharacters()) {
        result = result && getCharacters()
            .equals(other.getCharacters());
      }
      result = result && unknownFields.equals(other.unknownFields);
      return result;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      hash = (37 * hash) + ID_FIELD_NUMBER;
      hash = (53 * hash) + getId();
      hash = (37 * hash) + NAME_FIELD_NUMBER;
      hash = (53 * hash) + getName().hashCode();
      hash = (37 * hash) + EMAIL_FIELD_NUMBER;
      hash = (53 * hash) + getEmail().hashCode();
      hash = (37 * hash) + LIKES_FIELD_NUMBER;
      hash = (53 * hash) + getLikes().hashCode();
      if (hasCharacters()) {
        hash = (37 * hash) + CHARACTERS_FIELD_NUMBER;
        hash = (53 * hash) + getCharacters().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.gojek.test.proto.PersonTestProto.Person parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.gojek.test.proto.PersonTestProto.Person parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.gojek.test.proto.PersonTestProto.Person parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.gojek.test.proto.PersonTestProto.Person parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.gojek.test.proto.PersonTestProto.Person parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.gojek.test.proto.PersonTestProto.Person parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.gojek.test.proto.PersonTestProto.Person parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.gojek.test.proto.PersonTestProto.Person parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.gojek.test.proto.PersonTestProto.Person parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static com.gojek.test.proto.PersonTestProto.Person parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.gojek.test.proto.PersonTestProto.Person parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.gojek.test.proto.PersonTestProto.Person parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(com.gojek.test.proto.PersonTestProto.Person prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code gojek.test.proto.Person}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:gojek.test.proto.Person)
        com.gojek.test.proto.PersonTestProto.PersonOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.gojek.test.proto.PersonTestProto.internal_static_gojek_test_proto_Person_descriptor;
      }

      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.gojek.test.proto.PersonTestProto.internal_static_gojek_test_proto_Person_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.gojek.test.proto.PersonTestProto.Person.class, com.gojek.test.proto.PersonTestProto.Person.Builder.class);
      }

      // Construct using com.gojek.test.proto.PersonTestProto.Person.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      public Builder clear() {
        super.clear();
        id_ = 0;

        name_ = "";

        email_ = "";

        likes_ = "";

        if (charactersBuilder_ == null) {
          characters_ = null;
        } else {
          characters_ = null;
          charactersBuilder_ = null;
        }
        return this;
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.gojek.test.proto.PersonTestProto.internal_static_gojek_test_proto_Person_descriptor;
      }

      public com.gojek.test.proto.PersonTestProto.Person getDefaultInstanceForType() {
        return com.gojek.test.proto.PersonTestProto.Person.getDefaultInstance();
      }

      public com.gojek.test.proto.PersonTestProto.Person build() {
        com.gojek.test.proto.PersonTestProto.Person result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.gojek.test.proto.PersonTestProto.Person buildPartial() {
        com.gojek.test.proto.PersonTestProto.Person result = new com.gojek.test.proto.PersonTestProto.Person(this);
        result.id_ = id_;
        result.name_ = name_;
        result.email_ = email_;
        result.likes_ = likes_;
        if (charactersBuilder_ == null) {
          result.characters_ = characters_;
        } else {
          result.characters_ = charactersBuilder_.build();
        }
        onBuilt();
        return result;
      }

      public Builder clone() {
        return (Builder) super.clone();
      }
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return (Builder) super.setField(field, value);
      }
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return (Builder) super.clearField(field);
      }
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return (Builder) super.clearOneof(oneof);
      }
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return (Builder) super.setRepeatedField(field, index, value);
      }
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return (Builder) super.addRepeatedField(field, value);
      }
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.gojek.test.proto.PersonTestProto.Person) {
          return mergeFrom((com.gojek.test.proto.PersonTestProto.Person)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.gojek.test.proto.PersonTestProto.Person other) {
        if (other == com.gojek.test.proto.PersonTestProto.Person.getDefaultInstance()) return this;
        if (other.getId() != 0) {
          setId(other.getId());
        }
        if (!other.getName().isEmpty()) {
          name_ = other.name_;
          onChanged();
        }
        if (!other.getEmail().isEmpty()) {
          email_ = other.email_;
          onChanged();
        }
        if (!other.getLikes().isEmpty()) {
          likes_ = other.likes_;
          onChanged();
        }
        if (other.hasCharacters()) {
          mergeCharacters(other.getCharacters());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.gojek.test.proto.PersonTestProto.Person parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.gojek.test.proto.PersonTestProto.Person) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }

      private int id_ ;
      /**
       * <code>int32 id = 1;</code>
       */
      public int getId() {
        return id_;
      }
      /**
       * <code>int32 id = 1;</code>
       */
      public Builder setId(int value) {
        
        id_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>int32 id = 1;</code>
       */
      public Builder clearId() {
        
        id_ = 0;
        onChanged();
        return this;
      }

      private java.lang.Object name_ = "";
      /**
       * <code>string name = 2;</code>
       */
      public java.lang.String getName() {
        java.lang.Object ref = name_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          name_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string name = 2;</code>
       */
      public com.google.protobuf.ByteString
          getNameBytes() {
        java.lang.Object ref = name_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          name_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string name = 2;</code>
       */
      public Builder setName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        name_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>string name = 2;</code>
       */
      public Builder clearName() {
        
        name_ = getDefaultInstance().getName();
        onChanged();
        return this;
      }
      /**
       * <code>string name = 2;</code>
       */
      public Builder setNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
        
        name_ = value;
        onChanged();
        return this;
      }

      private java.lang.Object email_ = "";
      /**
       * <code>string email = 3;</code>
       */
      public java.lang.String getEmail() {
        java.lang.Object ref = email_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          email_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string email = 3;</code>
       */
      public com.google.protobuf.ByteString
          getEmailBytes() {
        java.lang.Object ref = email_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          email_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string email = 3;</code>
       */
      public Builder setEmail(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        email_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>string email = 3;</code>
       */
      public Builder clearEmail() {
        
        email_ = getDefaultInstance().getEmail();
        onChanged();
        return this;
      }
      /**
       * <code>string email = 3;</code>
       */
      public Builder setEmailBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
        
        email_ = value;
        onChanged();
        return this;
      }

      private java.lang.Object likes_ = "";
      /**
       * <code>string likes = 4;</code>
       */
      public java.lang.String getLikes() {
        java.lang.Object ref = likes_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          likes_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string likes = 4;</code>
       */
      public com.google.protobuf.ByteString
          getLikesBytes() {
        java.lang.Object ref = likes_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          likes_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string likes = 4;</code>
       */
      public Builder setLikes(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        likes_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>string likes = 4;</code>
       */
      public Builder clearLikes() {
        
        likes_ = getDefaultInstance().getLikes();
        onChanged();
        return this;
      }
      /**
       * <code>string likes = 4;</code>
       */
      public Builder setLikesBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
        
        likes_ = value;
        onChanged();
        return this;
      }

      private com.google.protobuf.Struct characters_ = null;
      private com.google.protobuf.SingleFieldBuilderV3<
          com.google.protobuf.Struct, com.google.protobuf.Struct.Builder, com.google.protobuf.StructOrBuilder> charactersBuilder_;
      /**
       * <code>.google.protobuf.Struct characters = 5;</code>
       */
      public boolean hasCharacters() {
        return charactersBuilder_ != null || characters_ != null;
      }
      /**
       * <code>.google.protobuf.Struct characters = 5;</code>
       */
      public com.google.protobuf.Struct getCharacters() {
        if (charactersBuilder_ == null) {
          return characters_ == null ? com.google.protobuf.Struct.getDefaultInstance() : characters_;
        } else {
          return charactersBuilder_.getMessage();
        }
      }
      /**
       * <code>.google.protobuf.Struct characters = 5;</code>
       */
      public Builder setCharacters(com.google.protobuf.Struct value) {
        if (charactersBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          characters_ = value;
          onChanged();
        } else {
          charactersBuilder_.setMessage(value);
        }

        return this;
      }
      /**
       * <code>.google.protobuf.Struct characters = 5;</code>
       */
      public Builder setCharacters(
          com.google.protobuf.Struct.Builder builderForValue) {
        if (charactersBuilder_ == null) {
          characters_ = builderForValue.build();
          onChanged();
        } else {
          charactersBuilder_.setMessage(builderForValue.build());
        }

        return this;
      }
      /**
       * <code>.google.protobuf.Struct characters = 5;</code>
       */
      public Builder mergeCharacters(com.google.protobuf.Struct value) {
        if (charactersBuilder_ == null) {
          if (characters_ != null) {
            characters_ =
              com.google.protobuf.Struct.newBuilder(characters_).mergeFrom(value).buildPartial();
          } else {
            characters_ = value;
          }
          onChanged();
        } else {
          charactersBuilder_.mergeFrom(value);
        }

        return this;
      }
      /**
       * <code>.google.protobuf.Struct characters = 5;</code>
       */
      public Builder clearCharacters() {
        if (charactersBuilder_ == null) {
          characters_ = null;
          onChanged();
        } else {
          characters_ = null;
          charactersBuilder_ = null;
        }

        return this;
      }
      /**
       * <code>.google.protobuf.Struct characters = 5;</code>
       */
      public com.google.protobuf.Struct.Builder getCharactersBuilder() {
        
        onChanged();
        return getCharactersFieldBuilder().getBuilder();
      }
      /**
       * <code>.google.protobuf.Struct characters = 5;</code>
       */
      public com.google.protobuf.StructOrBuilder getCharactersOrBuilder() {
        if (charactersBuilder_ != null) {
          return charactersBuilder_.getMessageOrBuilder();
        } else {
          return characters_ == null ?
              com.google.protobuf.Struct.getDefaultInstance() : characters_;
        }
      }
      /**
       * <code>.google.protobuf.Struct characters = 5;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          com.google.protobuf.Struct, com.google.protobuf.Struct.Builder, com.google.protobuf.StructOrBuilder> 
          getCharactersFieldBuilder() {
        if (charactersBuilder_ == null) {
          charactersBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              com.google.protobuf.Struct, com.google.protobuf.Struct.Builder, com.google.protobuf.StructOrBuilder>(
                  getCharacters(),
                  getParentForChildren(),
                  isClean());
          characters_ = null;
        }
        return charactersBuilder_;
      }
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFieldsProto3(unknownFields);
      }

      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:gojek.test.proto.Person)
    }

    // @@protoc_insertion_point(class_scope:gojek.test.proto.Person)
    private static final com.gojek.test.proto.PersonTestProto.Person DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new com.gojek.test.proto.PersonTestProto.Person();
    }

    public static com.gojek.test.proto.PersonTestProto.Person getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<Person>
        PARSER = new com.google.protobuf.AbstractParser<Person>() {
      public Person parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Person(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<Person> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<Person> getParserForType() {
      return PARSER;
    }

    public com.gojek.test.proto.PersonTestProto.Person getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_gojek_test_proto_Person_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_gojek_test_proto_Person_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\022proto/person.proto\022\020gojek.test.proto\032\034" +
      "google/protobuf/struct.proto\"m\n\006Person\022\n" +
      "\n\002id\030\001 \001(\005\022\014\n\004name\030\002 \001(\t\022\r\n\005email\030\003 \001(\t\022" +
      "\r\n\005likes\030\004 \001(\t\022+\n\ncharacters\030\005 \001(\0132\027.goo" +
      "gle.protobuf.StructB\'\n\024com.gojek.test.pr" +
      "otoB\017PersonTestProtob\006proto3"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          com.google.protobuf.StructProto.getDescriptor(),
        }, assigner);
    internal_static_gojek_test_proto_Person_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_gojek_test_proto_Person_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_gojek_test_proto_Person_descriptor,
        new java.lang.String[] { "Id", "Name", "Email", "Likes", "Characters", });
    com.google.protobuf.StructProto.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
