package com.clb.Coprocessor;

public final class MyStatisticsInterface {
    private MyStatisticsInterface() {
    }

    public static void registerAllExtensions(
            com.google.protobuf.ExtensionRegistry registry) {
    }

    public interface getStatisticsRequestOrBuilder
            extends com.google.protobuf.MessageOrBuilder {

        // required string type = 1;

        /**
         * <code>required string type = 1;</code>
         */
        boolean hasType();

        /**
         * <code>required string type = 1;</code>
         */
        String getType();

        /**
         * <code>required string type = 1;</code>
         */
        com.google.protobuf.ByteString
        getTypeBytes();

        // optional string famillyName = 2;

        /**
         * <code>optional string famillyName = 2;</code>
         */
        boolean hasFamillyName();

        /**
         * <code>optional string famillyName = 2;</code>
         */
        String getFamillyName();

        /**
         * <code>optional string famillyName = 2;</code>
         */
        com.google.protobuf.ByteString
        getFamillyNameBytes();

        // optional string columnName = 3;

        /**
         * <code>optional string columnName = 3;</code>
         */
        boolean hasColumnName();

        /**
         * <code>optional string columnName = 3;</code>
         */
        String getColumnName();

        /**
         * <code>optional string columnName = 3;</code>
         */
        com.google.protobuf.ByteString
        getColumnNameBytes();
    }

    /**
     * Protobuf type {@code getStatisticsRequest}
     */
    public static final class getStatisticsRequest extends
            com.google.protobuf.GeneratedMessage
            implements getStatisticsRequestOrBuilder {
        // Use getStatisticsRequest.newBuilder() to construct.
        private getStatisticsRequest(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
            super(builder);
            this.unknownFields = builder.getUnknownFields();
        }

        private getStatisticsRequest(boolean noInit) {
            this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance();
        }

        private static final getStatisticsRequest defaultInstance;

        public static getStatisticsRequest getDefaultInstance() {
            return defaultInstance;
        }

        public getStatisticsRequest getDefaultInstanceForType() {
            return defaultInstance;
        }

        private final com.google.protobuf.UnknownFieldSet unknownFields;

        @Override
        public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
            return this.unknownFields;
        }

        private getStatisticsRequest(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            initFields();
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
                            if (!parseUnknownField(input, unknownFields,
                                    extensionRegistry, tag)) {
                                done = true;
                            }
                            break;
                        }
                        case 10: {
                            bitField0_ |= 0x00000001;
                            type_ = input.readBytes();
                            break;
                        }
                        case 18: {
                            bitField0_ |= 0x00000002;
                            famillyName_ = input.readBytes();
                            break;
                        }
                        case 26: {
                            bitField0_ |= 0x00000004;
                            columnName_ = input.readBytes();
                            break;
                        }
                    }
                }
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw e.setUnfinishedMessage(this);
            } catch (java.io.IOException e) {
                throw new com.google.protobuf.InvalidProtocolBufferException(
                        e.getMessage()).setUnfinishedMessage(this);
            } finally {
                this.unknownFields = unknownFields.build();
                makeExtensionsImmutable();
            }
        }

        public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
            return MyStatisticsInterface.internal_static_getStatisticsRequest_descriptor;
        }

        protected FieldAccessorTable
        internalGetFieldAccessorTable() {
            return MyStatisticsInterface.internal_static_getStatisticsRequest_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            getStatisticsRequest.class, Builder.class);
        }

        public static com.google.protobuf.Parser<getStatisticsRequest> PARSER =
                new com.google.protobuf.AbstractParser<getStatisticsRequest>() {
                    public getStatisticsRequest parsePartialFrom(
                            com.google.protobuf.CodedInputStream input,
                            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                            throws com.google.protobuf.InvalidProtocolBufferException {
                        return new getStatisticsRequest(input, extensionRegistry);
                    }
                };

        @Override
        public com.google.protobuf.Parser<getStatisticsRequest> getParserForType() {
            return PARSER;
        }

        private int bitField0_;
        // required string type = 1;
        public static final int TYPE_FIELD_NUMBER = 1;
        private Object type_;

        /**
         * <code>required string type = 1;</code>
         */
        public boolean hasType() {
            return ((bitField0_ & 0x00000001) == 0x00000001);
        }

        /**
         * <code>required string type = 1;</code>
         */
        public String getType() {
            Object ref = type_;
            if (ref instanceof String) {
                return (String) ref;
            } else {
                com.google.protobuf.ByteString bs =
                        (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                if (bs.isValidUtf8()) {
                    type_ = s;
                }
                return s;
            }
        }

        /**
         * <code>required string type = 1;</code>
         */
        public com.google.protobuf.ByteString
        getTypeBytes() {
            Object ref = type_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8(
                                (String) ref);
                type_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        // optional string famillyName = 2;
        public static final int FAMILLYNAME_FIELD_NUMBER = 2;
        private Object famillyName_;

        /**
         * <code>optional string famillyName = 2;</code>
         */
        public boolean hasFamillyName() {
            return ((bitField0_ & 0x00000002) == 0x00000002);
        }

        /**
         * <code>optional string famillyName = 2;</code>
         */
        public String getFamillyName() {
            Object ref = famillyName_;
            if (ref instanceof String) {
                return (String) ref;
            } else {
                com.google.protobuf.ByteString bs =
                        (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                if (bs.isValidUtf8()) {
                    famillyName_ = s;
                }
                return s;
            }
        }

        /**
         * <code>optional string famillyName = 2;</code>
         */
        public com.google.protobuf.ByteString
        getFamillyNameBytes() {
            Object ref = famillyName_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8(
                                (String) ref);
                famillyName_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        // optional string columnName = 3;
        public static final int COLUMNNAME_FIELD_NUMBER = 3;
        private Object columnName_;

        /**
         * <code>optional string columnName = 3;</code>
         */
        public boolean hasColumnName() {
            return ((bitField0_ & 0x00000004) == 0x00000004);
        }

        /**
         * <code>optional string columnName = 3;</code>
         */
        public String getColumnName() {
            Object ref = columnName_;
            if (ref instanceof String) {
                return (String) ref;
            } else {
                com.google.protobuf.ByteString bs =
                        (com.google.protobuf.ByteString) ref;
                String s = bs.toStringUtf8();
                if (bs.isValidUtf8()) {
                    columnName_ = s;
                }
                return s;
            }
        }

        /**
         * <code>optional string columnName = 3;</code>
         */
        public com.google.protobuf.ByteString
        getColumnNameBytes() {
            Object ref = columnName_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8(
                                (String) ref);
                columnName_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        private void initFields() {
            type_ = "";
            famillyName_ = "";
            columnName_ = "";
        }

        private byte memoizedIsInitialized = -1;

        public final boolean isInitialized() {
            byte isInitialized = memoizedIsInitialized;
            if (isInitialized != -1) return isInitialized == 1;

            if (!hasType()) {
                memoizedIsInitialized = 0;
                return false;
            }
            memoizedIsInitialized = 1;
            return true;
        }

        public void writeTo(com.google.protobuf.CodedOutputStream output)
                throws java.io.IOException {
            getSerializedSize();
            if (((bitField0_ & 0x00000001) == 0x00000001)) {
                output.writeBytes(1, getTypeBytes());
            }
            if (((bitField0_ & 0x00000002) == 0x00000002)) {
                output.writeBytes(2, getFamillyNameBytes());
            }
            if (((bitField0_ & 0x00000004) == 0x00000004)) {
                output.writeBytes(3, getColumnNameBytes());
            }
            getUnknownFields().writeTo(output);
        }

        private int memoizedSerializedSize = -1;

        public int getSerializedSize() {
            int size = memoizedSerializedSize;
            if (size != -1) return size;

            size = 0;
            if (((bitField0_ & 0x00000001) == 0x00000001)) {
                size += com.google.protobuf.CodedOutputStream
                        .computeBytesSize(1, getTypeBytes());
            }
            if (((bitField0_ & 0x00000002) == 0x00000002)) {
                size += com.google.protobuf.CodedOutputStream
                        .computeBytesSize(2, getFamillyNameBytes());
            }
            if (((bitField0_ & 0x00000004) == 0x00000004)) {
                size += com.google.protobuf.CodedOutputStream
                        .computeBytesSize(3, getColumnNameBytes());
            }
            size += getUnknownFields().getSerializedSize();
            memoizedSerializedSize = size;
            return size;
        }

        private static final long serialVersionUID = 0L;

        @Override
        protected Object writeReplace()
                throws java.io.ObjectStreamException {
            return super.writeReplace();
        }

        public static getStatisticsRequest parseFrom(
                com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static getStatisticsRequest parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static getStatisticsRequest parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static getStatisticsRequest parseFrom(
                byte[] data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static getStatisticsRequest parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return PARSER.parseFrom(input);
        }

        public static getStatisticsRequest parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return PARSER.parseFrom(input, extensionRegistry);
        }

        public static getStatisticsRequest parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return PARSER.parseDelimitedFrom(input);
        }

        public static getStatisticsRequest parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return PARSER.parseDelimitedFrom(input, extensionRegistry);
        }

        public static getStatisticsRequest parseFrom(
                com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return PARSER.parseFrom(input);
        }

        public static getStatisticsRequest parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return PARSER.parseFrom(input, extensionRegistry);
        }

        public static Builder newBuilder() {
            return Builder.create();
        }

        public Builder newBuilderForType() {
            return newBuilder();
        }

        public static Builder newBuilder(getStatisticsRequest prototype) {
            return newBuilder().mergeFrom(prototype);
        }

        public Builder toBuilder() {
            return newBuilder(this);
        }

        @Override
        protected Builder newBuilderForType(
                BuilderParent parent) {
            Builder builder = new Builder(parent);
            return builder;
        }

        /**
         * Protobuf type {@code getStatisticsRequest}
         */
        public static final class Builder extends
                com.google.protobuf.GeneratedMessage.Builder<Builder>
                implements getStatisticsRequestOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor
            getDescriptor() {
                return MyStatisticsInterface.internal_static_getStatisticsRequest_descriptor;
            }

            protected FieldAccessorTable
            internalGetFieldAccessorTable() {
                return MyStatisticsInterface.internal_static_getStatisticsRequest_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                getStatisticsRequest.class, Builder.class);
            }

            // Construct using cn.xiahu.MyStatisticsInterface.getStatisticsRequest.newBuilder()
            private Builder() {
                maybeForceBuilderInitialization();
            }

            private Builder(
                    BuilderParent parent) {
                super(parent);
                maybeForceBuilderInitialization();
            }

            private void maybeForceBuilderInitialization() {
                if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
                }
            }

            private static Builder create() {
                return new Builder();
            }

            public Builder clear() {
                super.clear();
                type_ = "";
                bitField0_ = (bitField0_ & ~0x00000001);
                famillyName_ = "";
                bitField0_ = (bitField0_ & ~0x00000002);
                columnName_ = "";
                bitField0_ = (bitField0_ & ~0x00000004);
                return this;
            }

            public Builder clone() {
                return create().mergeFrom(buildPartial());
            }

            public com.google.protobuf.Descriptors.Descriptor
            getDescriptorForType() {
                return MyStatisticsInterface.internal_static_getStatisticsRequest_descriptor;
            }

            public getStatisticsRequest getDefaultInstanceForType() {
                return getStatisticsRequest.getDefaultInstance();
            }

            public getStatisticsRequest build() {
                getStatisticsRequest result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            public getStatisticsRequest buildPartial() {
                getStatisticsRequest result = new getStatisticsRequest(this);
                int from_bitField0_ = bitField0_;
                int to_bitField0_ = 0;
                if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
                    to_bitField0_ |= 0x00000001;
                }
                result.type_ = type_;
                if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
                    to_bitField0_ |= 0x00000002;
                }
                result.famillyName_ = famillyName_;
                if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
                    to_bitField0_ |= 0x00000004;
                }
                result.columnName_ = columnName_;
                result.bitField0_ = to_bitField0_;
                onBuilt();
                return result;
            }

            public Builder mergeFrom(com.google.protobuf.Message other) {
                if (other instanceof getStatisticsRequest) {
                    return mergeFrom((getStatisticsRequest) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(getStatisticsRequest other) {
                if (other == getStatisticsRequest.getDefaultInstance()) return this;
                if (other.hasType()) {
                    bitField0_ |= 0x00000001;
                    type_ = other.type_;
                    onChanged();
                }
                if (other.hasFamillyName()) {
                    bitField0_ |= 0x00000002;
                    famillyName_ = other.famillyName_;
                    onChanged();
                }
                if (other.hasColumnName()) {
                    bitField0_ |= 0x00000004;
                    columnName_ = other.columnName_;
                    onChanged();
                }
                this.mergeUnknownFields(other.getUnknownFields());
                return this;
            }

            public final boolean isInitialized() {
                if (!hasType()) {

                    return false;
                }
                return true;
            }

            public Builder mergeFrom(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException {
                getStatisticsRequest parsedMessage = null;
                try {
                    parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    parsedMessage = (getStatisticsRequest) e.getUnfinishedMessage();
                    throw e;
                } finally {
                    if (parsedMessage != null) {
                        mergeFrom(parsedMessage);
                    }
                }
                return this;
            }

            private int bitField0_;

            // required string type = 1;
            private Object type_ = "";

            /**
             * <code>required string type = 1;</code>
             */
            public boolean hasType() {
                return ((bitField0_ & 0x00000001) == 0x00000001);
            }

            /**
             * <code>required string type = 1;</code>
             */
            public String getType() {
                Object ref = type_;
                if (!(ref instanceof String)) {
                    String s = ((com.google.protobuf.ByteString) ref)
                            .toStringUtf8();
                    type_ = s;
                    return s;
                } else {
                    return (String) ref;
                }
            }

            /**
             * <code>required string type = 1;</code>
             */
            public com.google.protobuf.ByteString
            getTypeBytes() {
                Object ref = type_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8(
                                    (String) ref);
                    type_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>required string type = 1;</code>
             */
            public Builder setType(
                    String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                bitField0_ |= 0x00000001;
                type_ = value;
                onChanged();
                return this;
            }

            /**
             * <code>required string type = 1;</code>
             */
            public Builder clearType() {
                bitField0_ = (bitField0_ & ~0x00000001);
                type_ = getDefaultInstance().getType();
                onChanged();
                return this;
            }

            /**
             * <code>required string type = 1;</code>
             */
            public Builder setTypeBytes(
                    com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                bitField0_ |= 0x00000001;
                type_ = value;
                onChanged();
                return this;
            }

            // optional string famillyName = 2;
            private Object famillyName_ = "";

            /**
             * <code>optional string famillyName = 2;</code>
             */
            public boolean hasFamillyName() {
                return ((bitField0_ & 0x00000002) == 0x00000002);
            }

            /**
             * <code>optional string famillyName = 2;</code>
             */
            public String getFamillyName() {
                Object ref = famillyName_;
                if (!(ref instanceof String)) {
                    String s = ((com.google.protobuf.ByteString) ref)
                            .toStringUtf8();
                    famillyName_ = s;
                    return s;
                } else {
                    return (String) ref;
                }
            }

            /**
             * <code>optional string famillyName = 2;</code>
             */
            public com.google.protobuf.ByteString
            getFamillyNameBytes() {
                Object ref = famillyName_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8(
                                    (String) ref);
                    famillyName_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>optional string famillyName = 2;</code>
             */
            public Builder setFamillyName(
                    String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                bitField0_ |= 0x00000002;
                famillyName_ = value;
                onChanged();
                return this;
            }

            /**
             * <code>optional string famillyName = 2;</code>
             */
            public Builder clearFamillyName() {
                bitField0_ = (bitField0_ & ~0x00000002);
                famillyName_ = getDefaultInstance().getFamillyName();
                onChanged();
                return this;
            }

            /**
             * <code>optional string famillyName = 2;</code>
             */
            public Builder setFamillyNameBytes(
                    com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                bitField0_ |= 0x00000002;
                famillyName_ = value;
                onChanged();
                return this;
            }

            // optional string columnName = 3;
            private Object columnName_ = "";

            /**
             * <code>optional string columnName = 3;</code>
             */
            public boolean hasColumnName() {
                return ((bitField0_ & 0x00000004) == 0x00000004);
            }

            /**
             * <code>optional string columnName = 3;</code>
             */
            public String getColumnName() {
                Object ref = columnName_;
                if (!(ref instanceof String)) {
                    String s = ((com.google.protobuf.ByteString) ref)
                            .toStringUtf8();
                    columnName_ = s;
                    return s;
                } else {
                    return (String) ref;
                }
            }

            /**
             * <code>optional string columnName = 3;</code>
             */
            public com.google.protobuf.ByteString
            getColumnNameBytes() {
                Object ref = columnName_;
                if (ref instanceof String) {
                    com.google.protobuf.ByteString b =
                            com.google.protobuf.ByteString.copyFromUtf8(
                                    (String) ref);
                    columnName_ = b;
                    return b;
                } else {
                    return (com.google.protobuf.ByteString) ref;
                }
            }

            /**
             * <code>optional string columnName = 3;</code>
             */
            public Builder setColumnName(
                    String value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                bitField0_ |= 0x00000004;
                columnName_ = value;
                onChanged();
                return this;
            }

            /**
             * <code>optional string columnName = 3;</code>
             */
            public Builder clearColumnName() {
                bitField0_ = (bitField0_ & ~0x00000004);
                columnName_ = getDefaultInstance().getColumnName();
                onChanged();
                return this;
            }

            /**
             * <code>optional string columnName = 3;</code>
             */
            public Builder setColumnNameBytes(
                    com.google.protobuf.ByteString value) {
                if (value == null) {
                    throw new NullPointerException();
                }
                bitField0_ |= 0x00000004;
                columnName_ = value;
                onChanged();
                return this;
            }

            // @@protoc_insertion_point(builder_scope:getStatisticsRequest)
        }

        static {
            defaultInstance = new getStatisticsRequest(true);
            defaultInstance.initFields();
        }

        // @@protoc_insertion_point(class_scope:getStatisticsRequest)
    }

    public interface getStatisticsResponseOrBuilder
            extends com.google.protobuf.MessageOrBuilder {

        // optional int64 result = 1;

        /**
         * <code>optional int64 result = 1;</code>
         */
        boolean hasResult();

        /**
         * <code>optional int64 result = 1;</code>
         */
        long getResult();
    }

    /**
     * Protobuf type {@code getStatisticsResponse}
     */
    public static final class getStatisticsResponse extends
            com.google.protobuf.GeneratedMessage
            implements getStatisticsResponseOrBuilder {
        // Use getStatisticsResponse.newBuilder() to construct.
        private getStatisticsResponse(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
            super(builder);
            this.unknownFields = builder.getUnknownFields();
        }

        private getStatisticsResponse(boolean noInit) {
            this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance();
        }

        private static final getStatisticsResponse defaultInstance;

        public static getStatisticsResponse getDefaultInstance() {
            return defaultInstance;
        }

        public getStatisticsResponse getDefaultInstanceForType() {
            return defaultInstance;
        }

        private final com.google.protobuf.UnknownFieldSet unknownFields;

        @Override
        public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
            return this.unknownFields;
        }

        private getStatisticsResponse(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            initFields();
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
                            if (!parseUnknownField(input, unknownFields,
                                    extensionRegistry, tag)) {
                                done = true;
                            }
                            break;
                        }
                        case 8: {
                            bitField0_ |= 0x00000001;
                            result_ = input.readInt64();
                            break;
                        }
                    }
                }
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw e.setUnfinishedMessage(this);
            } catch (java.io.IOException e) {
                throw new com.google.protobuf.InvalidProtocolBufferException(
                        e.getMessage()).setUnfinishedMessage(this);
            } finally {
                this.unknownFields = unknownFields.build();
                makeExtensionsImmutable();
            }
        }

        public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
            return MyStatisticsInterface.internal_static_getStatisticsResponse_descriptor;
        }

        protected FieldAccessorTable
        internalGetFieldAccessorTable() {
            return MyStatisticsInterface.internal_static_getStatisticsResponse_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            getStatisticsResponse.class, Builder.class);
        }

        public static com.google.protobuf.Parser<getStatisticsResponse> PARSER =
                new com.google.protobuf.AbstractParser<getStatisticsResponse>() {
                    public getStatisticsResponse parsePartialFrom(
                            com.google.protobuf.CodedInputStream input,
                            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                            throws com.google.protobuf.InvalidProtocolBufferException {
                        return new getStatisticsResponse(input, extensionRegistry);
                    }
                };

        @Override
        public com.google.protobuf.Parser<getStatisticsResponse> getParserForType() {
            return PARSER;
        }

        private int bitField0_;
        // optional int64 result = 1;
        public static final int RESULT_FIELD_NUMBER = 1;
        private long result_;

        /**
         * <code>optional int64 result = 1;</code>
         */
        public boolean hasResult() {
            return ((bitField0_ & 0x00000001) == 0x00000001);
        }

        /**
         * <code>optional int64 result = 1;</code>
         */
        public long getResult() {
            return result_;
        }

        private void initFields() {
            result_ = 0L;
        }

        private byte memoizedIsInitialized = -1;

        public final boolean isInitialized() {
            byte isInitialized = memoizedIsInitialized;
            if (isInitialized != -1) return isInitialized == 1;

            memoizedIsInitialized = 1;
            return true;
        }

        public void writeTo(com.google.protobuf.CodedOutputStream output)
                throws java.io.IOException {
            getSerializedSize();
            if (((bitField0_ & 0x00000001) == 0x00000001)) {
                output.writeInt64(1, result_);
            }
            getUnknownFields().writeTo(output);
        }

        private int memoizedSerializedSize = -1;

        public int getSerializedSize() {
            int size = memoizedSerializedSize;
            if (size != -1) return size;

            size = 0;
            if (((bitField0_ & 0x00000001) == 0x00000001)) {
                size += com.google.protobuf.CodedOutputStream
                        .computeInt64Size(1, result_);
            }
            size += getUnknownFields().getSerializedSize();
            memoizedSerializedSize = size;
            return size;
        }

        private static final long serialVersionUID = 0L;

        @Override
        protected Object writeReplace()
                throws java.io.ObjectStreamException {
            return super.writeReplace();
        }

        public static getStatisticsResponse parseFrom(
                com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static getStatisticsResponse parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static getStatisticsResponse parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data);
        }

        public static getStatisticsResponse parseFrom(
                byte[] data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return PARSER.parseFrom(data, extensionRegistry);
        }

        public static getStatisticsResponse parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return PARSER.parseFrom(input);
        }

        public static getStatisticsResponse parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return PARSER.parseFrom(input, extensionRegistry);
        }

        public static getStatisticsResponse parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return PARSER.parseDelimitedFrom(input);
        }

        public static getStatisticsResponse parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return PARSER.parseDelimitedFrom(input, extensionRegistry);
        }

        public static getStatisticsResponse parseFrom(
                com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return PARSER.parseFrom(input);
        }

        public static getStatisticsResponse parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return PARSER.parseFrom(input, extensionRegistry);
        }

        public static Builder newBuilder() {
            return Builder.create();
        }

        public Builder newBuilderForType() {
            return newBuilder();
        }

        public static Builder newBuilder(getStatisticsResponse prototype) {
            return newBuilder().mergeFrom(prototype);
        }

        public Builder toBuilder() {
            return newBuilder(this);
        }

        @Override
        protected Builder newBuilderForType(
                BuilderParent parent) {
            Builder builder = new Builder(parent);
            return builder;
        }

        /**
         * Protobuf type {@code getStatisticsResponse}
         */
        public static final class Builder extends
                com.google.protobuf.GeneratedMessage.Builder<Builder>
                implements getStatisticsResponseOrBuilder {
            public static final com.google.protobuf.Descriptors.Descriptor
            getDescriptor() {
                return MyStatisticsInterface.internal_static_getStatisticsResponse_descriptor;
            }

            protected FieldAccessorTable
            internalGetFieldAccessorTable() {
                return MyStatisticsInterface.internal_static_getStatisticsResponse_fieldAccessorTable
                        .ensureFieldAccessorsInitialized(
                                getStatisticsResponse.class, Builder.class);
            }

            // Construct using cn.xiahu.MyStatisticsInterface.getStatisticsResponse.newBuilder()
            private Builder() {
                maybeForceBuilderInitialization();
            }

            private Builder(
                    BuilderParent parent) {
                super(parent);
                maybeForceBuilderInitialization();
            }

            private void maybeForceBuilderInitialization() {
                if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
                }
            }

            private static Builder create() {
                return new Builder();
            }

            public Builder clear() {
                super.clear();
                result_ = 0L;
                bitField0_ = (bitField0_ & ~0x00000001);
                return this;
            }

            public Builder clone() {
                return create().mergeFrom(buildPartial());
            }

            public com.google.protobuf.Descriptors.Descriptor
            getDescriptorForType() {
                return MyStatisticsInterface.internal_static_getStatisticsResponse_descriptor;
            }

            public getStatisticsResponse getDefaultInstanceForType() {
                return getStatisticsResponse.getDefaultInstance();
            }

            public getStatisticsResponse build() {
                getStatisticsResponse result = buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                }
                return result;
            }

            public getStatisticsResponse buildPartial() {
                getStatisticsResponse result = new getStatisticsResponse(this);
                int from_bitField0_ = bitField0_;
                int to_bitField0_ = 0;
                if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
                    to_bitField0_ |= 0x00000001;
                }
                result.result_ = result_;
                result.bitField0_ = to_bitField0_;
                onBuilt();
                return result;
            }

            public Builder mergeFrom(com.google.protobuf.Message other) {
                if (other instanceof getStatisticsResponse) {
                    return mergeFrom((getStatisticsResponse) other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(getStatisticsResponse other) {
                if (other == getStatisticsResponse.getDefaultInstance()) return this;
                if (other.hasResult()) {
                    setResult(other.getResult());
                }
                this.mergeUnknownFields(other.getUnknownFields());
                return this;
            }

            public final boolean isInitialized() {
                return true;
            }

            public Builder mergeFrom(
                    com.google.protobuf.CodedInputStream input,
                    com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                    throws java.io.IOException {
                getStatisticsResponse parsedMessage = null;
                try {
                    parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    parsedMessage = (getStatisticsResponse) e.getUnfinishedMessage();
                    throw e;
                } finally {
                    if (parsedMessage != null) {
                        mergeFrom(parsedMessage);
                    }
                }
                return this;
            }

            private int bitField0_;

            // optional int64 result = 1;
            private long result_;

            /**
             * <code>optional int64 result = 1;</code>
             */
            public boolean hasResult() {
                return ((bitField0_ & 0x00000001) == 0x00000001);
            }

            /**
             * <code>optional int64 result = 1;</code>
             */
            public long getResult() {
                return result_;
            }

            /**
             * <code>optional int64 result = 1;</code>
             */
            public Builder setResult(long value) {
                bitField0_ |= 0x00000001;
                result_ = value;
                onChanged();
                return this;
            }

            /**
             * <code>optional int64 result = 1;</code>
             */
            public Builder clearResult() {
                bitField0_ = (bitField0_ & ~0x00000001);
                result_ = 0L;
                onChanged();
                return this;
            }

            // @@protoc_insertion_point(builder_scope:getStatisticsResponse)
        }

        static {
            defaultInstance = new getStatisticsResponse(true);
            defaultInstance.initFields();
        }

        // @@protoc_insertion_point(class_scope:getStatisticsResponse)
    }

    /**
     * Protobuf service {@code myStatisticsService}
     */
    public static abstract class myStatisticsService
            implements com.google.protobuf.Service {
        protected myStatisticsService() {
        }

        public interface Interface {
            /**
             * <code>rpc getStatisticsResult(.getStatisticsRequest) returns (.getStatisticsResponse);</code>
             */
            public abstract void getStatisticsResult(
                    com.google.protobuf.RpcController controller,
                    getStatisticsRequest request,
                    com.google.protobuf.RpcCallback<getStatisticsResponse> done);

        }

        public static com.google.protobuf.Service newReflectiveService(
                final Interface impl) {
            return new myStatisticsService() {
                @Override
                public void getStatisticsResult(
                        com.google.protobuf.RpcController controller,
                        getStatisticsRequest request,
                        com.google.protobuf.RpcCallback<getStatisticsResponse> done) {
                    impl.getStatisticsResult(controller, request, done);
                }

            };
        }

        public static com.google.protobuf.BlockingService
        newReflectiveBlockingService(final BlockingInterface impl) {
            return new com.google.protobuf.BlockingService() {
                public final com.google.protobuf.Descriptors.ServiceDescriptor
                getDescriptorForType() {
                    return getDescriptor();
                }

                public final com.google.protobuf.Message callBlockingMethod(
                        com.google.protobuf.Descriptors.MethodDescriptor method,
                        com.google.protobuf.RpcController controller,
                        com.google.protobuf.Message request)
                        throws com.google.protobuf.ServiceException {
                    if (method.getService() != getDescriptor()) {
                        throw new IllegalArgumentException(
                                "Service.callBlockingMethod() given method descriptor for " +
                                        "wrong service type.");
                    }
                    switch (method.getIndex()) {
                        case 0:
                            return impl.getStatisticsResult(controller, (getStatisticsRequest) request);
                        default:
                            throw new AssertionError("Can't get here.");
                    }
                }

                public final com.google.protobuf.Message
                getRequestPrototype(
                        com.google.protobuf.Descriptors.MethodDescriptor method) {
                    if (method.getService() != getDescriptor()) {
                        throw new IllegalArgumentException(
                                "Service.getRequestPrototype() given method " +
                                        "descriptor for wrong service type.");
                    }
                    switch (method.getIndex()) {
                        case 0:
                            return getStatisticsRequest.getDefaultInstance();
                        default:
                            throw new AssertionError("Can't get here.");
                    }
                }

                public final com.google.protobuf.Message
                getResponsePrototype(
                        com.google.protobuf.Descriptors.MethodDescriptor method) {
                    if (method.getService() != getDescriptor()) {
                        throw new IllegalArgumentException(
                                "Service.getResponsePrototype() given method " +
                                        "descriptor for wrong service type.");
                    }
                    switch (method.getIndex()) {
                        case 0:
                            return getStatisticsResponse.getDefaultInstance();
                        default:
                            throw new AssertionError("Can't get here.");
                    }
                }

            };
        }

        /**
         * <code>rpc getStatisticsResult(.getStatisticsRequest) returns (.getStatisticsResponse);</code>
         */
        public abstract void getStatisticsResult(
                com.google.protobuf.RpcController controller,
                getStatisticsRequest request,
                com.google.protobuf.RpcCallback<getStatisticsResponse> done);

        public static final com.google.protobuf.Descriptors.ServiceDescriptor
        getDescriptor() {
            return MyStatisticsInterface.getDescriptor().getServices().get(0);
        }

        public final com.google.protobuf.Descriptors.ServiceDescriptor
        getDescriptorForType() {
            return getDescriptor();
        }

        public final void callMethod(
                com.google.protobuf.Descriptors.MethodDescriptor method,
                com.google.protobuf.RpcController controller,
                com.google.protobuf.Message request,
                com.google.protobuf.RpcCallback<
                        com.google.protobuf.Message> done) {
            if (method.getService() != getDescriptor()) {
                throw new IllegalArgumentException(
                        "Service.callMethod() given method descriptor for wrong " +
                                "service type.");
            }
            switch (method.getIndex()) {
                case 0:
                    this.getStatisticsResult(controller, (getStatisticsRequest) request,
                            com.google.protobuf.RpcUtil.<getStatisticsResponse>specializeCallback(
                                    done));
                    return;
                default:
                    throw new AssertionError("Can't get here.");
            }
        }

        public final com.google.protobuf.Message
        getRequestPrototype(
                com.google.protobuf.Descriptors.MethodDescriptor method) {
            if (method.getService() != getDescriptor()) {
                throw new IllegalArgumentException(
                        "Service.getRequestPrototype() given method " +
                                "descriptor for wrong service type.");
            }
            switch (method.getIndex()) {
                case 0:
                    return getStatisticsRequest.getDefaultInstance();
                default:
                    throw new AssertionError("Can't get here.");
            }
        }

        public final com.google.protobuf.Message
        getResponsePrototype(
                com.google.protobuf.Descriptors.MethodDescriptor method) {
            if (method.getService() != getDescriptor()) {
                throw new IllegalArgumentException(
                        "Service.getResponsePrototype() given method " +
                                "descriptor for wrong service type.");
            }
            switch (method.getIndex()) {
                case 0:
                    return getStatisticsResponse.getDefaultInstance();
                default:
                    throw new AssertionError("Can't get here.");
            }
        }

        public static Stub newStub(
                com.google.protobuf.RpcChannel channel) {
            return new Stub(channel);
        }

        public static final class Stub extends myStatisticsService implements Interface {
            private Stub(com.google.protobuf.RpcChannel channel) {
                this.channel = channel;
            }

            private final com.google.protobuf.RpcChannel channel;

            public com.google.protobuf.RpcChannel getChannel() {
                return channel;
            }

            public void getStatisticsResult(
                    com.google.protobuf.RpcController controller,
                    getStatisticsRequest request,
                    com.google.protobuf.RpcCallback<getStatisticsResponse> done) {
                channel.callMethod(
                        getDescriptor().getMethods().get(0),
                        controller,
                        request,
                        getStatisticsResponse.getDefaultInstance(),
                        com.google.protobuf.RpcUtil.generalizeCallback(
                                done,
                                getStatisticsResponse.class,
                                getStatisticsResponse.getDefaultInstance()));
            }
        }

        public static BlockingInterface newBlockingStub(
                com.google.protobuf.BlockingRpcChannel channel) {
            return new BlockingStub(channel);
        }

        public interface BlockingInterface {
            public getStatisticsResponse getStatisticsResult(
                    com.google.protobuf.RpcController controller,
                    getStatisticsRequest request)
                    throws com.google.protobuf.ServiceException;
        }

        private static final class BlockingStub implements BlockingInterface {
            private BlockingStub(com.google.protobuf.BlockingRpcChannel channel) {
                this.channel = channel;
            }

            private final com.google.protobuf.BlockingRpcChannel channel;

            public getStatisticsResponse getStatisticsResult(
                    com.google.protobuf.RpcController controller,
                    getStatisticsRequest request)
                    throws com.google.protobuf.ServiceException {
                return (getStatisticsResponse) channel.callBlockingMethod(
                        getDescriptor().getMethods().get(0),
                        controller,
                        request,
                        getStatisticsResponse.getDefaultInstance());
            }

        }

        // @@protoc_insertion_point(class_scope:myStatisticsService)
    }

    private static com.google.protobuf.Descriptors.Descriptor
            internal_static_getStatisticsRequest_descriptor;
    private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
            internal_static_getStatisticsRequest_fieldAccessorTable;
    private static com.google.protobuf.Descriptors.Descriptor
            internal_static_getStatisticsResponse_descriptor;
    private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
            internal_static_getStatisticsResponse_fieldAccessorTable;

    public static com.google.protobuf.Descriptors.FileDescriptor
    getDescriptor() {
        return descriptor;
    }

    private static com.google.protobuf.Descriptors.FileDescriptor
            descriptor;

    static {
        String[] descriptorData = {
                "\n\020statistics.proto\"M\n\024getStatisticsReque" +
                        "st\022\014\n\004type\030\001 \002(\t\022\023\n\013famillyName\030\002 \001(\t\022\022\n" +
                        "\ncolumnName\030\003 \001(\t\"\'\n\025getStatisticsRespon" +
                        "se\022\016\n\006result\030\001 \001(\0032[\n\023myStatisticsServic" +
                        "e\022D\n\023getStatisticsResult\022\025.getStatistics" +
                        "Request\032\026.getStatisticsResponseBJ\n,cn.co" +
                        "m.newbee.feng.Statistics.protointerfaceB" +
                        "\025MyStatisticsInterfaceH\001\210\001\001"
        };
        com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
                new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
                    public com.google.protobuf.ExtensionRegistry assignDescriptors(
                            com.google.protobuf.Descriptors.FileDescriptor root) {
                        descriptor = root;
                        internal_static_getStatisticsRequest_descriptor =
                                getDescriptor().getMessageTypes().get(0);
                        internal_static_getStatisticsRequest_fieldAccessorTable = new
                                com.google.protobuf.GeneratedMessage.FieldAccessorTable(
                                internal_static_getStatisticsRequest_descriptor,
                                new String[]{"Type", "FamillyName", "ColumnName",});
                        internal_static_getStatisticsResponse_descriptor =
                                getDescriptor().getMessageTypes().get(1);
                        internal_static_getStatisticsResponse_fieldAccessorTable = new
                                com.google.protobuf.GeneratedMessage.FieldAccessorTable(
                                internal_static_getStatisticsResponse_descriptor,
                                new String[]{"Result",});
                        return null;
                    }
                };
        com.google.protobuf.Descriptors.FileDescriptor
                .internalBuildGeneratedFileFrom(descriptorData,
                        new com.google.protobuf.Descriptors.FileDescriptor[]{
                        }, assigner);
    }

    // @@protoc_insertion_point(outer_class_scope)
}
