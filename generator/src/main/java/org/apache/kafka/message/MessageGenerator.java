/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.message;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * The Kafka message generator.
 */
public final class MessageGenerator {
    static final String JSON_SUFFIX = ".json";

    static final String JSON_GLOB = "*" + JSON_SUFFIX;

    static final String JAVA_SUFFIX = ".java";

    static final String API_MESSAGE_TYPE_JAVA = "ApiMessageType.java";

    static final String API_SCOPE_JAVA = "ApiScope.java";

    static final String COORDINATOR_RECORD_TYPE_JAVA = "CoordinatorRecordType.java";

    static final String COORDINATOR_RECORD_JSON_CONVERTERS_JAVA = "CoordinatorRecordJsonConverters.java";

    static final String METADATA_RECORD_TYPE_JAVA = "MetadataRecordType.java";

    static final String METADATA_JSON_CONVERTERS_JAVA = "MetadataJsonConverters.java";

    static final String DEFAULT_KAFKA_BASE_PACKAGE = "org.apache.kafka";

    // Mutable so callers can redirect generated imports to a different base package via setKafkaBasePackage().
    // Suffix mappings are defined solely in setKafkaBasePackage(); the static initializer seeds the defaults.
    static String API_MESSAGE_CLASS;
    static String MESSAGE_CLASS;
    static String MESSAGE_UTIL_CLASS;
    static String READABLE_CLASS;
    static String WRITABLE_CLASS;
    static String IMPLICIT_LINKED_HASH_COLLECTION_CLASS;
    static String IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS;
    static String UNSUPPORTED_VERSION_EXCEPTION_CLASS;
    static String TYPE_CLASS;
    static String FIELD_CLASS;
    static String SCHEMA_CLASS;
    static String NULLABLE_SCHEMA_CLASS;
    static String ARRAYOF_CLASS;
    static String COMPACT_ARRAYOF_CLASS;
    static String BYTES_CLASS;
    static String UUID_CLASS;
    static String BASE_RECORDS_CLASS;
    static String MEMORY_RECORDS_CLASS;
    static String BYTE_UTILS_CLASS;
    static String TAGGED_FIELDS_SECTION_CLASS;
    static String OBJECT_SERIALIZATION_CACHE_CLASS;
    static String MESSAGE_SIZE_ACCUMULATOR_CLASS;
    static String RAW_TAGGED_FIELD_CLASS;
    static String RAW_TAGGED_FIELD_WRITER_CLASS;

    static {
        setKafkaBasePackage(DEFAULT_KAFKA_BASE_PACKAGE);
    }

    static final String ARRAYS_CLASS = "java.util.Arrays";

    static final String OBJECTS_CLASS = "java.util.Objects";

    static final String LIST_CLASS = "java.util.List";

    static final String ARRAYLIST_CLASS = "java.util.ArrayList";

    static final String ITERATOR_CLASS = "java.util.Iterator";

    static final String ENUM_SET_CLASS = "java.util.EnumSet";

    static final String REQUEST_SUFFIX = "Request";

    static final String RESPONSE_SUFFIX = "Response";

    static final String STANDARD_CHARSETS = "java.nio.charset.StandardCharsets";

    static final String TREE_MAP_CLASS = "java.util.TreeMap";

    static final String BYTE_BUFFER_CLASS = "java.nio.ByteBuffer";

    static final String NAVIGABLE_MAP_CLASS = "java.util.NavigableMap";

    static final String MAP_ENTRY_CLASS = "java.util.Map.Entry";

    static final String JSON_NODE_CLASS = "com.fasterxml.jackson.databind.JsonNode";

    static final String OBJECT_NODE_CLASS = "com.fasterxml.jackson.databind.node.ObjectNode";

    static final String JSON_NODE_FACTORY_CLASS = "com.fasterxml.jackson.databind.node.JsonNodeFactory";

    static final String BOOLEAN_NODE_CLASS = "com.fasterxml.jackson.databind.node.BooleanNode";

    static final String SHORT_NODE_CLASS = "com.fasterxml.jackson.databind.node.ShortNode";

    static final String INT_NODE_CLASS = "com.fasterxml.jackson.databind.node.IntNode";

    static final String LONG_NODE_CLASS = "com.fasterxml.jackson.databind.node.LongNode";

    static final String TEXT_NODE_CLASS = "com.fasterxml.jackson.databind.node.TextNode";

    static final String BINARY_NODE_CLASS = "com.fasterxml.jackson.databind.node.BinaryNode";

    static final String NULL_NODE_CLASS = "com.fasterxml.jackson.databind.node.NullNode";

    static final String ARRAY_NODE_CLASS = "com.fasterxml.jackson.databind.node.ArrayNode";

    static final String DOUBLE_NODE_CLASS = "com.fasterxml.jackson.databind.node.DoubleNode";

    static final long UNSIGNED_INT_MAX = 4294967295L;

    static final int UNSIGNED_SHORT_MAX = 65535;

    /**
     * The Jackson serializer we use for JSON objects.
     */
    public static final ObjectMapper JSON_SERDE;

    static {
        JSON_SERDE = new ObjectMapper();
        JSON_SERDE.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JSON_SERDE.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        JSON_SERDE.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true);
        JSON_SERDE.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        JSON_SERDE.setDefaultPropertyInclusion(JsonInclude.Include.NON_EMPTY);
        JSON_SERDE.registerModule(new Jdk8Module());
    }

    private static List<TypeClassGenerator> createTypeClassGenerators(String packageName,
                                                                      List<String> types) {
        if (types == null) return List.of();
        List<TypeClassGenerator> generators = new ArrayList<>();
        for (String type : types) {
            switch (type) {
                case "ApiMessageTypeGenerator":
                    generators.add(new ApiMessageTypeGenerator(packageName));
                    break;
                case "MetadataRecordTypeGenerator":
                    generators.add(new MetadataRecordTypeGenerator(packageName));
                    break;
                case "MetadataJsonConvertersGenerator":
                    generators.add(new MetadataJsonConvertersGenerator(packageName));
                    break;
                case "CoordinatorRecordTypeGenerator":
                    generators.add(new CoordinatorRecordTypeGenerator(packageName));
                    break;
                case "CoordinatorRecordJsonConvertersGenerator":
                    generators.add(new CoordinatorRecordJsonConvertersGenerator(packageName));
                    break;
                default:
                    throw new RuntimeException("Unknown type class generator type '" + type + "'");
            }
        }
        return generators;
    }

    private static List<MessageClassGenerator> createMessageClassGenerators(String packageName,
                                                                            List<String> types) {
        if (types == null) return List.of();
        List<MessageClassGenerator> generators = new ArrayList<>();
        for (String type : types) {
            switch (type) {
                case "MessageDataGenerator":
                    generators.add(new MessageDataGenerator(packageName));
                    break;
                case "JsonConverterGenerator":
                    generators.add(new JsonConverterGenerator(packageName));
                    break;
                default:
                    throw new RuntimeException("Unknown message class generator type '" + type + "'");
            }
        }
        return generators;
    }

    public static void processDirectories(String packageName,
                                          String outputDir,
                                          String inputDir,
                                          List<String> typeClassGeneratorTypes,
                                          List<String> messageClassGeneratorTypes) throws Exception {
        Files.createDirectories(Paths.get(outputDir));
        int numProcessed = 0;

        List<TypeClassGenerator> typeClassGenerators = createTypeClassGenerators(packageName, typeClassGeneratorTypes);
        Set<String> outputFileNames = new HashSet<>();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(inputDir), JSON_GLOB)) {
            for (Path inputPath : directoryStream) {
                try {
                    MessageSpec spec = JSON_SERDE.readValue(inputPath.toFile(), MessageSpec.class);
                    outputFileNames.addAll(
                        generateAndWriteMessageClasses(spec, packageName, outputDir, messageClassGeneratorTypes));
                    numProcessed++;
                    typeClassGenerators.forEach(generator -> generator.registerMessageType(spec));
                } catch (Exception e) {
                    throw new RuntimeException("Exception while processing " + inputPath.toString(), e);
                }
            }
        }
        for (TypeClassGenerator typeClassGenerator : typeClassGenerators) {
            outputFileNames.add(typeClassGenerator.outputName());
            generateAndWriteTypeClasses(outputDir, typeClassGenerator);
        }

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(outputDir))) {
            for (Path outputPath : directoryStream) {
                Path fileName = outputPath.getFileName();
                if (fileName != null) {
                    if (!outputFileNames.contains(fileName.toString())) {
                        Files.delete(outputPath);
                    }
                }
            }
        }
        System.out.printf("MessageGenerator: processed %d Kafka message JSON file(s).%n", numProcessed);
    }

    /**
     * Generate and write message classes.
     * @return output file names.
     */
    static Set<String> generateAndWriteMessageClasses(MessageSpec spec,
                                                      String packageName,
                                                      String outputDir,
                                                      List<String> messageClassGeneratorTypes) throws Exception {
        var outputFileNames = new HashSet<String>();
        // Only generate `Data` classes for apis that have valid version(s)
        if (spec.hasValidVersion()) {
            List<MessageClassGenerator> generators = createMessageClassGenerators(packageName, messageClassGeneratorTypes);
            for (MessageClassGenerator generator : generators) {
                String name = generator.outputName(spec) + JAVA_SUFFIX;
                outputFileNames.add(name);
                Path outputPath = Paths.get(outputDir, name);
                try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
                    generator.generateAndWrite(spec, writer);
                }
            }
        }
        return outputFileNames;
    }

    static void generateAndWriteTypeClasses(String outputDir, TypeClassGenerator typeClassGenerator) throws IOException {
        Path factoryOutputPath = Paths.get(outputDir, typeClassGenerator.outputName());
        try (BufferedWriter writer = Files.newBufferedWriter(factoryOutputPath, StandardCharsets.UTF_8)) {
            typeClassGenerator.generateAndWrite(writer);
        }
    }

    public static String capitalizeFirst(String string) {
        if (string.isEmpty()) {
            return string;
        }
        return string.substring(0, 1).toUpperCase(Locale.ENGLISH) +
            string.substring(1);
    }

    static String lowerCaseFirst(String string) {
        if (string.isEmpty()) {
            return string;
        }
        return string.substring(0, 1).toLowerCase(Locale.ENGLISH) +
            string.substring(1);
    }

    static boolean firstIsCapitalized(String string) {
        if (string.isEmpty()) {
            return false;
        }
        return Character.isUpperCase(string.charAt(0));
    }

    static String toSnakeCase(String string) {
        StringBuilder bld = new StringBuilder();
        boolean prevWasCapitalized = true;
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            if (Character.isUpperCase(c)) {
                if (!prevWasCapitalized) {
                    bld.append('_');
                }
                bld.append(Character.toLowerCase(c));
                prevWasCapitalized = true;
            } else {
                bld.append(c);
                prevWasCapitalized = false;
            }
        }
        return bld.toString();
    }

    static String stripSuffix(String str, String suffix) {
        if (str.endsWith(suffix)) {
            return str.substring(0, str.length() - suffix.length());
        } else {
            throw new RuntimeException("String " + str + " does not end with the " +
                "expected suffix " + suffix);
        }
    }

    /**
     * Reconfigure all Kafka-specific class name constants to use the given base package instead of
     * the default {@value #DEFAULT_KAFKA_BASE_PACKAGE}. Must be called before any generators are
     * instantiated. Useful when generating code for a fork or a shaded distribution.
     */
    public static void setKafkaBasePackage(String basePackage) {
        API_MESSAGE_CLASS = basePackage + ".common.protocol.ApiMessage";
        MESSAGE_CLASS = basePackage + ".common.protocol.Message";
        MESSAGE_UTIL_CLASS = basePackage + ".common.protocol.MessageUtil";
        READABLE_CLASS = basePackage + ".common.protocol.Readable";
        WRITABLE_CLASS = basePackage + ".common.protocol.Writable";
        IMPLICIT_LINKED_HASH_COLLECTION_CLASS = basePackage + ".common.utils.ImplicitLinkedHashCollection";
        IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS = basePackage + ".common.utils.ImplicitLinkedHashMultiCollection";
        UNSUPPORTED_VERSION_EXCEPTION_CLASS = basePackage + ".common.errors.UnsupportedVersionException";
        TYPE_CLASS = basePackage + ".common.protocol.types.Type";
        FIELD_CLASS = basePackage + ".common.protocol.types.Field";
        SCHEMA_CLASS = basePackage + ".common.protocol.types.Schema";
        NULLABLE_SCHEMA_CLASS = basePackage + ".common.protocol.types.NullableSchema";
        ARRAYOF_CLASS = basePackage + ".common.protocol.types.ArrayOf";
        COMPACT_ARRAYOF_CLASS = basePackage + ".common.protocol.types.CompactArrayOf";
        BYTES_CLASS = basePackage + ".common.utils.Bytes";
        UUID_CLASS = basePackage + ".common.Uuid";
        BASE_RECORDS_CLASS = basePackage + ".common.record.internal.BaseRecords";
        MEMORY_RECORDS_CLASS = basePackage + ".common.record.internal.MemoryRecords";
        BYTE_UTILS_CLASS = basePackage + ".common.utils.ByteUtils";
        TAGGED_FIELDS_SECTION_CLASS = basePackage + ".common.protocol.types.Field.TaggedFieldsSection";
        OBJECT_SERIALIZATION_CACHE_CLASS = basePackage + ".common.protocol.ObjectSerializationCache";
        MESSAGE_SIZE_ACCUMULATOR_CLASS = basePackage + ".common.protocol.MessageSizeAccumulator";
        RAW_TAGGED_FIELD_CLASS = basePackage + ".common.protocol.types.RawTaggedField";
        RAW_TAGGED_FIELD_WRITER_CLASS = basePackage + ".common.protocol.types.RawTaggedFieldWriter";
    }

    /**
     * Return the number of bytes needed to encode an integer in unsigned variable-length format.
     */
    static int sizeOfUnsignedVarint(int value) {
        int bytes = 1;
        while ((value & 0xffffff80) != 0L) {
            bytes += 1;
            value >>>= 7;
        }
        return bytes;
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("message-generator")
            .defaultHelp(true)
            .description("The Kafka message generator");
        parser.addArgument("--package", "-p")
            .action(store())
            .required(true)
            .metavar("PACKAGE")
            .help("The java package to use in generated files.");
        parser.addArgument("--output", "-o")
            .action(store())
            .required(true)
            .metavar("OUTPUT")
            .help("The output directory to create.");
        parser.addArgument("--input", "-i")
            .action(store())
            .required(true)
            .metavar("INPUT")
            .help("The input directory to use.");
        parser.addArgument("--typeclass-generators", "-t")
            .nargs("+")
            .action(store())
            .metavar("TYPECLASS_GENERATORS")
            .help("The type class generators to use, if any.");
        parser.addArgument("--message-class-generators", "-m")
            .nargs("+")
            .action(store())
            .metavar("MESSAGE_CLASS_GENERATORS")
            .help("The message class generators to use.");
        parser.addArgument("--base-package", "-b")
            .action(store())
            .setDefault(DEFAULT_KAFKA_BASE_PACKAGE)
            .metavar("BASE_PACKAGE")
            .help("The base Java package for Kafka runtime classes referenced in generated code " +
                "(default: " + DEFAULT_KAFKA_BASE_PACKAGE + "). Override when generating for a fork " +
                "or shaded distribution whose runtime classes live under a different root package.");
        Namespace res = parser.parseArgsOrFail(args);
        setKafkaBasePackage(res.getString("base_package"));
        processDirectories(res.getString("package"), res.getString("output"),
            res.getString("input"), res.getList("typeclass_generators"),
            res.getList("message_class_generators"));
    }
}
