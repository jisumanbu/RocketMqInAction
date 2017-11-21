package com.luodifz.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;

/**
 */
public class JsonUtil {

    private static final Logger logger = LoggerFactory.getLogger(JsonUtil.class);

    private static Gson gson = null;

    static {
        gson = newGson();
    }

    private static Gson newPrettyGson() {
        return getGsonBuilder()

                .setPrettyPrinting()

                .serializeSpecialFloatingPointValues()

                .create();
    }

    private static Gson newGson() {
        return getGsonBuilder()

                .serializeSpecialFloatingPointValues()
                .create();
    }

    protected static GsonBuilder getGsonBuilder() {

        return new GsonBuilder()

                // .setPrettyPrinting()

                .registerTypeHierarchyAdapter(Integer.class, new TypeAdapter<Integer>() {

                    @Override
                    public Integer read(JsonReader in) throws IOException {
                        if (in.peek() == JsonToken.NULL) {
                            in.nextNull();
                            return null;
                        }
                        String value = in.nextString();
                        if (StringUtils.isBlank(value)) {
                            return null;
                        } else {
                            try {
                                return new Integer(value);
                            } catch (NumberFormatException e) {
                                if (logger.isDebugEnabled()) logger.debug("JSON error: ", e);
                                return null;
                            }
                        }
                    }

                    @Override
                    public void write(JsonWriter out, Integer value) throws IOException {
                        out.value(value);
                    }

                })

                .registerTypeAdapter(BigDecimal.class, new TypeAdapter<BigDecimal>() {

                    @Override
                    public BigDecimal read(JsonReader in) throws IOException {
                        if (in.peek() == JsonToken.NULL) {
                            in.nextNull();
                            return null;
                        }
                        String value = in.nextString();
                        if (StringUtils.isBlank(value)) {
                            return null;
                        } else {
                            try {
                                return new BigDecimal(value);
                            } catch (NumberFormatException e) {
                                if (logger.isDebugEnabled()) logger.debug("JSON error: ", e);
                                return null;
                            }
                        }
                    }

                    @Override
                    public void write(JsonWriter out, BigDecimal value) throws IOException {
                        out.value(value);
                    }

                })
                ;
    }

    public static <T> String toJson(T src) {
        return gson.toJson(src);
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return gson.fromJson(json, clazz);
        } catch (Throwable e) {
            logger.error("解析 JSON 出错，class > " + clazz.getSimpleName() + ", json > " + json);
            throw new RuntimeException(e);
        }
    }
}
