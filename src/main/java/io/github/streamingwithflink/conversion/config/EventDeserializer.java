package io.github.streamingwithflink.conversion.config;

import io.github.streamingwithflink.conversion.model.Event;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class EventDeserializer implements DeserializationSchema<Event> {

    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Event object = null;
        try {
            object = mapper.readValue(bytes, Event.class);
        } catch (Exception exception) {
            System.out.println("Error in deserializing bytes " + exception);
        }
        return object;
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return null;
    }
}
