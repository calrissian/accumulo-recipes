package org.calrissian.accumulorecipes.web.controller;

import static org.calrissian.mango.collect.CloseableIterables.limit;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import java.util.ArrayList;
import java.util.Collection;
import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.web.AuthsFactory;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.json.MangoModule;
import org.calrissian.mango.types.TypeRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/event")
public class EventStoreController {

    private final AuthsFactory authsFactory;
    private final EventStore eventStore;
    private final ObjectMapper objectMapper;
    private final TypeRegistry<String> typeRegistry;

    @Autowired
    public EventStoreController(AuthsFactory authsFactory, EventStore eventStore, ObjectMapper objectMapper, TypeRegistry<String> typeRegistry) {
        this.authsFactory = authsFactory;
        this.eventStore = eventStore;
        this.objectMapper = objectMapper;
        this.typeRegistry = typeRegistry;

        objectMapper.registerModule(new MangoModule());
    }


    @RequestMapping(value = "/types", method = GET)
    @ResponseBody
    public String getTypes(HttpServletRequest request,
                           @RequestParam String prefix,
                           @RequestParam Integer limit) throws JsonProcessingException {

        CloseableIterable<String> types = limit(eventStore.getTypes(prefix, authsFactory.buildAuths(request)), limit);
        Collection<String> typesCollection = new ArrayList<String>();
        for(String type : types)
            typesCollection.add(type);

        types.closeQuietly();
        return objectMapper.writeValueAsString(typesCollection);
    }

    @RequestMapping(value = "/{eventType}/keys")
    public String getKeysForType(HttpServletRequest request,
                                 @PathVariable String eventType,
                                 @RequestParam String prefix) throws JsonProcessingException {
        CloseableIterable<Pair<String,String>> keys = eventStore.uniqueKeys(prefix, eventType, authsFactory.buildAuths(request));
        Collection<Pair<String,String>> keysCollection = new ArrayList<Pair<String,String>>();
        for(Pair<String,String> keyAndDataTypeAlias : keys)
            keysCollection.add(keyAndDataTypeAlias);

        keys.closeQuietly();

        return objectMapper.writeValueAsString(keysCollection);
    }

    @RequestMapping(value = "/{eventType}/values")
    public String getValuesForTypeAndKey(HttpServletRequest request,
        @PathVariable String eventType,
        @RequestParam String key,
        @RequestParam String dataType,
        @RequestParam String prefix) throws JsonProcessingException {
        CloseableIterable<Object> keys = eventStore.uniqueValuesForKey(prefix, eventType, dataType, key, authsFactory.buildAuths(request));
        Collection<String> valuesCollection = new ArrayList<String>();
        for(Object keyAndDataTypeAlias : keys)
            valuesCollection.add(typeRegistry.encode(keyAndDataTypeAlias));

        keys.closeQuietly();

        return objectMapper.writeValueAsString(valuesCollection);
    }



}
