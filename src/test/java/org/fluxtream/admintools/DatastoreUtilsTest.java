package org.fluxtream.admintools;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.InputStream;

public class DatastoreUtilsTest {


    Gson gson = new GsonBuilder().registerTypeAdapter(DatastoreUtils.ChannelBounds.class, new DatastoreUtils.ChannelBoundsDeserializer()).create();

    @Test
    public void testParseJSON() throws Exception {
        final InputStream jsonStream = getClass().getClassLoader().getResourceAsStream("channelSpecs.json");
        final String result = IOUtils.toString(jsonStream);
        DatastoreUtils.ChannelInfoResponse infoResponse = gson.fromJson(result, DatastoreUtils.ChannelInfoResponse.class);
        System.out.println(infoResponse);
    }
}