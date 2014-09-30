package org.fluxtream.admintools;

import com.google.gson.*;

import java.io.*;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Properties;

/**
 * User: candide
 * Date: 30/09/14
 * Time: 15:11
 */
public class DatastoreUtils {

    final String datastoreExecLocation;
    final String datastoreDbLocation;
    final boolean showOutput = false;
    final boolean verboseOutput = false;
    final Properties props;
    Gson gson = new GsonBuilder().registerTypeAdapter(ChannelBounds.class, new ChannelBoundsDeserializer()).create();

    DatastoreUtils() throws IOException {
        String resPath = Main.class.getClassLoader().getResource("other.properties").getPath();
        System.out.println("Looking for other.properties in " + resPath);

        props = new Properties();
        InputStream inputStream = null;

        try {
            inputStream = Main.class.getClassLoader()
                    .getResourceAsStream("other.properties");
        } catch (Throwable e) {
            inputStream = null;
        }

        if (inputStream == null) {
            System.out.println("**** Could not find other.properties.  Please make sure it is in fluxtream-admin-tools/src/main/resources and rebuild");
            System.exit(-1);
        }

        props.load(inputStream);

        final String localPropertiesPath = (String) props.get("local.properties.path");
        System.out.println("Looking for local.properties in " + localPropertiesPath);

        try {
            inputStream = new FileInputStream(localPropertiesPath);
        } catch (Throwable e) {
            inputStream = null;
        }

        if (inputStream == null) {
            System.out.println("**** Could not find local.properties.  Please make sure fluxtream-admin-tools/src/main/resources/other.properties contains a valid local.properties.path value and rebuild");
            System.exit(-1);
        }
        props.load(inputStream);

        datastoreExecLocation = props.getProperty("btdatastore.exec.location");
        datastoreDbLocation = props.getProperty("btdatastore.db.location");
    }

    DataStoreExecutionResult executeDataStore(String commandName, Object[] parameters){
        final StringBuilder responseBuilder = new StringBuilder();
        int result = executeDataStore(commandName,parameters,new OutputStream(){

            @Override
            public void write(final int b) throws IOException {
                responseBuilder.append((char) b);
            }
        });
        return new DataStoreExecutionResult(result,responseBuilder.toString());

    }

    public ChannelInfoResponse listSources(Long guestId) {
        final DataStoreExecutionResult dataStoreExecutionResult = executeDataStore("info",new Object[]{"-r",guestId});
        String result = dataStoreExecutionResult.getResponse();

        // TODO: check statusCode in DataStoreExecutionResult
        ChannelInfoResponse infoResponse = gson.fromJson(result,ChannelInfoResponse.class);

        return infoResponse;
    }

    static class ChannelInfoResponse {
        Map<String,ChannelSpecs> channel_specs;
        double max_time;
        double min_time;
    }

    static class ChannelSpecs{
        String channelType;
        String objectTypeName;
        String time_type;
        ChannelBounds channel_bounds;

        public ChannelSpecs(){
            // time_type defaults to gmt.  It can be overridden to "local" for channels that only know local time
            time_type = "gmt";
        }
    }

    private static class ChannelBounds{
        double max_time;
        double max_value;
        double min_time;
        double min_value;
    }

    class ChannelBoundsDeserializer implements JsonDeserializer<ChannelBounds> {
        // Create a custom deserializer for ChannelBounds to deal with the possibility
        // of the values being interpreted as +/- Infinity and causing json creation errors
        // later on.  The min/max time fields are required.  The min/max value fields are
        // optional and default to 0.
        @Override
        public ChannelBounds deserialize(JsonElement json, Type typeOfT,
                                         JsonDeserializationContext context) throws JsonParseException {
            ChannelBounds cb=new ChannelBounds();

            JsonObject jo = (JsonObject)json;
            cb.max_time=Math.max(Math.min(jo.get("max_time").getAsDouble(), Double.MAX_VALUE),-Double.MAX_VALUE);
            cb.min_time=Math.max(Math.min(jo.get("min_time").getAsDouble(), Double.MAX_VALUE), -Double.MAX_VALUE);

            try {
                cb.max_value=Math.max(Math.min(jo.get("max_value").getAsDouble(), Double.MAX_VALUE),-Double.MAX_VALUE);
                cb.min_value=Math.max(Math.min(jo.get("min_value").getAsDouble(), Double.MAX_VALUE), -Double.MAX_VALUE);
            } catch(Throwable e) {
                cb.min_value=cb.max_value=0;
            }

            return cb;
        }
    }

    private int executeDataStore(String commandName, Object[] parameters,OutputStream out){
        try{
            Runtime rt = Runtime.getRuntime();
            String launchCommand = this.datastoreExecLocation + "/" + commandName + " " +
                    this.datastoreDbLocation;

            //            Path commandPath= Paths.get(env.targetEnvironmentProps.getString("btdatastore.exec.location"));
            //            Path launchExecutable = commandPath.resolve(commandName);
            //            String launchCommand = launchExecutable.toString()+ " " + env.targetEnvironmentProps.getString("btdatastore.db.location");
            for (Object param : parameters){
                launchCommand += ' ';
                String part = param.toString();
                if (part.indexOf(' ') == -1){
                    launchCommand += part;
                }
                else{
                    launchCommand += "\"" + part + "\"";
                }
            }
            if (showOutput)
                System.out.println("BTDataStore: running with command: " + launchCommand);

            //create process for operation
            final Process pr = rt.exec(launchCommand);

            new Thread(){//outputs the errorstream
                public void run(){
                    BufferedReader error = new BufferedReader(new InputStreamReader(pr.getErrorStream()));
                    String line=null;
                    try{
                        if (verboseOutput && showOutput){
                            while((line=error.readLine()) != null) { //output all console output from the execution
                                System.out.println("BTDataStore-error: " + line);
                            }
                        }
                        else
                            while (error.readLine() != null) {}
                    } catch(Exception ignored){}
                }

            }.start();

            BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

            String line;

            boolean first = true;

            while((line=input.readLine()) != null) { //output all console output from the execution
                if (showOutput)
                    System.out.println("BTDataStore: " + line);
                if (first){
                    first = false;
                }
                else{
                    out.write("\n".getBytes());
                }
                out.write(line.getBytes());
            }
            int exitValue = pr.waitFor();
            if (showOutput)
                System.out.println("BTDataStore: exited with code " + exitValue);
            return exitValue;
        }
        catch (Exception e){
            if (showOutput)
                System.out.println("BTDataStore: datastore execution failed!");
            throw new RuntimeException("Datastore execution failed");
        }
    }

    public interface BodyTrackUploadResult {
        /**
         * Status code for the upload operation.
         * @see #isSuccess()
         */
        int getStatusCode();

        /** Text output from the upload, usually (always?) JSON. */
        String getResponse();

        /** Returns <code>true</code> if the upload was successful, <code>false</code> otherwise. */
        boolean isSuccess();
    }

    public static final class DataStoreExecutionResult implements BodyTrackUploadResult {
        private final int statusCode;
        private final String response;

        private DataStoreExecutionResult(final int statusCode, final String response) {
            this.statusCode = statusCode;
            this.response = response;
        }

        @Override
        public int getStatusCode() {
            return statusCode;
        }

        @Override
        public String getResponse() {
            return response;
        }

        @Override
        public boolean isSuccess() {
            return statusCode == 0;
        }
    }

}
