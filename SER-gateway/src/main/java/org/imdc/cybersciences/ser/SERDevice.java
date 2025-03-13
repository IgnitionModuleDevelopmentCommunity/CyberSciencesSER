package org.imdc.cybersciences.ser;

import com.inductiveautomation.ignition.common.datasource.DatasourceStatus;
import com.inductiveautomation.ignition.common.db.schema.ColumnProperty;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import com.inductiveautomation.ignition.common.util.TimeUnits;
import com.inductiveautomation.ignition.gateway.datasource.Datasource;
import com.inductiveautomation.ignition.gateway.datasource.SRConnection;
import com.inductiveautomation.ignition.gateway.datasource.records.DatasourceRecord;
import com.inductiveautomation.ignition.gateway.db.schema.DBTableSchema;
import com.inductiveautomation.ignition.gateway.localdb.persistence.IRecordListener;
import com.inductiveautomation.ignition.gateway.util.DBUtilities;
import com.inductiveautomation.ignition.gateway.web.models.KeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AUTH;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.auth.DigestScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.sql.SQLException;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SERDevice implements IRecordListener<DatasourceRecord> {
    private static final int INIT_RETRY = 60000;
    private final Logger logger;
    private SERDeviceManager deviceManager;
    private ScheduledFuture channelPollFuture, eventPollFuture, pruneFuture;
    private SERDeviceRecord deviceRecord;
    private DeviceStatus deviceStatus;
    private DeviceDatasourceStatus datasourceStatus;
    private String datasource, insertQuery, deleteQuery;
    private String tableName, keyColumn, timestampColumn, sequenceNumberColumn, eventCodeColumn, eventTypeColumn, channelColumn, statusColumn, coincidentStatusColumn, timeQualityColumn;
    private boolean datasourceInitialized = false;
    private long lastDatasourceInitTry = 0;
    private CredentialsProvider credsProvider;
    private CloseableHttpClient httpClient;
    private HttpClientContext context;
    private BasicAuthCache authCache;
    private Map<String, ChannelConfig> channelConfigMap;

    public SERDevice(SERDeviceManager deviceManager, SERDeviceRecord deviceRecord) {
        this.logger = LoggerFactory.getLogger("SER.Device." + deviceRecord.getName());
        this.deviceManager = deviceManager;
        this.deviceRecord = deviceRecord;
        this.channelConfigMap = new HashMap<>();
    }

    private void initDatasource() {
        setDatasourceStatus(DeviceDatasourceStatus.UNKNOWN);

        this.tableName = deviceRecord.getTableName();
        this.keyColumn = deviceRecord.getKeyColumn();
        this.timestampColumn = deviceRecord.getTimestampColumn();
        this.sequenceNumberColumn = deviceRecord.getSequenceNumberColumn();
        this.eventCodeColumn = deviceRecord.getEventCodeColumn();
        this.eventTypeColumn = deviceRecord.getEventTypeColumn();
        this.channelColumn = deviceRecord.getChannelColumn();
        this.statusColumn = deviceRecord.getStatusColumn();
        this.coincidentStatusColumn = deviceRecord.getCoincidentStatusColumn();
        this.timeQualityColumn = deviceRecord.getTimeQualityColumn();

        DatasourceRecord dsr = deviceManager.getGatewayContext().getPersistenceInterface().find(DatasourceRecord.META, deviceRecord.getDatasourceId());
        this.datasource = dsr == null ? null : dsr.getName();

        try {
            if (System.currentTimeMillis() - lastDatasourceInitTry >= INIT_RETRY) {
                lastDatasourceInitTry = System.currentTimeMillis();
                Datasource ds = deviceManager.getGatewayContext().getDatasourceManager().getDatasource(datasource);
                if (ds == null || ds.getStatus() == DatasourceStatus.DISABLED) {
                    setDatasourceStatus(ds == null ? DeviceDatasourceStatus.NOTCONFIGURED : DeviceDatasourceStatus.DISABLED);
                    logger.error("Datasource '{}' {}. Device will be unavailable until targeted to valid database.", datasource, ds == null ? "doesn't exist" : "is disabled");
                    return;
                }

                if (deviceRecord.isAutoCreate() && !checkTable()) {
                    setDatasourceStatus(DeviceDatasourceStatus.NOTVERIFIED);
                    return;
                }

                String q = ds.getTranslator().getColumnQuoteChar();

                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(tableName);
                sb.append(" (");
                sb.append(q).append(sequenceNumberColumn).append(q).append(", ");
                sb.append(q).append(timestampColumn).append(q).append(", ");
                sb.append(q).append(eventCodeColumn).append(q).append(", ");
                sb.append(q).append(eventTypeColumn).append(q).append(", ");
                sb.append(q).append(channelColumn).append(q).append(", ");
                sb.append(q).append(statusColumn).append(q).append(", ");
                sb.append(q).append(coincidentStatusColumn).append(q).append(", ");
                sb.append(q).append(timeQualityColumn).append(q);
                sb.append(") VALUES (?,?,?,?,?,?,?,?)");
                insertQuery = sb.toString();

                sb.setLength(0);

                sb.append("DELETE FROM ").append(tableName).append(" WHERE ");
                sb.append(q).append(timestampColumn).append(q).append(" < ?");
                deleteQuery = sb.toString();

                if (deviceRecord.isPruneEnabled()) {
                    if (pruneFuture != null) {
                        pruneFuture.cancel(true);
                    }

                    pruneFuture = deviceManager.getGatewayContext().getExecutionManager().scheduleWithFixedDelay(this::prune, 30, 30, TimeUnit.MINUTES);
                }

                datasourceInitialized = true;
                setDatasourceStatus(DeviceDatasourceStatus.VALID);
            }
        } catch (Throwable t) {
            setDatasourceStatus(DeviceDatasourceStatus.FAULTED);
            logger.error("Error initializing database", t);
        }
    }

    private String getTagPrefix() {
        return String.format("%s", deviceRecord.getName());
    }

    public void updateTag(String tag, Object value) {
        deviceManager.getTagManager().tagUpdate(String.format("%s/%s", getTagPrefix(), tag), value);
    }

    private void registerUDTs() throws Exception {
        TagBuilder builder = TagBuilder.createUDTInstance("Status", String.format("%s/Status", getTagPrefix()));
        deviceManager.getTagManager().registerUDT(builder.build());

        builder = TagBuilder.createUDTInstance("Event", String.format("%s/LastEvent", getTagPrefix()));
        deviceManager.getTagManager().registerUDT(builder.build());

        builder = TagBuilder.createUDTInstance("EventStatus", String.format("%s/EventStatus", getTagPrefix()));
        deviceManager.getTagManager().registerUDT(builder.build());

        builder = TagBuilder.createUDTInstance("Diagnostics", String.format("%s/Diagnostics", getTagPrefix()));
        deviceManager.getTagManager().registerUDT(builder.build());
    }

    public void startup() {
        logger.debug("Starting up");

        try {
            registerUDTs();
        } catch (Throwable t) {
            logger.error("Error registering UDTs", t);
            setDeviceStatus(DeviceStatus.FAULTED);
            return;
        }

        setDeviceStatus(DeviceStatus.UNKNOWN);

        if (!deviceRecord.isEnabled()) {
            setDeviceStatus(DeviceStatus.DISABLED);
            setDatasourceStatus(DeviceDatasourceStatus.UNKNOWN);
            return;
        }

        try {
            credsProvider = new BasicCredentialsProvider();
            credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(deviceRecord.getUsername(), deviceRecord.getPassword()));

            httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).setSSLSocketFactory(new SSLConnectionSocketFactory(SSLContexts.custom().loadTrustMaterial(null, new TrustAllStrategy()).build(), NoopHostnameVerifier.INSTANCE)).build();
            context = HttpClientContext.create();
            authCache = new BasicAuthCache();
            context.setAuthCache(authCache);
        } catch (Throwable ex) {
            logger.error("Error creating HTTP client", ex);
            setDeviceStatus(DeviceStatus.FAULTED);
            return;
        }

        setDeviceStatus(DeviceStatus.STARTING);
        DatasourceRecord.META.addRecordListener(this);
        initDatasource();

        channelPollFuture = deviceManager.getGatewayContext().getExecutionManager().scheduleWithFixedDelay(this::channelRun, deviceRecord.getChannelPollRate(), deviceRecord.getChannelPollRate(), TimeUnit.MILLISECONDS);
        eventPollFuture = deviceManager.getGatewayContext().getExecutionManager().scheduleWithFixedDelay(this::eventRun, deviceRecord.getEventPollRate(), deviceRecord.getEventPollRate(), TimeUnit.MILLISECONDS);
        setDeviceStatus(DeviceStatus.RUNNING);
    }

    public void shutdown() {
        logger.debug("Shutting down");

        DatasourceRecord.META.removeRecordListener(this);

        try {
            if (httpClient != null) {
                httpClient.close();
            }
        } catch (Throwable ex) {
            logger.error("Error shutting down HTTP client", ex);
        }

        try {
            if (pruneFuture != null) {
                pruneFuture.cancel(true);
            }
        } catch (Throwable ex) {
            logger.error("Error shutting down pruning execution", ex);
        }

        try {
            if (channelPollFuture != null) {
                channelPollFuture.cancel(true);
            }
        } catch (Throwable ex) {
            logger.error("Error shutting down channel polling execution", ex);
        }

        try {
            if (eventPollFuture != null) {
                eventPollFuture.cancel(true);
            }
        } catch (Throwable ex) {
            logger.error("Error shutting down event polling execution", ex);
        }
    }

    private boolean checkTable() {
        Datasource ds = deviceManager.getGatewayContext().getDatasourceManager().getDatasource(datasource);
        if (ds == null || ds.getStatus() == DatasourceStatus.DISABLED) {
            logger.error("Datasource '{}' {}. Device will be unavailable until targeted to valid database.", datasource, ds == null ? "doesn't exist" : "is disabled");
            return false;
        }

        DBTableSchema schema = new DBTableSchema(tableName, ds.getTranslator());
        schema.addRequiredColumn(keyColumn, DataType.Int4, EnumSet.of(ColumnProperty.PrimaryKey, ColumnProperty.AutoIncrement));
        schema.addRequiredColumn(timestampColumn, DataType.Int8, EnumSet.of(ColumnProperty.Indexed));
        schema.addRequiredColumn(sequenceNumberColumn, DataType.Int4, EnumSet.of(ColumnProperty.Indexed));
        schema.addRequiredColumn(eventCodeColumn, DataType.Int4, EnumSet.of(ColumnProperty.Indexed));
        schema.addRequiredColumn(eventTypeColumn, DataType.String, null);
        schema.addRequiredColumn(channelColumn, DataType.Int4, EnumSet.of(ColumnProperty.Indexed));
        schema.addRequiredColumn(statusColumn, DataType.String, null);
        schema.addRequiredColumn(coincidentStatusColumn, DataType.Int8, null);
        schema.addRequiredColumn(timeQualityColumn, DataType.String, null);

        SRConnection conn = null;

        try {
            conn = ds.getConnection();
            schema.verifyAndUpdate(conn);
            //Update columns with proper casing.
            keyColumn = schema.getCasedColumnName(keyColumn);
            timestampColumn = schema.getCasedColumnName(timestampColumn);
            sequenceNumberColumn = schema.getCasedColumnName(sequenceNumberColumn);
            eventCodeColumn = schema.getCasedColumnName(eventCodeColumn);
            eventTypeColumn = schema.getCasedColumnName(eventTypeColumn);
            channelColumn = schema.getCasedColumnName(channelColumn);
            statusColumn = schema.getCasedColumnName(statusColumn);
            coincidentStatusColumn = schema.getCasedColumnName(coincidentStatusColumn);
            timeQualityColumn = schema.getCasedColumnName(timeQualityColumn);
            return true;
        } catch (Exception e) {
            logger.error("Error verifying SER events table.", e);
            return false;
        } finally {
            DBUtilities.close(conn, null);
        }
    }

    private void prune() {
        try {
            try (SRConnection con = deviceManager.getGatewayContext().getDatasourceManager().getConnection(datasource)) {
                long now = System.currentTimeMillis();

                Date since = new Date(now - TimeUnits.toMillis((double) deviceRecord.getRetentionDays(), TimeUnits.DAY));
                int affected = con.runPrepUpdate(deleteQuery, since);
                if (affected > 0 && logger.isDebugEnabled()) {
                    logger.debug("Deleted {} old SER events (from {} to now)", affected, since);
                }
            }
        } catch (SQLException e) {
            logger.error("Error pruning SER events table \"{}\" due to underlying exception.", deviceRecord.getTableName(), e);
        }
    }

    @Override
    public void recordUpdated(DatasourceRecord datasourceRecord) {
        if (datasourceRecord.getId() == deviceRecord.getDatasourceId()) {
            datasourceInitialized = false;
            lastDatasourceInitTry = 0;
            initDatasource();
        }
    }

    @Override
    public void recordAdded(DatasourceRecord datasourceRecord) {

    }

    @Override
    public void recordDeleted(KeyValue keyValue) {
        long id = (long) keyValue.getKeyValues()[0];
        if (id == deviceRecord.getDatasourceId()) {
            try {
                if (pruneFuture != null) {
                    pruneFuture.cancel(true);
                }
            } catch (Throwable ex) {
                logger.error("Error shutting down pruning execution", ex);
            }

            setDatasourceStatus(DeviceDatasourceStatus.NOTCONFIGURED);
            datasource = null;
            datasourceInitialized = false;
            lastDatasourceInitTry = 0;
        }
    }

    public synchronized DeviceStatus getDeviceStatus() {
        return deviceStatus;
    }

    public synchronized void setDeviceStatus(DeviceStatus deviceStatus) {
        if (!deviceStatus.equals(this.deviceStatus)) {
            this.deviceStatus = deviceStatus;

            try {
                updateTag("Status/Status", deviceStatus.getDisplay());
            } catch (Throwable ignored) {
            }
        }
    }

    public synchronized String getStatusDisplay() {
        return getDeviceStatus() == null ? DeviceStatus.UNKNOWN.getDisplay() : getDeviceStatus().getDisplay();
    }

    public synchronized DeviceDatasourceStatus getDatasourceStatus() {
        return datasourceStatus;
    }

    public synchronized void setDatasourceStatus(DeviceDatasourceStatus datasourceStatus) {
        if (!datasourceStatus.equals(this.datasourceStatus)) {
            this.datasourceStatus = datasourceStatus;

            try {
                updateTag("Status/DatabaseStatus", datasourceStatus.getDisplay());
            } catch (Throwable ignored) {
            }
        }
    }

    public synchronized String getDatasourceStatusDisplay() {
        return getDatasourceStatus() == null ? DeviceDatasourceStatus.UNKNOWN.getDisplay() : getDatasourceStatus().getDisplay();
    }

    private synchronized Boolean isDatasourceNotInitialized() {
        return datasource != null && !datasourceInitialized;
    }

    private synchronized Boolean isDatasourceInitialized() {
        return datasourceInitialized;
    }

    public void channelRun() {
        try {
            updateTag("Status/Channel/LastExecution", new Date());
            long functionStartTime = System.currentTimeMillis();

            getDiag();
            getChannelInfo();
            getChannelData();

            long functionEndTime = System.currentTimeMillis();
            long functionTotalTime = functionEndTime - functionStartTime;
            updateTag("Status/Channel/LastExecutionDuration", functionTotalTime);
            updateTag("Status/Channel/NextExecution", new Date(new Date().getTime() + deviceRecord.getChannelPollRate()));
            setDeviceStatus(DeviceStatus.RUNNING);
        } catch (Throwable ex) {
            setDeviceStatus(DeviceStatus.FAULTED);
            logger.error("Error polling device for channel status", ex);
        }
    }

    public void eventRun() {
        try {
            updateTag("Status/Event/LastExecution", new Date());
            long functionStartTime = System.currentTimeMillis();

            if (isDatasourceNotInitialized()) {
                initDatasource();
            }

            if (isDatasourceInitialized()) {
                getEvents();
            }

            long functionEndTime = System.currentTimeMillis();
            long functionTotalTime = functionEndTime - functionStartTime;
            updateTag("Status/Event/LastExecutionDuration", functionTotalTime);
            updateTag("Status/Event/NextExecution", new Date(new Date().getTime() + deviceRecord.getEventPollRate()));
            setDeviceStatus(DeviceStatus.RUNNING);
        } catch (Throwable ex) {
            setDeviceStatus(DeviceStatus.FAULTED);
            logger.error("Error polling device for events", ex);
        }
    }

    private void getDiag() throws Exception {
        String response = httpGet("/diag");
        JSONObject jsonObj = new JSONObject(response);

        updateTag("Diagnostics/Mac1", jsonObj.getString("mac1"));
        updateTag("Diagnostics/Mac2", jsonObj.getString("mac2"));
        updateTag("Diagnostics/Eport", jsonObj.getString("eport"));
        updateTag("Diagnostics/Model", jsonObj.getString("model"));
        updateTag("Diagnostics/DeviceName", jsonObj.getString("device_name"));
        updateTag("Diagnostics/DeviceId", jsonObj.getString("device_ID"));
        updateTag("Diagnostics/CatalogNumber", jsonObj.getString("catalog_number"));
        updateTag("Diagnostics/DOM", jsonObj.getString("dom"));
        updateTag("Diagnostics/SerialNumber", jsonObj.getString("serial_number"));
        updateTag("Diagnostics/HardwareVersion", jsonObj.getString("hardware_version"));
        updateTag("Diagnostics/FirmwareVersion", jsonObj.getString("firmware_version"));
        updateTag("Diagnostics/Build", jsonObj.getInt("build"));
        updateTag("Diagnostics/CFM0Version", jsonObj.getString("cfm0_version"));
        updateTag("Diagnostics/CFM1Version", jsonObj.getString("cfm1_version"));
        updateTag("Diagnostics/UFMVersion", jsonObj.getString("ufm_version"));
        updateTag("Diagnostics/PCMVersion", jsonObj.getString("pcm_version"));
        updateTag("Diagnostics/StorageTotal", jsonObj.getLong("storage_total"));
        updateTag("Diagnostics/StorageFree", jsonObj.getLong("storage_free"));
        updateTag("Diagnostics/StorageScale", jsonObj.getLong("storage_scale"));
        updateTag("Diagnostics/SecondsUTC", jsonObj.getLong("secondsUTC"));
        updateTag("Diagnostics/DSTActive", jsonObj.getInt("dst_active") != 0);
        updateTag("Diagnostics/TimeZoneOffset", jsonObj.getInt("time_zone_offset"));
        updateTag("Diagnostics/AltDateFormat", jsonObj.getInt("alt_date_format"));
        updateTag("Diagnostics/AltTimeFormat", jsonObj.getInt("alt_time_format"));
        updateTag("Diagnostics/TimeSourceSetup", jsonObj.getInt("time_source_setup"));
        updateTag("Diagnostics/Slot1", jsonObj.getInt("slot1"));
        updateTag("Diagnostics/Slot2", jsonObj.getInt("slot2"));
    }

    private void getChannelInfo() throws Exception {
        String response = httpGet("/channels/name/ext");
        JSONObject jsonObj = new JSONObject(response);
        JSONArray channels = jsonObj.getJSONArray("channels_name_ext");
        for (int i = 0; i < channels.length(); i++) {
            JSONObject channelObj = channels.getJSONObject(i);
            ChannelConfig channelConfig = ChannelConfig.fromChannelObj(channelObj);

            String channelTagPath = "Channels/Channel" + channelConfig.getChannel();

            channelConfigMap.put(channelConfig.getChannel(), channelConfig);

            if (!deviceManager.getTagManager().tagExists(channelTagPath)) {
                TagBuilder builder = TagBuilder.createUDTInstance("Channel", String.format("%s/%s", getTagPrefix(), channelTagPath));
                deviceManager.getTagManager().registerUDT(builder.build());
            }

            updateTag(String.format("%s/Channel", channelTagPath), channelConfig.getChannel());
            updateTag(String.format("%s/Name", channelTagPath), channelConfig.getName());
        }
    }

    private void getChannelData() throws Exception {
        String response = httpGet("/channels/status");
        JSONObject jsonObj = new JSONObject(response);
        Long status = jsonObj.getLong("status");

        response = httpGet("/channels/data");
        jsonObj = new JSONObject(response);
        JSONArray channels = jsonObj.getJSONArray("channels_data");
        for (int i = 0; i < channels.length(); i++) {
            Integer channelNum = i + 1;
            String channel = StringUtils.leftPad(channelNum.toString(), 2, "0");
            JSONObject channelObj = channels.getJSONObject(i);

            ChannelConfig channelConfig = channelConfigMap.getOrDefault(channel, new ChannelConfig(channel, "", "Off", "On"));

            String channelTagPath = "Channels/Channel" + channelConfig.getChannel();
            Integer value = channelObj.getInt("value");
            Boolean channelStatus = ((status >> i) & 0x1) == 0x1;
            String channelStatusStr = channelStatus ? channelConfig.getOnText() : channelConfig.getOffText();

            updateTag(String.format("%s/SecondsUTC", channelTagPath), channelObj.getLong("secondsUTC"));
            updateTag(String.format("%s/DSTActive", channelTagPath), channelObj.getInt("dst_active") != 0);
            updateTag(String.format("%s/Value", channelTagPath), channelStatus);
            updateTag(String.format("%s/Counter", channelTagPath), value);
            updateTag(String.format("%s/Status", channelTagPath), channelStatusStr);
        }
    }

    private EventStatus getEventStatus() throws Exception {
        String response = httpGet("/events/last");
        JSONObject jsonObj = new JSONObject(response);
        return EventStatus.fromEventObj(jsonObj);
    }

    private void getEvents() throws Exception {
        EventStatus eventStatus = getEventStatus();

        Integer diffSequenceNumber = 0;
        Integer lastSequenceNumber = (Integer) deviceManager.getTagManager().readTag(String.format("%s/EventStatus/LastSequenceNumber", getTagPrefix())).getValue();
        if (lastSequenceNumber == null) {
            diffSequenceNumber = eventStatus.getLastSequenceNumber();
        } else {
            diffSequenceNumber = eventStatus.getLastSequenceNumber() - lastSequenceNumber;
        }

        if (diffSequenceNumber > 8192) {
            diffSequenceNumber = 8192;
        }

        if (diffSequenceNumber > 0) {
            Integer numIterations = (int) Math.ceil(diffSequenceNumber / 100.0);
            Integer leftOver = diffSequenceNumber - ((numIterations - 1) * 100);
            Integer nextRecord = eventStatus.getLastRecord() - (diffSequenceNumber - 1);
            if (nextRecord < 0) {
                nextRecord = 8192 + nextRecord;
            }

            EventParser.Event event = null;
            for (int i = 0; i < numIterations; i++) {
                Integer count = ((i + 1) < numIterations) ? 100 : leftOver;

                String response = httpGet(String.format("/events?record=%d&count=%d", nextRecord, count));
                JSONObject jsonObj = new JSONObject(response);
                JSONArray events = jsonObj.getJSONArray("events");
                for (int j = 0; j < events.length(); j++) {
                    JSONObject eventObj = events.getJSONObject(j);
                    event = EventParser.parse(logger.getName(), insertQuery, eventObj.getString("r"));
                    deviceManager.getGatewayContext().getHistoryManager().storeHistory(datasource, event);
                }

                nextRecord += count;
                if (nextRecord >= 8192) {
                    nextRecord -= 8192;
                }
            }

            if (event != null) {
                event.writeTags(deviceManager.getTagManager(), getTagPrefix());
            }
        }

        updateTag("EventStatus/NumberOfEvents", eventStatus.getNumberOfEvents());
        updateTag("EventStatus/FirstRecord", eventStatus.getFirstRecord());
        updateTag("EventStatus/LastRecord", eventStatus.getLastRecord());
        updateTag("EventStatus/LastSequenceNumber", eventStatus.getLastSequenceNumber());
    }

    private String httpGet(String uri) throws Exception {
        String ret = null;

        URL url = new URL("https://" + deviceRecord.getHostname() + uri);
        HttpHost targetHost = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        HttpGet httpGet = new HttpGet(url.toURI().toString());
        CloseableHttpResponse response = httpClient.execute(targetHost, httpGet, context);
        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
            Header authHeader = response.getFirstHeader(AUTH.WWW_AUTH);
            DigestScheme digestScheme = new DigestScheme();
            digestScheme.processChallenge(authHeader);
            authCache.put(targetHost, digestScheme);
            httpGet.addHeader(digestScheme.authenticate(credsProvider.getCredentials(AuthScope.ANY), httpGet, context));
            response.close();
            response = httpClient.execute(targetHost, httpGet, context);
        }

        ret = EntityUtils.toString(response.getEntity());
        response.close();

        return ret;
    }

    public enum DeviceStatus {
        UNKNOWN("Unknown"), DISABLED("Disabled"), FAULTED("Faulted"), STARTING("Starting"), RUNNING("Running"), DISCONNECTED("Disconnected");

        private String display;

        DeviceStatus(String display) {
            this.display = display;
        }

        public String getDisplay() {
            return display;
        }

        @Override
        public String toString() {
            return getDisplay();
        }
    }

    public enum DeviceDatasourceStatus {
        UNKNOWN("Unknown"), NOTCONFIGURED("Not Configured"), NOTVERIFIED("Tables Not Verified"), VALID("Valid"), FAULTED("Faulted"), DISABLED("Disabled");

        private String display;

        DeviceDatasourceStatus(String display) {
            this.display = display;
        }

        public String getDisplay() {
            return display;
        }

        @Override
        public String toString() {
            return getDisplay();
        }
    }

    public static class ChannelConfig {
        private String channel, name, offText, onText;

        public ChannelConfig(String channel, String name, String offText, String onText) {
            this.channel = channel;
            this.name = name;
            this.offText = offText;
            this.onText = onText;
        }

        public static ChannelConfig fromChannelObj(JSONObject channelObj) throws JSONException {
            return new ChannelConfig(channelObj.getString("channel"), channelObj.getString("name"), channelObj.getString("offText"), channelObj.getString("onText"));
        }

        public String getChannel() {
            return channel;
        }

        public String getName() {
            return name;
        }

        public String getOffText() {
            return offText;
        }

        public String getOnText() {
            return onText;
        }
    }

    public static class EventStatus {
        private Integer numberOfEvents, firstRecord, lastRecord, lastSequenceNumber;

        public EventStatus(Integer numberOfEvents, Integer firstRecord, Integer lastRecord, Integer lastSequenceNumber) {
            this.numberOfEvents = numberOfEvents;
            this.firstRecord = firstRecord;
            this.lastRecord = lastRecord;
            this.lastSequenceNumber = lastSequenceNumber;
        }

        public static EventStatus fromEventObj(JSONObject channelObj) throws JSONException {
            return new EventStatus(channelObj.getInt("NumberOfEvents"), channelObj.getInt("FirstRecord"), channelObj.getInt("LastRecord"), channelObj.getInt("LastSequenceNumber"));
        }

        public Integer getNumberOfEvents() {
            return numberOfEvents;
        }

        public Integer getFirstRecord() {
            return firstRecord;
        }

        public Integer getLastRecord() {
            return lastRecord;
        }

        public Integer getLastSequenceNumber() {
            return lastSequenceNumber;
        }
    }
}
