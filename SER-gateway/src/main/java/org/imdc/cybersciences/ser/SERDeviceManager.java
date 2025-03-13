package org.imdc.cybersciences.ser;

import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import com.inductiveautomation.ignition.gateway.localdb.persistence.IRecordListener;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.web.models.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simpleorm.dataset.SQuery;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SERDeviceManager implements IRecordListener<SERDeviceRecord> {
    private static final Logger logger = LoggerFactory.getLogger("SER.Device.Manager");

    private static SERDeviceManager _INSTANCE = null;
    private GatewayContext gatewayContext;
    private TagManager tagManager;

    private Map<Long, String> deviceIdMap;
    private Map<String, SERDevice> deviceConfigurations;

    public SERDeviceManager() {
        deviceIdMap = new ConcurrentHashMap<>();
        deviceConfigurations = new ConcurrentHashMap<>();
        tagManager = new TagManager();
    }

    public static SERDeviceManager get() {
        if (_INSTANCE == null) {
            _INSTANCE = new SERDeviceManager();
        }
        return _INSTANCE;
    }

    public void setGatewayContext(GatewayContext gatewayContext) {
        this.gatewayContext = gatewayContext;
        this.tagManager.init(gatewayContext);
    }

    public void startup() {
        logger.debug("Starting up");
        SERDeviceRecord.META.addRecordListener(this);
        tagManager.startup();
        init();
    }

    public void shutdown() {
        logger.debug("Shutting down");
        SERDeviceRecord.META.removeRecordListener(this);
        tagManager.shutdown();

        for (SERDevice device : deviceConfigurations.values()) {
            try {
                device.shutdown();
            } catch (Throwable t) {
                logger.error("Error initializing manager", t);
            }
        }
    }

    private void init() {
        try {
            registerUDTs();

            SQuery<SERDeviceRecord> query = new SQuery<>(SERDeviceRecord.META);
            List<SERDeviceRecord> devices = gatewayContext.getPersistenceInterface().query(query);
            for (SERDeviceRecord deviceRecord : devices) {
                deviceAddAndStartup(deviceRecord);
            }
        } catch (Throwable t) {
            logger.error("Error initializing manager", t);
        }
    }

    private void registerUDTs() throws Exception {
        TagBuilder builder = TagBuilder.createUDTDefinition("Status");
        builder.addMember("Channel/LastExecution", DataType.DateTime);
        builder.addMember("Channel/NextExecution", DataType.DateTime);
        builder.addMember("Channel/LastExecutionDuration", DataType.Int8);
        builder.addMember("Event/LastExecution", DataType.DateTime);
        builder.addMember("Event/NextExecution", DataType.DateTime);
        builder.addMember("Event/LastExecutionDuration", DataType.Int8);
        builder.addMember("Status", DataType.String);
        builder.addMember("DatabaseStatus", DataType.String);
        getTagManager().registerUDT(builder.build());

        builder = TagBuilder.createUDTDefinition("Diagnostics");
        builder.addMember("Mac1", DataType.String);
        builder.addMember("Mac2", DataType.String);
        builder.addMember("Eport", DataType.String);
        builder.addMember("Model", DataType.String);
        builder.addMember("DeviceName", DataType.String);
        builder.addMember("DeviceId", DataType.String);
        builder.addMember("CatalogNumber", DataType.String);
        builder.addMember("DOM", DataType.String);
        builder.addMember("SerialNumber", DataType.String);
        builder.addMember("HardwareVersion", DataType.String);
        builder.addMember("FirmwareVersion", DataType.String);
        builder.addMember("Build", DataType.Int4);
        builder.addMember("CFM0Version", DataType.String);
        builder.addMember("CFM1Version", DataType.String);
        builder.addMember("UFMVersion", DataType.String);
        builder.addMember("PCMVersion", DataType.String);
        builder.addMember("StorageTotal", DataType.Int8);
        builder.addMember("StorageFree", DataType.Int8);
        builder.addMember("StorageScale", DataType.Int8);
        builder.addMember("SecondsUTC", DataType.Int8);
        builder.addMember("DSTActive", DataType.Boolean);
        builder.addMember("TimeZoneOffset", DataType.Int4);
        builder.addMember("AltDateFormat", DataType.Int4);
        builder.addMember("AltTimeFormat", DataType.Int4);
        builder.addMember("TimeSourceSetup", DataType.Int4);
        builder.addMember("Slot1", DataType.Int4);
        builder.addMember("Slot2", DataType.Int4);
        getTagManager().registerUDT(builder.build());

        builder = TagBuilder.createUDTDefinition("Channel");
        builder.addMember("Channel", DataType.String);
        builder.addMember("Name", DataType.String);
        builder.addMember("SecondsUTC", DataType.Int8);
        builder.addMember("DSTActive", DataType.Boolean);
        builder.addMember("Value", DataType.Boolean);
        builder.addMember("Counter", DataType.Int8);
        builder.addMember("Status", DataType.String);
        getTagManager().registerUDT(builder.build());

        builder = TagBuilder.createUDTDefinition("Event");
        builder.addMember("SequenceNumber", DataType.Int4);
        builder.addMember("EventCode", DataType.String);
        builder.addMember("Channel", DataType.Int4);
        builder.addMember("Status", DataType.Int4);
        builder.addMember("CoincidentStatus", DataType.Int4);
        builder.addMember("Timestamp", DataType.Int8);
        builder.addMember("DST", DataType.String);
        builder.addMember("TimeQuality", DataType.String);
        getTagManager().registerUDT(builder.build());

        builder = TagBuilder.createUDTDefinition("EventStatus");
        builder.addMember("NumberOfEvents", DataType.Int4);
        builder.addMember("FirstRecord", DataType.Int4);
        builder.addMember("LastRecord", DataType.Int4);
        builder.addMember("LastSequenceNumber", DataType.Int4);
        getTagManager().registerUDT(builder.build());
    }

    private void deviceAddAndStartup(SERDeviceRecord deviceRecord) {
        try {
            logger.debug("Starting up device '" + deviceRecord.getName() + "'");
            SERDevice device = new SERDevice(this, deviceRecord);
            device.startup();
            deviceIdMap.put(deviceRecord.getId(), deviceRecord.getName());
            deviceConfigurations.put(deviceRecord.getName(), device);
        } catch (Throwable ex) {
            logger.error("Error starting up " + deviceRecord.getName(), ex);
        }
    }

    @Override
    public void recordUpdated(SERDeviceRecord deviceRecord) {
        logger.debug("Device " + deviceRecord.getName() + "' record updated");

        long id = deviceRecord.getId();
        String oldName = null;

        if (deviceIdMap.containsKey(id)) {
            oldName = deviceIdMap.get(id);
            deviceIdMap.remove(id);
        }

        if (oldName != null && deviceConfigurations.containsKey(oldName)) {
            try {
                SERDevice device = deviceConfigurations.get(oldName);
                device.shutdown();
                deviceConfigurations.remove(oldName);
            } catch (Throwable ex) {
                logger.error("Error shutting down old instance", ex);
            }
        }

        deviceAddAndStartup(deviceRecord);
    }

    @Override
    public void recordAdded(SERDeviceRecord deviceRecord) {
        logger.debug("Device " + deviceRecord.getName() + "' record added");
        deviceAddAndStartup(deviceRecord);
    }

    @Override
    public void recordDeleted(KeyValue keyValue) {
        long id = (long) keyValue.getKeyValues()[0];
        logger.debug("Collector " + id + "' record deleted");

        if (deviceIdMap.containsKey(id)) {
            try {
                SERDevice device = deviceConfigurations.get(deviceIdMap.get(id));
                device.shutdown();
            } catch (Throwable ex) {
                logger.error("Error shutting down old instance", ex);
            }
        }
    }

    public GatewayContext getGatewayContext() {
        return gatewayContext;
    }

    public TagManager getTagManager() {
        return tagManager;
    }

    public SERDevice getDeviceById(long id) throws Exception {
        if (deviceIdMap.containsKey(id)) {
            return deviceConfigurations.get(deviceIdMap.get(id));
        }

        throw new Exception("Device with id '" + id + "' doesn't exist");
    }

    public SERDevice getDeviceByName(String name) throws Exception {
        if (deviceConfigurations.containsKey(name)) {
            return deviceConfigurations.get(name);
        }

        throw new Exception("Device with name '" + name + "' doesn't exist");
    }
}
