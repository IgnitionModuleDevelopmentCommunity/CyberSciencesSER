package org.imdc.cybersciences.ser;

import com.inductiveautomation.ignition.gateway.datasource.records.DatasourceRecord;
import com.inductiveautomation.ignition.gateway.localdb.persistence.*;
import com.inductiveautomation.ignition.gateway.web.components.editors.PasswordEditorSource;
import simpleorm.dataset.SFieldFlags;

public class SERDeviceRecord extends PersistentRecord {
    public static final RecordMeta<SERDeviceRecord> META = new RecordMeta<>(
            SERDeviceRecord.class,
            "SERDevices"
    ).setNounKey("SER.Noun")
            .setNounPluralKey("SER.Noun.Plural");

    public static final IdentityField Id = new IdentityField(META, "Id");
    public static final StringField Name = new StringField(META, "Name", SFieldFlags.SMANDATORY, SFieldFlags.SDESCRIPTIVE);
    public static final StringField Hostname = new StringField(META, "Hostname", SFieldFlags.SMANDATORY);
    public static final StringField Username = new StringField(META, "Username", SFieldFlags.SMANDATORY);
    public static final EncodedStringField Password = new EncodedStringField(META, "Password", SFieldFlags.SMANDATORY);
    public static final IntField ChannelPollRate = new IntField(META, "ChannelPollRate", SFieldFlags.SMANDATORY).setDefault(5000);
    public static final IntField EventPollRate = new IntField(META, "EventPollRate", SFieldFlags.SMANDATORY).setDefault(10000);
    public static final BooleanField Enabled = new BooleanField(META, "Enabled").setDefault(true);
    public static final LongField DatasourceId = new LongField(META, "DatasourceId");
    public static final ReferenceField<DatasourceRecord> Datasource =
            new ReferenceField<DatasourceRecord>(META, DatasourceRecord.META, "Datasource", DatasourceId);

    public static final BooleanField AutoCreate = new BooleanField(META, "AutoCreate").setDefault(true);
    public static final BooleanField PruneEnabled = new BooleanField(META, "PruneEnabled").setDefault(false);
    public static final IntField RetentionDays = new IntField(META, "RetentionDays", SFieldFlags.SMANDATORY).setDefault(90);
    public static final StringField TableName = new StringField(META, "TableName",
            SFieldFlags.SMANDATORY).setDefault("SER_EVENTS");
    public static final StringField KeyColumn = new StringField(META, "KeyColumn",
            SFieldFlags.SMANDATORY).setDefault("EVENT_ID");
    public static final StringField SequenceNumberColumn = new StringField(META, "SequenceNumberColumn",
            SFieldFlags.SMANDATORY).setDefault("SEQUENCE_NUMBER");
    public static final StringField TimestampColumn = new StringField(META, "TimestampColumn",
            SFieldFlags.SMANDATORY).setDefault("T_STAMP");
    public static final StringField EventCodeColumn = new StringField(META, "EventCodeColumn",
            SFieldFlags.SMANDATORY).setDefault("EVENT_CODE");
    public static final StringField EventTypeColumn = new StringField(META, "EventTypeColumn",
            SFieldFlags.SMANDATORY).setDefault("EVENT_TYPE");
    public static final StringField ChannelColumn = new StringField(META, "ChannelColumn",
            SFieldFlags.SMANDATORY).setDefault("CHANNEL");
    public static final StringField StatusColumn = new StringField(META, "StatusColumn",
            SFieldFlags.SMANDATORY).setDefault("STATUS");

    public static final StringField CoincidentStatusColumn = new StringField(META, "CoincidentStatusColumn",
            SFieldFlags.SMANDATORY).setDefault("COINCIDENT_STATUS");
    public static final StringField TimeQualityColumn = new StringField(META, "TimeQualityColumn",
            SFieldFlags.SMANDATORY).setDefault("TIME_QUALITY");

    public static final Category ConnectionCategory = new Category("SERDeviceRecord.Category.Connection", 125).include(Name, Hostname, Username, Password, ChannelPollRate, EventPollRate, Enabled);

    public static final Category DatasourceCategory = new Category("SERDeviceRecord.Category.Datasource", 126).include(Datasource, AutoCreate, PruneEnabled, RetentionDays, TableName, KeyColumn, SequenceNumberColumn, TimestampColumn, EventCodeColumn, EventTypeColumn, ChannelColumn, StatusColumn, CoincidentStatusColumn, TimestampColumn, TimeQualityColumn);

    static {
        Password.getFormMeta().setEditorSource(PasswordEditorSource.getSharedInstance());
    }

    @Override
    public RecordMeta<?> getMeta() {
        return META;
    }

    public Long getId() {
        return getLong(Id);
    }

    public Boolean isEnabled() {
        return getBoolean(Enabled);
    }

    public String getName() {
        return getString(Name);
    }

    public String getHostname() {
        return getString(Hostname);
    }

    public String getUsername() {
        return getString(Username);
    }

    public String getPassword() {
        return getString(Password);
    }

    public Integer getChannelPollRate() {
        return getInt(ChannelPollRate);
    }

    public Integer getEventPollRate() {
        return getInt(EventPollRate);
    }

    public Long getDatasourceId() {
        return getLong(DatasourceId);
    }

    public Boolean isAutoCreate() {
        return getBoolean(AutoCreate);
    }

    public Boolean isPruneEnabled() {
        return getBoolean(PruneEnabled);
    }

    public Integer getRetentionDays() {
        return getInt(RetentionDays);
    }

    public String getTableName() {
        return getString(TableName);
    }

    public String getKeyColumn() {
        return getString(KeyColumn);
    }

    public String getSequenceNumberColumn() {
        return getString(SequenceNumberColumn);
    }

    public String getTimestampColumn() {
        return getString(TimestampColumn);
    }

    public String getEventCodeColumn() {
        return getString(EventCodeColumn);
    }

    public String getEventTypeColumn() {
        return getString(EventTypeColumn);
    }

    public String getChannelColumn() {
        return getString(ChannelColumn);
    }

    public String getStatusColumn() {
        return getString(StatusColumn);
    }

    public String getCoincidentStatusColumn() {
        return getString(CoincidentStatusColumn);
    }

    public String getTimeQualityColumn() {
        return getString(TimeQualityColumn);
    }
}
