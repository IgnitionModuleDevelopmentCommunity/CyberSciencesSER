package org.imdc.cybersciences.ser;

import com.inductiveautomation.ignition.gateway.localdb.persistence.RecordMeta;
import com.inductiveautomation.ignition.gateway.web.components.RecordActionTable;
import com.inductiveautomation.ignition.gateway.web.models.ConfigCategory;
import com.inductiveautomation.ignition.gateway.web.models.DefaultConfigTab;
import com.inductiveautomation.ignition.gateway.web.models.IConfigTab;
import com.inductiveautomation.ignition.gateway.web.pages.IConfigPage;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SERDevicesManagerPage extends RecordActionTable<SERDeviceRecord> {
    private final Logger logger = LoggerFactory.getLogger("SER.Devices.Page");

    transient List<ICalculatedField<SERDeviceRecord>> calcFields;

    public static ConfigCategory CONFIG_CATEGORY = new ConfigCategory("SER", "SER.MenuTitle");
    public static IConfigTab MENU_ENTRY = DefaultConfigTab.builder()
            .category(CONFIG_CATEGORY)
            .name("serdevices")
            .i18n("SER.Devices.MenuTitle")
            .page(SERDevicesManagerPage.class)
            .terms("CyberSciences", "SER", "32e")
            .build();

    public SERDevicesManagerPage(IConfigPage configPage) {
        super(configPage);
    }

    @Override
    protected RecordMeta<SERDeviceRecord> getRecordMeta() {
        return SERDeviceRecord.META;
    }

    @Override
    public Pair<String, String> getMenuLocation() {
        return MENU_ENTRY.getMenuLocation();
    }

    @Override
    protected String getTitleKey() {
        return "SER.PageTitle";
    }

    @Override
    protected List<ICalculatedField<SERDeviceRecord>> getCalculatedFields() {
        if (calcFields == null) {
            calcFields = new ArrayList<>(1);
            calcFields.add(new ICalculatedField<>() {
                @Override
                public String getFieldvalue(SERDeviceRecord record) {
                    try {
                        SERDevice device = SERDeviceManager.get().getDeviceById(record.getId());
                        return device.getStatusDisplay();
                    } catch (Throwable t) {
                        logger.warn("Error getting status: " + t.getMessage(), t);
                        return "Error";
                    }
                }

                @Override
                public String getHeaderKey() {
                    return "SER.Status.Name";
                }
            });
            calcFields.add(new ICalculatedField<>() {
                @Override
                public String getFieldvalue(SERDeviceRecord record) {
                    try {
                        SERDevice device = SERDeviceManager.get().getDeviceById(record.getId());
                        return device.getDatasourceStatusDisplay();
                    } catch (Throwable t) {
                        logger.warn("Error getting datasource status: " + t.getMessage(), t);
                        return "Error";
                    }
                }

                @Override
                public String getHeaderKey() {
                    return "SER.DatasourceStatus.Name";
                }
            });
        }
        return calcFields;
    }

    @Override
    protected String getNoRowsKey() {
        return "SER.Devices.NoRows";
    }
}
