package org.imdc.cybersciences.ser;

import com.google.common.collect.Lists;
import com.inductiveautomation.ignition.common.BundleUtil;
import com.inductiveautomation.ignition.common.licensing.LicenseState;
import com.inductiveautomation.ignition.gateway.model.AbstractGatewayModuleHook;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.web.models.ConfigCategory;
import com.inductiveautomation.ignition.gateway.web.models.IConfigTab;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public class GatewayHook extends AbstractGatewayModuleHook {

    private final Logger logger = LoggerFactory.getLogger("SER.Gateway.Hook");
    private GatewayContext gatewayContext;
    private SERDeviceManager deviceManager;

    @Override
    public void setup(GatewayContext gatewayContext) {
        this.gatewayContext = gatewayContext;

        try {
            gatewayContext.getSchemaUpdater().updatePersistentRecords(SERDeviceRecord.META);
        } catch (SQLException e) {
            logger.error("Error verifying schemas", e);
        }

        this.deviceManager = SERDeviceManager.get();
        this.deviceManager.setGatewayContext(gatewayContext);

        BundleUtil.get().addBundle("SER", GatewayHook.class, "SER");
    }

    @Override
    public void startup(LicenseState licenseState) {
        logger.debug("Starting up module");

        try {
            deviceManager.startup();
        } catch (Throwable ex) {
            logger.error("Error starting up form collector manager", ex);
        }
    }

    @Override
    public void shutdown() {
        logger.debug("Shutting down module");

        try {
            deviceManager.shutdown();
        } catch (Throwable ex) {
            logger.error("Error shutting down form collector manager.", ex);
        }

        BundleUtil.get().removeBundle("SER");
    }

    @Override
    public List<? extends IConfigTab> getConfigPanels() {
        return Lists.newArrayList(SERDevicesManagerPage.MENU_ENTRY);
    }

    @Override
    public List<ConfigCategory> getConfigCategories() {
        return Lists.newArrayList(SERDevicesManagerPage.CONFIG_CATEGORY);
    }

    @Override
    public boolean isMakerEditionCompatible() {
        return true;
    }
}
