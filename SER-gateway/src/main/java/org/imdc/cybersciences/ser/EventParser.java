package org.imdc.cybersciences.ser;

import com.inductiveautomation.ignition.gateway.datasource.SRConnection;
import com.inductiveautomation.ignition.gateway.history.DatasourceData;
import com.inductiveautomation.ignition.gateway.history.HistoryFlavor;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EventParser {
    static SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static Event parse(String parentLog, String insertQuery, String event) {
        event = event.replace("-", "");

        Long eventDescPart1 = Long.parseLong(event.substring(0, 4), 16);
        Long eventDescPart2 = Long.parseLong(event.substring(4, 8), 16);

        EventCode eventCode = EventCode.values()[(int) (eventDescPart1 & 0x1F)];
        Integer inputs = (int) (eventDescPart1 >> 5) & 0x1F;
        InputStatus inputStatus = InputStatus.values()[(int) (eventDescPart1 >> 10) & 0x1];
        DST dst = DST.values()[(int) (eventDescPart1 >> 11) & 0x1];

        TimeQuality timeQuality = TimeQuality.values()[(int) (eventDescPart2 >> 14) & 0x3];

        Integer ms = (int) (eventDescPart2 & 0x3FF);
        Integer seconds = 441792000 + Integer.parseInt(event.substring(12, 16) + event.substring(8, 12), 16);
        Long milliseconds = seconds * 1000L + ms;

        Integer sequenceNumber = Integer.parseInt(event.substring(20, 24) + event.substring(16, 20), 16);
        Long inputCoincidentStatus = Long.parseLong(event.substring(28, 32) + event.substring(24, 28), 16);

        return new Event(parentLog, insertQuery, eventCode, inputs, sequenceNumber, inputCoincidentStatus, inputStatus, dst, timeQuality, milliseconds);
    }

    public static class Event implements DatasourceData {
        private String parentLog, insertQuery;
        private EventCode eventCode;
        private Integer input, sequenceNumber;
        private Long inputCoincidentStatus;
        private InputStatus inputStatus;
        private DST dst;
        private TimeQuality timeQuality;
        private Long msTime;

        public Event(String parentLog, String insertQuery, EventCode eventCode, Integer input, Integer sequenceNumber, Long inputCoincidentStatus, InputStatus inputStatus, DST dst, TimeQuality timeQuality, Long msTime) {
            this.parentLog = parentLog;
            this.insertQuery = insertQuery;
            this.eventCode = eventCode;
            this.input = input + 1;
            this.sequenceNumber = sequenceNumber;
            this.inputCoincidentStatus = inputCoincidentStatus;
            this.inputStatus = inputStatus;
            this.dst = dst;
            this.timeQuality = timeQuality;
            this.msTime = msTime;
        }

        public EventCode getEventCode() {
            return eventCode;
        }

        public Integer getInput() {
            return input;
        }

        public Integer getSequenceNumber() {
            return sequenceNumber;
        }

        public Long getInputCoincidentStatus() {
            return inputCoincidentStatus;
        }

        public InputStatus getInputStatus() {
            return inputStatus;
        }

        public DST getDst() {
            return dst;
        }

        public TimeQuality getTimeQuality() {
            return timeQuality;
        }

        public Long getMsTime() {
            return msTime;
        }

        public Date getDate() {
            return new Date(getMsTime());
        }

        public void writeTags(TagManager tagManager, String tagPrefix) {
            tagManager.tagUpdate(String.format("%s/LastEvent/SequenceNumber", tagPrefix), getSequenceNumber());
            tagManager.tagUpdate(String.format("%s/LastEvent/EventCode", tagPrefix), getEventCode().getDisplay());
            tagManager.tagUpdate(String.format("%s/LastEvent/Channel", tagPrefix), getInput());
            tagManager.tagUpdate(String.format("%s/LastEvent/Status", tagPrefix), getInputStatus().toString());
            tagManager.tagUpdate(String.format("%s/LastEvent/CoincidentStatus", tagPrefix), getInputCoincidentStatus().toString());
            tagManager.tagUpdate(String.format("%s/LastEvent/Timestamp", tagPrefix), getMsTime());
            tagManager.tagUpdate(String.format("%s/LastEvent/DST", tagPrefix), getDst().toString());
            tagManager.tagUpdate(String.format("%s/LastEvent/TimeQuality", tagPrefix), getTimeQuality().toString());
        }

        @Override
        public String toString() {
            return "Event{" +
                    "eventCode=" + eventCode +
                    ", input=" + input +
                    ", sequenceNumber=" + sequenceNumber +
                    ", inputCoincidentStatus=" + inputCoincidentStatus +
                    ", inputStatus=" + inputStatus +
                    ", dst=" + dst +
                    ", timeQuality=" + timeQuality +
                    ", msTime=" + msTime +
                    '}';
        }

        @Override
        public void storeToConnection(SRConnection conn) throws Exception {
            if (insertQuery != null) {
                conn.runPrepUpdate(
                        insertQuery,
                        getSequenceNumber(),
                        getMsTime(),
                        getEventCode().ordinal(),
                        getEventCode().getDisplay(),
                        getInput(),
                        getInputStatus().toString(),
                        getInputCoincidentStatus().toString(),
                        getTimeQuality().toString()
                );
            }
        }

        @Override
        public HistoryFlavor getFlavor() {
            return FLAVOR;
        }

        @Override
        public String getSignature() {
            return "SER Event Data";
        }

        @Override
        public int getDataCount() {
            return 1;
        }

        @Override
        public String getLoggerName() {
            return parentLog + ".StoreAndForward";
        }
    }

    public enum EventCode {
        Code0("Reserved"),
        Code1("Input Status Change"),
        Code2("Input Enabled for Event Recording (by User)"),
        Code3("Input Disabled for Event Recording (by User)"),
        Code4("Input Chatter Count Off (Event Recording Resumed)"),
        Code5("Input Chatter Count Off (Event Recording Suspended)"),
        Code6("Power On"),
        Code7("SER Inter-Device (RS-485) Time Sync Lock"),
        Code8("SER Inter-Device (RS-485) Time Sync Fail"),
        Code9("Internal Error"),
        Code10("Event Log Cleared"),
        Code11("24V Power Loss"),
        Code12("24V Power Restored"),
        Code13("Reserved"),
        Code14("Manual Time Set"),
        Code15("Setup Configuration Changed"),
        Code16("Daylight Saving Time (DST) Start/End Switchover"),
        Code17("Reset"),
        Code18("Firmware Upgraded"),
        Code19("Power Fail"),
        Code20("PTP/NTP Time Sync Lock"),
        Code21("PTP/NTP Time Sync Fail"),
        Code22("Time Sync Lock"),
        Code23("Time Sync Fail"),
        Code24("Test Mode On"),
        Code25("Test Mode Off"),
        Code26("High-Speed Trigger Out"),
        Code27("Test Mode Input Status Change"),
        Code28("Reserved"),
        Code29("RTC Battery Low"),
        Code30("Power Control Module Issue"),
        Code31("Reserved");

        private String display;

        EventCode(String display) {
            this.display = display;
        }

        public String getDisplay() {
            return display;
        }
    }

    public enum InputStatus {
        Off, On;
    }

    public enum DST {
        STD, DST;
    }

    public enum TimeQuality {
        Good, Fair, Poor, Bad;
    }
}
