"""
Student Name :  Charlene LAGADEC
Student ID   :  014962406
"""
class Data:
    
    def __init__(self, energyRate, batteryHealth, batteryTemperature, batteryVoltage, cpuUsage, distanceTraveled, 
                 mobileDataActivity, mobileDataStatus, mobileNetworkType, networkType, roamingEnabled, screenBrightness, 
                 wifiLinkSpeed, wifiSignalStrength):
        self.energyRate = energyRate
        self.batteryHealth = batteryHealth
        self.batteryTemperature = batteryTemperature
        self.batteryVoltage = batteryVoltage
        self.cpuUsage = cpuUsage
        self.distanceTraveled = distanceTraveled
        self.mobileDataActivity = mobileDataActivity
        self.mobileDataStatus = mobileDataStatus
        self.mobileNetworkType = mobileNetworkType
        self.networkType = networkType
        self.roamingEnabled = roamingEnabled
        self.screenBrightness = screenBrightness
        self.wifiLinkSpeed = wifiLinkSpeed
        self.wifiSignalStrength = wifiSignalStrength
        
    def set(self, energyRate, batteryHealth, batteryTemperature, batteryVoltage, cpuUsage, distanceTraveled, 
                 mobileDataActivity, mobileDataStatus, mobileNetworkType, networkType, roamingEnabled, screenBrightness, 
                 wifiLinkSpeed, wifiSignalStrength):
        self.energyRate = energyRate
        self.batteryHealth = batteryHealth
        self.batteryTemperature = batteryTemperature
        self.batteryVoltage = batteryVoltage
        self.cpuUsage = cpuUsage
        self.distanceTraveled = distanceTraveled
        self.mobileDataActivity = mobileDataActivity
        self.mobileDataStatus = mobileDataStatus
        self.mobileNetworkType = mobileNetworkType
        self.networkType = networkType
        self.roamingEnabled = roamingEnabled
        self.screenBrightness = screenBrightness
        self.wifiLinkSpeed = wifiLinkSpeed
        self.wifiSignalStrength = wifiSignalStrength
        return self