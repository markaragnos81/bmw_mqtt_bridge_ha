import re


EXACT_PROPERTY_LABELS = {
    "chargingLevelHv": "Akkustand",
    "electricalRange": "Elektrische Restreichweite",
    "fuelPercentage": "Tankfuellstand",
    "mileage": "Kilometerstand",
    "position": "Standort",
    "chargingStatus": "Ladestatus",
    "chargingConnectionType": "Ladeverbindung",
    "vehicle.drivetrain.batteryManagement.batterySizeMax": "Hochvoltbatterie Groesse",
    "vehicle.drivetrain.batteryManagement.maxEnergy": "Hochvoltbatterie Energieinhalt",
    "vehicle.drivetrain.batteryManagement.header": "Akkustand",
    "vehicle.drivetrain.fuelSystem.level": "Tankfuellstand",
    "vehicle.drivetrain.fuelSystem.remainingFuel": "Restkraftstoff",
    "vehicle.drivetrain.lastRemainingRange": "Restreichweite",
    "vehicle.drivetrain.totalRemainingRange": "Gesamtreichweite",
    "vehicle.drivetrain.avgElectricRangeConsumption": "Durchschnittsverbrauch elektrisch",
    "vehicle.drivetrain.electricEngine.remainingElectricRange": "Elektrische Restreichweite",
    "vehicle.drivetrain.electricEngine.kombiRemainingElectricRange": "Elektrische Restreichweite",
    "vehicle.drivetrain.electricEngine.charging.status": "Ladestatus",
    "vehicle.drivetrain.electricEngine.charging.method": "Lademethode",
    "vehicle.drivetrain.electricEngine.charging.connectionType": "Ladeverbindung",
    "vehicle.drivetrain.electricEngine.charging.phaseNumber": "Ladephasen",
    "vehicle.drivetrain.electricEngine.charging.chargingMode": "Ladebetriebsart",
    "vehicle.drivetrain.electricEngine.charging.timeRemaining": "Restladezeit",
    "vehicle.drivetrain.electricEngine.charging.timeToFullyCharged": "Zeit bis voll geladen",
    "vehicle.drivetrain.electricEngine.charging.hvStatus": "Hochvolt-Ladestatus",
    "vehicle.drivetrain.electricEngine.charging.connectorStatus": "Steckerstatus",
    "vehicle.drivetrain.electricEngine.charging.isSingleImmediateCharging": "Sofortladen aktiv",
    "vehicle.drivetrain.electricEngine.charging.isImmediateChargingSystemReason": "Sofortladen durch System aktiviert",
    "vehicle.drivetrain.electricEngine.charging.windowSelection": "Ladefenster ausgewaehlt",
    "vehicle.drivetrain.electricEngine.charging.modeDeviation": "Abweichung Ladebetriebsart",
    "vehicle.drivetrain.electricEngine.charging.lastChargingResult": "Ergebnis letzter Ladevorgang",
    "vehicle.drivetrain.electricEngine.charging.lastChargingReason": "Grund letzter Ladevorgang",
    "vehicle.drivetrain.electricEngine.charging.reasonChargingEnd": "Grund Ladeende",
    "vehicle.drivetrain.electricEngine.charging.routeOptimizedChargingStatus": "Routenoptimiertes Laden Status",
    "vehicle.drivetrain.electricEngine.charging.authentication.status": "Plug-and-Charge Autorisierung",
    "vehicle.drivetrain.electricEngine.charging.smeEnergyDeltaFullyCharged": "Energie bis voll geladen",
    "vehicle.drivetrain.electricEngine.charging.profile.mode": "Ladeprofil Modus",
    "vehicle.drivetrain.electricEngine.charging.profile.preference": "Ladeprofil Praeferenz",
    "vehicle.drivetrain.electricEngine.charging.profile.climatizationActive": "Ladeprofil Klimatisierung aktiv",
    "vehicle.drivetrain.electricEngine.charging.profile.isRcpConfigComplete": "Ladeprofil vollstaendig konfiguriert",
    "vehicle.drivetrain.electricEngine.charging.profile.timerType": "Ladeprofil Timer-Typ",
    "vehicle.drivetrain.electricEngine.charging.acVoltage": "Ladespannung AC",
    "vehicle.drivetrain.electricEngine.charging.acAmpere": "Ladestrom AC",
    "vehicle.drivetrain.electricEngine.charging.acRestriction.isChosen": "AC-Begrenzung aktiv",
    "vehicle.drivetrain.electricEngine.charging.acRestriction.factor": "AC-Begrenzung Faktor",
    "vehicle.drivetrain.electricEngine.charging.hvpmFinishReason": "Grund Ladeende Hochvolt",
    "vehicle.vehicle.travelledDistance": "Kilometerstand",
    "vehicle.vehicle.avgSpeed": "Durchschnittsgeschwindigkeit",
    "vehicle.vehicle.avgAuxPower": "Nebenverbraucherleistung",
    "vehicle.vehicle.averageWeeklyDistanceShortTerm": "Wochendistanz kurzfristig",
    "vehicle.vehicle.averageWeeklyDistanceLongTerm": "Wochendistanz langfristig",
    "vehicle.vehicle.deepSleepModeActive": "Tiefschlafmodus aktiv",
    "vehicle.vehicle.timeSetting": "Zeiteinstellung",
    "vehicle.vehicle.preConditioning.activity": "Vorkonditionierung Aktivitaet",
    "vehicle.vehicle.preConditioning.remainingTime": "Vorkonditionierung Restzeit",
    "vehicle.vehicle.preConditioning.error": "Vorkonditionierung Fehler",
    "vehicle.vehicle.preConditioning.isRemoteEngineRunning": "Fernstart aktiv",
    "vehicle.vehicle.preConditioning.isRemoteEngineStartAllowed": "Fernstart erlaubt",
    "vehicle.vehicle.antiTheftAlarmSystem.alarm.isOn": "Diebstahlwarnanlage Alarm aktiv",
    "vehicle.vehicle.antiTheftAlarmSystem.alarm.activationTime": "Diebstahlwarnanlage letzte Ausloesung",
    "vehicle.vehicle.antiTheftAlarmSystem.alarm.armStatus": "Diebstahlwarnanlage Schaerfungsstatus",
    "vehicle.vehicle.speedRange.lowerBound": "Geschwindigkeitsbereich Untergrenze",
    "vehicle.vehicle.speedRange.upperBound": "Geschwindigkeitsbereich Obergrenze",
    "vehicle.body.hood.isOpen": "Motorhaube offen",
    "vehicle.body.trunk.isOpen": "Kofferraum offen",
    "vehicle.body.trunk.isLocked": "Kofferraum verriegelt",
    "vehicle.body.trunk.door.isOpen": "Kofferraumklappe offen",
    "vehicle.body.trunk.window.isOpen": "Heckscheibe entriegelt",
    "vehicle.body.flap.isLocked": "Ladeklappe verriegelt",
    "vehicle.body.flap.isPermanentlyUnlocked": "Ladeklappe dauerhaft entriegelt",
    "vehicle.body.lights.isRunningOn": "Licht aktiv",
    "vehicle.body.chargingPort.status": "Ladeanschluss Status",
    "vehicle.body.chargingPort.dcStatus": "DC-Ladeanschluss Status",
    "vehicle.body.chargingPort.statusClearText": "Ladeanschluss Klartext",
    "vehicle.body.chargingPort.isHospitalityActive": "Ladeanschluss Auto-Entriegelung",
    "vehicle.body.chargingPort.plugEventId": "Ladeanschluss Plug-Event-ID",
    "vehicle.cabin.door.status": "Tueren verriegelt",
    "vehicle.cabin.sunroof.status": "Schiebedach Status",
    "vehicle.cabin.sunroof.overallStatus": "Schiebedach Gesamtstatus",
    "vehicle.cabin.sunroof.tiltStatus": "Schiebedach Kippstatus",
    "vehicle.cabin.sunroof.relativePosition": "Schiebedach Oeffnungsposition",
    "vehicle.cabin.sunroof.shade.position": "Sonnenblende Schiebedach Position",
    "vehicle.cabin.steeringWheel.heating": "Lenkradheizung",
    "vehicle.cabin.hvac.statusAirPurification": "Luftreinigung Status",
    "vehicle.cabin.infotainment.isMobilePhoneConnected": "Mobiltelefon verbunden",
    "vehicle.cabin.infotainment.navigation.remainingRange": "Navigations-Restreichweite",
    "vehicle.cabin.infotainment.navigation.destinationSet.distance": "Distanz zum Navigationsziel",
    "vehicle.cabin.infotainment.navigation.destinationSet.arrivalTime": "Ankunftszeit Navigationsziel",
    "vehicle.cabin.infotainment.navigation.pointsOfInterests.available": "Verfuegbare POIs",
    "vehicle.cabin.infotainment.navigation.pointsOfInterests.max": "Maximale POIs",
    "vehicle.cabin.infotainment.displayUnit.distance": "Distanzanzeige Einheit",
    "vehicle.cabin.infotainment.hmi.distanceUnit": "HMI Distanz-Einheit",
    "vehicle.cabin.convertible.roofStatus": "Cabrioverdeck Status",
    "vehicle.cabin.convertible.roofRetractableStatus": "Faltdach Status",
    "vehicle.channel.ngtp.timeVehicle": "Fahrzeugzeit",
    "vehicle.channel.teleservice.status": "Teleservice Verfuegbarkeit",
    "vehicle.channel.teleservice.lastAutomaticServiceCallTime": "Letzter automatischer Service-Call",
    "vehicle.channel.teleservice.lastTeleserviceReportTime": "Letzter Teleservice-Report",
    "vehicle.status.serviceDistance.next": "Kilometer bis Service",
    "vehicle.status.serviceDistance.yellow": "Servicehinweis Schwelle Kilometer",
    "vehicle.status.serviceTime.inspectionDateLegal": "Naechste Inspektion",
    "vehicle.status.serviceTime.yellow": "Servicehinweis Schwelle Zeit",
    "vehicle.status.serviceTime.hUandAuServiceYellow": "HU/AU Hinweis Schwelle",
    "vehicle.status.conditionBasedServicesCount": "Anzahl Servicebedarfsmeldungen",
    "vehicle.isMoving": "Fahrzeug in Bewegung",
    "vehicle.electricalSystem.battery.voltage": "Bordnetzspannung",
    "vehicle.electricalSystem.battery.stateOfCharge": "12V Batterie Ladestand",
    "vehicle.electricalSystem.battery48V.stateOfHealth.displayed": "48V Batterie Gesundheitszustand",
    "vehicle.drivetrain.engine.isActive": "Motor aktiv",
    "vehicle.drivetrain.engine.isIgnitionOn": "Zuendung an",
    "vehicle.drivetrain.internalCombustionEngine.engine.ect": "Kuehlmitteltemperatur",
    "vehicle.electronicControlUnit.diagnosticTroubleCodes.raw": "Diagnosefehler Rohdaten",
    "vehicle.powertrain.electric.battery.charging.preferenceSmartCharging": "Smart Charging Praeferenz",
    "vehicle.powertrain.electric.battery.charging.acLimit.selected": "AC-Ladestrom Soll",
    "vehicle.powertrain.electric.battery.charging.acLimit.min": "AC-Ladestrom Minimum",
    "vehicle.powertrain.electric.battery.charging.acLimit.max": "AC-Ladestrom Maximum",
    "vehicle.powertrain.electric.battery.charging.acLimit.isActive": "AC-Ladestrombegrenzung aktiv",
    "vehicle.powertrain.electric.battery.charging.power": "Ladeleistung",
    "vehicle.powertrain.electric.battery.stateOfCharge.target": "Ziel-Akkustand",
    "vehicle.powertrain.electric.battery.stateOfCharge.targetMin": "Minimaler Ziel-Akkustand",
    "vehicle.powertrain.electric.battery.stateOfCharge.targetSoCForProfessionalMode": "Profi-Modus Ziel-Akkustand",
    "vehicle.powertrain.electric.battery.preconditioning.manualMode.statusFeedback": "Batterievorkonditionierung manuell",
    "vehicle.powertrain.electric.battery.preconditioning.automaticMode.statusFeedback": "Batterievorkonditionierung automatisch",
    "vehicle.powertrain.electric.battery.preconditioning.state": "Batterievorkonditionierung Status",
    "vehicle.powertrain.electric.range.target": "Elektrische Ziel-Reichweite",
    "vehicle.powertrain.electric.range.displayControl": "Anzeige elektrische Reichweite",
    "vehicle.powertrain.electric.departureTime.displayControl": "Anzeige Abfahrtszeit",
    "vehicle.powertrain.electric.chargingDuration.displayControl": "Anzeige Ladedauer",
    "vehicle.powertrain.tractionBattery.charging.port.anyPosition.isPlugged": "Ladekabel angeschlossen",
    "vehicle.trip.segment.end.time": "Letzte Fahrt Ende",
    "vehicle.trip.segment.end.travelledDistance": "Kilometerstand nach letzter Fahrt",
    "vehicle.trip.segment.end.drivetrain.batteryManagement.hvSoc": "Akkustand letzte Fahrt Ende",
    "vehicle.trip.segment.accumulated.acceleration.starsAverage": "Fahrstil Sterne Beschleunigung",
    "vehicle.trip.segment.accumulated.chassis.brake.starsAverage": "Fahrstil Sterne Bremsen",
    "vehicle.trip.segment.accumulated.drivetrain.transmission.setting.fractionDriveElectric": "Fahrtanteil elektrisch",
    "vehicle.trip.segment.accumulated.drivetrain.transmission.setting.fractionDriveEcoPro": "Fahrtanteil ECO PRO",
    "vehicle.trip.segment.accumulated.drivetrain.transmission.setting.fractionDriveEcoProPlus": "Fahrtanteil ECO PRO Plus",
    "vehicle.trip.segment.accumulated.drivetrain.electricEngine.energyConsumptionComfort": "Energieverbrauch Comfort",
    "vehicle.trip.segment.accumulated.drivetrain.electricEngine.recuperationTotal": "Rekuperation gesamt",
}


ROW_LABELS = {"row1": "vorne", "row2": "hinten", "row3": "3. Reihe"}
SIDE_LABELS = {
    "driver": "Fahrer",
    "driverSide": "Fahrerseite",
    "passenger": "Beifahrer",
    "passengerSide": "Beifahrerseite",
    "left": "links",
    "right": "rechts",
    "frontLeft": "vorne links",
    "frontMiddle": "vorne mittig",
    "frontRight": "vorne rechts",
    "rearLeft": "hinten links",
    "rearMiddle": "hinten mittig",
    "rearRight": "hinten rechts",
    "anyPosition": "beliebige Position",
}


def _camel_to_words(text: str) -> str:
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text or "").replace("_", " ").strip()


def _seat_label(kind: str, row: str, side: str) -> str:
    kind_label = "Sitzheizung" if kind == "heating" else "Sitzkuehlung"
    return f"{kind_label} {SIDE_LABELS.get(side, side)} {ROW_LABELS.get(row, row)}"


def friendly_bmw_property_name(prop: str) -> str:
    prop = (prop or "").strip()
    if not prop:
        return "BMW Wert"

    if prop in EXACT_PROPERTY_LABELS:
        return EXACT_PROPERTY_LABELS[prop]

    if "/" in prop:
        base, leaf = prop.rsplit("/", 1)
        base_label = friendly_bmw_property_name(base)
        leaf_label = _camel_to_words(leaf).title()
        return f"{base_label} {leaf_label}".strip()

    m = re.match(r"^vehicle\.cabin\.door\.(row[123])\.(driver|passenger)\.(isOpen|position)$", prop)
    if m:
        row, side, metric = m.groups()
        metric_label = "offen" if metric == "isOpen" else "Oeffnungsposition"
        return f"Tuer {SIDE_LABELS[side]} {ROW_LABELS[row]} {metric_label}"

    m = re.match(r"^vehicle\.cabin\.window\.(row[123])\.(driver|passenger)\.(status)$", prop)
    if m:
        row, side, _metric = m.groups()
        return f"Fenster {SIDE_LABELS[side]} {ROW_LABELS[row]} Status"

    m = re.match(r"^vehicle\.cabin\.seat\.(row[123])\.(driverSide|passengerSide)\.(heating|cooling)$", prop)
    if m:
        row, side, kind = m.groups()
        return _seat_label(kind, row, side)

    m = re.match(
        r"^vehicle\.cabin\.hvac\.preconditioning\.configuration\.(directStartSettings|defaultSettings)\.seat\.(row[123])\.(driverSide|passengerSide)\.(heating|cooling)$",
        prop,
    )
    if m:
        settings, row, side, kind = m.groups()
        settings_label = "Direktstart" if settings == "directStartSettings" else "Standard"
        return f"Vorklimatisierung {settings_label} {_seat_label(kind, row, side)}"

    m = re.match(
        r"^vehicle\.cabin\.hvac\.preconditioning\.configuration\.(directStartSettings|defaultSettings)\.(steeringWheel\.heating|targetTemperature)$",
        prop,
    )
    if m:
        settings, metric = m.groups()
        settings_label = "Direktstart" if settings == "directStartSettings" else "Standard"
        if metric == "steeringWheel.heating":
            return f"Vorklimatisierung {settings_label} Lenkradheizung"
        return f"Vorklimatisierung {settings_label} Solltemperatur"

    m = re.match(
        r"^vehicle\.cabin\.hvac\.preconditioning\.status\.(rearDefrostActive|progress|comfortState|remainingRunningTime|isExteriorMirrorHeatingActive)$",
        prop,
    )
    if m:
        labels = {
            "rearDefrostActive": "Vorklimatisierung Heckscheibenheizung aktiv",
            "progress": "Vorklimatisierung Fortschritt",
            "comfortState": "Vorklimatisierung Komfortstatus",
            "remainingRunningTime": "Vorklimatisierung Restlaufzeit",
            "isExteriorMirrorHeatingActive": "Vorklimatisierung Spiegelheizung aktiv",
        }
        return labels[m.group(1)]

    m = re.match(r"^vehicle\.body\.trunk\.(left|right|upper|lower)\.door\.isOpen$", prop)
    if m:
        side = m.group(1)
        side_label = {
            "left": "links",
            "right": "rechts",
            "upper": "oben",
            "lower": "unten",
        }[side]
        return f"Kofferraumtuer {side_label} offen"

    m = re.match(r"^vehicle\.chassis\.axle\.(row[12])\.wheel\.(left|right)\.tire\.(pressure|pressureTarget|temperature)$", prop)
    if m:
        axle, side, metric = m.groups()
        metric_label = {
            "pressure": "Reifendruck",
            "pressureTarget": "Reifendruck Soll",
            "temperature": "Reifentemperatur",
        }[metric]
        return f"{metric_label} {ROW_LABELS[axle]} {SIDE_LABELS[side]}"

    m = re.match(r"^vehicle\.cabin\.infotainment\.navigation\.currentLocation\.(heading|latitude|longitude|altitude|numberOfSatellites|fixStatus)$", prop)
    if m:
        labels = {
            "heading": "Standort Richtung",
            "latitude": "Standort Breitengrad",
            "longitude": "Standort Laengengrad",
            "altitude": "Standort Hoehe",
            "numberOfSatellites": "Standort Satelliten",
            "fixStatus": "Standort GPS-Fix",
        }
        return labels[m.group(1)]

    m = re.match(r"^vehicle\.powertrain\.tractionBattery\.charging\.port\.(anyPosition|rearLeft|rearMiddle|rearRight|frontLeft|frontMiddle|frontRight)\.(isPlugged|flap\.isAutomaticOpenAndCloseActive|flap\.isOpen)$", prop)
    if m:
        port, metric = m.groups()
        port_label = SIDE_LABELS.get(port, port)
        metric_label = {
            "isPlugged": "Ladekabel angeschlossen",
            "flap.isAutomaticOpenAndCloseActive": "Ladeklappe Auto-oeffnen aktiv",
            "flap.isOpen": "Ladeklappe offen",
        }[metric]
        return f"{metric_label} {port_label}"

    m = re.match(r"^vehicle\.cabin\.climate\.timers\.(overwriteTimer|weekdaysTimer1|weekdaysTimer2)\.(action|hour|minute)$", prop)
    if m:
        timer, metric = m.groups()
        timer_label = {
            "overwriteTimer": "Klimatimer Override",
            "weekdaysTimer1": "Klimatimer Werktage 1",
            "weekdaysTimer2": "Klimatimer Werktage 2",
        }[timer]
        metric_label = {"action": "Aktion", "hour": "Stunde", "minute": "Minute"}[metric]
        return f"{timer_label} {metric_label}"

    words = [_camel_to_words(part) for part in prop.split(".") if part and part not in {"vehicle", "cabin", "body", "drivetrain", "chassis"}]
    cleaned = " ".join(word for word in words if word)
    return cleaned or "BMW Wert"
