# Changelog

## 3.2.25
- Improved friendly names for dynamically discovered Home Assistant entities while keeping original BMW MQTT topic names unchanged.

## 3.2.24
- Added a REST snapshot fallback for missing BMW core values such as SoC, range, mileage, fuel level, and position.
- Cached raw vehicle API responses locally so fallback values can be reused without unnecessary extra requests.

## 3.2.23
- Fixed Home Assistant MQTT discovery IDs for streamed BMW properties by converting them to a safe ID format.

## 3.2.22
- Improved dynamic Home Assistant discovery for streamed BMW properties.
- Added automatic typing for boolean and numeric values, including device classes and state classes where possible.

## 3.2.21
- Added BMW MQTT subscribe acknowledgement and RX diagnostics to simplify stream debugging.

## 3.2.20
- Added automatic discovery for streamed BMW properties so newly received BMW fields appear in Home Assistant.

## 3.2.19
- Fixed BMW stream client ID handling and updated payload parsing for current BMW CarData streaming messages.
