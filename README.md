# Sensor Bundle Module

The `viam:sensor-bundle` module provides these models:

1. **`viam:sensor-bundle:stateful-sensor`** - A sensor component that holds a value you set via `DoCommand`, serves it through the Readings API, and persists it to a file on disk so it survives restarts.

---

## Model: `viam:sensor-bundle:stateful-sensor`

**API:** `rdk:component:sensor`

A stateful sensor that holds an arbitrary value. Set the value with the `set` DoCommand; read it back through the Readings API. The value is kept in memory and persisted to a JSON file. On initialization the value is loaded from that file; if the file does not exist it is created. Writes are atomic (temp file + rename) so a crash mid-write cannot corrupt the state file.

### Configuration

```json
{
  "file_path": "<string>"
}
```

| Name        | Type   | Required | Description                                                                                                  |
| ----------- | ------ | -------- | ------------------------------------------------------------------------------------------------------------ |
| `file_path` | string | No       | Where the value is persisted. Defaults to `<resource-name>_state.json` in the module's working directory.    |

### Example Configuration

```json
{
  "file_path": "/var/lib/viam/stateful-sensor.json"
}
```

### Readings

Returns the value most recently set (or the value loaded from `file_path` on startup). Before anything has been set, it returns an empty object.

```json
{
  "temperature": 72.5,
  "unit": "F"
}
```

### DoCommand

**`set`** - Replace the value the sensor holds. The object provided under `set` becomes the sensor's value, is persisted to `file_path`, and is what Readings returns.

```json
{
  "set": {
    "temperature": 72.5,
    "unit": "F"
  }
}
```

Returns:

```json
{"set": "ok"}
```
