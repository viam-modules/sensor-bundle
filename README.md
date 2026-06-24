# Sensor Bundle Module

The `viam:sensor-bundle` module provides these models:

1. **`viam:sensor-bundle:stateful-sensor`** - A sensor component that holds a value you set via `DoCommand`, serves it through the Readings API, and persists it to a file on disk so it survives restarts.
2. **`viam:sensor-bundle:sensor-monitor`** - A sensor component that watches another sensor's readings against numeric trigger rules and sends a notification via a generic service's `DoCommand` when a threshold is crossed.

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
| `file_path` | string | No       | Where the value is persisted. Defaults to `<resource-name>_state.json` inside the module's data directory (the writable path Viam provides via `VIAM_MODULE_DATA`), falling back to the working directory when that is unset. |

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

---

## Model: `viam:sensor-bundle:sensor-monitor`

**API:** `rdk:component:sensor`

Monitors the readings of another sensor and fires notifications when a numeric reading crosses a configured threshold. It takes two dependencies — the sensor to watch and a generic service to notify — and evaluates a set of numeric trigger rules on every poll.

When a rule fires, the monitor calls `DoCommand` on the notifier with the payload:

```json
{"command": "send", "text": "<notification message>"}
```

Notifications are **edge-triggered**: a message is sent when a rule transitions from not-triggered to triggered. While the rule stays triggered no further message is sent, unless `cooldown_seconds` is set, in which case the message repeats at most once per cooldown window. When the reading clears the threshold and crosses it again, a new notification is sent.

**Actions.** Besides notifying, a rule can fire arbitrary `DoCommand`s on other resources when it triggers and when it resolves, via the `on_trigger` and `on_resolve` lists. Each action names a `resource` (any component or service on the machine, by name) and a `command` (the `DoCommand` payload). Actions fire on the edges only — once when the rule transitions to triggered, once when it clears — not on `cooldown_seconds` repeats. Each named resource is automatically added as a dependency. Actions are best-effort: a failure is logged and never affects monitoring.

### Configuration

```json
{
  "sensor": "<string>",
  "notifier": "<string>",
  "poll_interval_seconds": <number>,
  "cooldown_seconds": <number>,
  "rules": [
    {
      "key": "<string>",
      "operator": "<string>",
      "threshold": <number>,
      "message": "<string>",
      "on_trigger": [{ "resource": "<string>", "command": { } }],
      "on_resolve": [{ "resource": "<string>", "command": { } }]
    }
  ]
}
```

| Name                    | Type   | Required | Description                                                                                                                                                  |
| ----------------------- | ------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sensor`                | string | **Yes**  | Name of the sensor dependency whose readings are monitored.                                                                                                  |
| `notifier`              | string | **Yes**  | Name of the generic service dependency that receives notification `DoCommand`s.                                                                              |
| `rules`                 | array  | **Yes**  | One or more numeric trigger rules (see below). At least one rule is required.                                                                                |
| `poll_interval_seconds` | number | No       | How often the sensor is polled, in seconds. Defaults to `10`.                                                                                                |
| `cooldown_seconds`      | number | No       | Minimum time between repeat notifications while a rule stays triggered. `0` (default) means no repeat until the rule clears and fires again.                  |

Each entry in `rules`:

| Name        | Type   | Required | Description                                                                                                                                                  |
| ----------- | ------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `key`       | string | **Yes**  | The reading key to watch, e.g. `"temperature"`. Non-numeric or missing values are skipped.                                                                   |
| `operator`  | string | **Yes**  | Comparison applied between the reading and `threshold`. One of `>`, `>=`, `<`, `<=`, `==`, `!=` (aliases: `gt`, `gte`, `lt`, `lte`, `eq`, `ne`).             |
| `threshold` | number | **Yes**  | The value the reading is compared against.                                                                                                                   |
| `message`   | string | No       | Notification template. Supports placeholders `{key}`, `{value}`, `{threshold}`, `{operator}`. If omitted, a message like `temperature is 95 (> 90)` is used. |
| `on_trigger` | array  | No       | DoCommands to fire when the rule transitions to triggered. Each is `{ "resource": <name>, "command": <DoCommand payload> }`. |
| `on_resolve` | array  | No       | DoCommands to fire when the rule clears (reading returns to the non-triggered side of the threshold). Same shape as `on_trigger`. |

### Example Configuration

```json
{
  "sensor": "outdoor-temp",
  "notifier": "slack-notifier",
  "poll_interval_seconds": 30,
  "cooldown_seconds": 3600,
  "rules": [
    {
      "key": "temperature",
      "operator": ">",
      "threshold": 90,
      "message": "High temperature alert: {key} is {value} (limit {threshold})",
      "on_trigger": [{ "resource": "cooling-fan", "command": { "set": { "power": 1 } } }],
      "on_resolve": [{ "resource": "cooling-fan", "command": { "set": { "power": 0 } } }]
    },
    {
      "key": "humidity",
      "operator": "<",
      "threshold": 20
    }
  ]
}
```

### Readings

Returns the most recent readings from the monitored sensor, plus a `<key>_triggered` boolean for each rule indicating whether it is currently firing.

```json
{
  "temperature": 95.0,
  "humidity": 35.0,
  "temperature_triggered": true,
  "humidity_triggered": false
}
```

### DoCommand

**`check`** - Force an immediate poll of the sensor (instead of waiting for the next interval), evaluating all rules and sending any notifications.

```json
{"check": true}
```

Returns:

```json
{"check": "ok"}
```
