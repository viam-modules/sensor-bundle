# Sensor Bundle Module

The `viam:sensor-bundle` module provides these models:

1. **`viam:sensor-bundle:stateful-sensor`** - A sensor component that holds a value you set via `DoCommand`, serves it through the Readings API, and persists it to a file on disk so it survives restarts.
2. **`viam:sensor-bundle:sensor-monitor`** - A sensor component that watches another sensor's readings against numeric trigger rules and fires `DoCommand` actions on other resources when a rule triggers or resolves.

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

Monitors the readings of another sensor and, when a numeric reading crosses a configured threshold, fires `DoCommand` **actions** on other resources. It evaluates a set of numeric trigger rules on every poll; the only dependency is the sensor to watch (plus whatever resources the actions target).

**Actions.** Each rule has an `on_trigger` and an `on_resolve` list. Every action names a `resource` (any component or service on the machine, by name) and a `command` (the `DoCommand` payload sent to it).

- `on_trigger` fires when the rule transitions from not-triggered to triggered, and again at most once per `cooldown_seconds` while it stays triggered (with the default `cooldown_seconds: 0`, that's edge-only).
- `on_resolve` fires once when the reading clears the threshold.
- Actions are best-effort: a failure is logged and never affects monitoring. Each named resource is automatically added as a dependency.

**References and captures.** String values in a `command` may embed `${...}` references, resolved just before the command is sent:

- Rule/reading context: `${value}` (the current reading), `${key}`, `${threshold}`, `${operator}`.
- Captured responses: set `"capture": "<name>"` on an action to store its `DoCommand` response, then reference its fields from later actions (including `on_resolve`) as `${<name>.<field>}`.

A value that is exactly one reference keeps the referenced value's type; a reference embedded in a larger string is substituted as text. If a reference can't be resolved (e.g. nothing was captured), that action is skipped. A capture is overwritten on each cooldown re-fire (so resolve references the most recent response) and cleared once the rule resolves.

### Configuration

```json
{
  "sensor": "<string>",
  "poll_interval_seconds": <number>,
  "cooldown_seconds": <number>,
  "rules": [
    {
      "key": "<string>",
      "operator": "<string>",
      "threshold": <number>,
      "on_trigger": [{ "resource": "<string>", "command": { }, "capture": "<string>" }],
      "on_resolve": [{ "resource": "<string>", "command": { } }]
    }
  ]
}
```

| Name                    | Type   | Required | Description                                                                                                                                                  |
| ----------------------- | ------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sensor`                | string | **Yes**  | Name of the sensor dependency whose readings are monitored.                                                                                                  |
| `rules`                 | array  | **Yes**  | One or more numeric trigger rules (see below). At least one rule is required.                                                                                |
| `poll_interval_seconds` | number | No       | How often the sensor is polled, in seconds. Defaults to `10`.                                                                                                |
| `cooldown_seconds`      | number | No       | Minimum time between repeat `on_trigger` firings while a rule stays triggered. `0` (default) means fire only on the edge.                                     |

Each entry in `rules`:

| Name        | Type   | Required | Description                                                                                                                                                  |
| ----------- | ------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `key`       | string | **Yes**  | The reading key to watch, e.g. `"temperature"`. Non-numeric or missing values are skipped.                                                                   |
| `operator`  | string | **Yes**  | Comparison applied between the reading and `threshold`. One of `>`, `>=`, `<`, `<=`, `==`, `!=` (aliases: `gt`, `gte`, `lt`, `lte`, `eq`, `ne`).             |
| `threshold` | number | **Yes**  | The value the reading is compared against.                                                                                                                   |
| `on_trigger` | array  | No       | Actions to fire when the rule triggers (and on each cooldown window). Each is `{ "resource": <name>, "command": <DoCommand payload>, "capture": <name?> }`. |
| `on_resolve` | array  | No       | Actions to fire when the rule clears. Same shape as `on_trigger`.                                                                                            |

### Example Configuration

A low-bean alert that posts to Slack on trigger and adds a ✅ reaction to that same message on resolve, plus a fan toggled directly:

```json
{
  "sensor": "usage-sensor",
  "poll_interval_seconds": 30,
  "cooldown_seconds": 3600,
  "rules": [
    {
      "key": "regular_grinds",
      "operator": ">=",
      "threshold": 20,
      "on_trigger": [
        {
          "resource": "slack-notifier",
          "command": { "command": "send", "text": "☕ Regular beans are low (${value}), please refill." },
          "capture": "msg"
        }
      ],
      "on_resolve": [
        {
          "resource": "slack-notifier",
          "command": { "command": "react", "name": "white_check_mark", "ts": "${msg.ts}", "channel": "${msg.channel}" }
        }
      ]
    },
    {
      "key": "temperature",
      "operator": ">",
      "threshold": 90,
      "on_trigger": [{ "resource": "cooling-fan", "command": { "set": { "power": 1 } } }],
      "on_resolve": [{ "resource": "cooling-fan", "command": { "set": { "power": 0 } } }]
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

**`check`** - Force an immediate poll of the sensor (instead of waiting for the next interval), evaluating all rules and firing any actions.

```json
{"check": true}
```

Returns:

```json
{"check": "ok"}
```
