---
name: weather
description: Get current weather and forecasts (no API key required).
homepage: https://open-meteo.com/en/docs
metadata: {"nanobot":{"emoji":"🌤️","requires":{"bins":["curl"]}}}
---

# Weather

Two free services, no API keys needed.

## Open-Meteo (primary, recommended)

Free, no key, reliable and works globally. Returns JSON.

**Current weather:**
```bash
curl -s -m 10 "https://api.open-meteo.com/v1/forecast?latitude=39.9&longitude=116.4&current_weather=true&timezone=auto"
```

**With daily forecast:**
```bash
curl -s -m 10 "https://api.open-meteo.com/v1/forecast?latitude=39.9&longitude=116.4&current_weather=true&daily=temperature_2m_max,temperature_2m_min,weathercode&timezone=auto"
```

Common city coordinates:
- Beijing: 39.9, 116.4
- Shanghai: 31.2, 121.5
- Guangzhou: 23.1, 113.3
- Shenzhen: 22.5, 114.1
- Hong Kong: 22.3, 114.2
- Tokyo: 35.7, 139.7
- London: 51.5, -0.12
- New York: 40.7, -74.0

Weather codes: 0=Clear, 1-3=Cloudy, 45-48=Fog, 51-55=Drizzle, 61-65=Rain, 71-77=Snow, 80-82=Showers, 95-99=Thunderstorm

Docs: https://open-meteo.com/en/docs

## wttr.in (fallback, may timeout in some regions)

Quick one-liner (add `-m 5` for 5s timeout):
```bash
curl -s -m 5 "wttr.in/London?format=3"
# Output: London: ⛅️ +8°C
```

Compact format:
```bash
curl -s -m 5 "wttr.in/London?format=%l:+%c+%t+%h+%w"
# Output: London: ⛅️ +8°C 71% ↙5km/h
```

Full forecast:
```bash
curl -s -m 5 "wttr.in/London?T"
```

Format codes: `%c` condition · `%t` temp · `%h` humidity · `%w` wind · `%l` location · `%m` moon

Tips:
- URL-encode spaces: `wttr.in/New+York`
- Airport codes: `wttr.in/JFK`
- Units: `?m` (metric) `?u` (USCS)
- Today only: `?1` · Current only: `?0`
- PNG: `curl -s "wttr.in/Berlin.png" -o /tmp/weather.png`
