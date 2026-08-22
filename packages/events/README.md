# ms-events

Общий пакет микросервисов: контракт событий (`EventEnvelope`), справочник топиков
и типов событий, единая настройка структурированного логирования.

Подключается как зависимость через uv workspace:

```toml
dependencies = ["ms-events"]

[tool.uv.sources]
ms-events = { workspace = true }
```
