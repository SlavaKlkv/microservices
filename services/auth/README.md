# auth

Пользователи и JWT. В саге не участвует: события не публикует и не
слушает — выдаёт токены, по которым `orders` пускает к своему API.

## API

| Метод | Путь | |
|---|---|---|
| POST | `/api/v1/auth/register` | регистрация |
| POST | `/api/v1/auth/login` | вход через form-data (для Swagger Authorize) |
| POST | `/api/v1/auth/login_json` | вход по JSON, отдаёт пару токенов |
| POST | `/api/v1/auth/refresh` | обновить access-токен |
| POST | `/api/v1/auth/logout` | отозвать refresh-токен |
| GET | `/api/v1/auth/identity` | кто я по текущему токену |
| — | `/api/v1/users/...` | CRUD пользователей |

Плюс `/health`, `/ready` и `/metrics`.

## Настройки

`AUTH_JWT_SECRET` обязателен и в примерах заполнен плейсхолдером
`change-me` — при реальном развёртывании его нужно заменить.

`orders` ходит сюда по `AUTH_SERVICE_URL` для проверки токена.
