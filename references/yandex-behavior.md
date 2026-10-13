# Публичный API Яндекс.Диска и наблюдаемое поведение

Всё ниже снято с реальных ответов (октябрь 2026), не из документации. Токен для публичных
ссылок не нужен.

## Содержание
1. Эндпоинты
2. Форма ответа
3. Цепочка скачивания и Range
4. Превью
5. Коды и ошибки
6. Замеры
7. Быстрые проверки

## 1. Эндпоинты

| Что | Запрос |
|---|---|
| Метаданные / листинг | `GET https://cloud-api.yandex.net/v1/disk/public/resources?public_key=<URL ссылки>&path=<путь внутри>&limit=200&offset=0` |
| Ссылка на скачивание | `GET https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key=<URL>&path=<путь>` → `{"href": "https://downloader.disk.yandex.ru/disk/..."}` |

- `public_key` — полный URL ссылки как есть; работают `disk.yandex.ru/d/…`, `disk.360.yandex.ru/d/…`, `yadi.sk/d/…`.
- `path` опускается для корня ссылки и для ссылки на одиночный файл.
- Листинг с большим `limit` медленный (≈10 с на 250 элементов — Яндекс генерирует ссылки на каждый элемент); `limit=200` с пагинацией по `offset` и `_embedded.total` — нормально.

## 2. Форма ответа

Папка: `{"type":"dir","name":...,"path":"/...","resource_id":"...","_embedded":{"items":[...],"total":N,"limit":..,"offset":..}}`.

Элемент-файл (поля, на которые опирается утилита):

```json
{"type":"file","name":"IMG_0042.jpg","path":"/Отпуск 2021/IMG_0042.jpg",
 "size":3145728,"md5":"<32 hex>","sha256":"<64 hex>","media_type":"image",
 "antivirus_status":"clean|not-scanned|infected","mime_type":"image/jpeg",
 "file":"https://downloader.disk.yandex.ru/disk/...","preview":"https://downloader.disk.yandex.ru/preview/...",
 "sizes":[{"name":"ORIGINAL","url":".../disk/..."},{"name":"XXXL","url":".../preview/...&size=XXXL"}, ...],
 "resource_id":"<uid>:<hash>"}
```

- `media_type`: `image`, `video`, `document`, `compressed`, … — превью имеет смысл только для `image`.
- `resource_id` одинаков у двух разных ссылок на одну и ту же папку → способ найти дубли ссылок.
- `sizes.ORIGINAL` ведёт на тот же узел хранилища, что и `/download` — не обходной путь.

## 3. Цепочка скачивания и Range

`/download` → `href` на `downloader.disk.yandex.ru` → **302** на конкретный узел
`sNNN<суффикс>.storage.yandex.net` (суффиксы: `klg`, `nrg`, `iva`, …) → тело файла.

- Узел поддерживает `Range`: `206 Partial Content`, `Content-Range: bytes a-b/total`. На этом
  держатся докачка и загрузка кусками.
- Чтобы узнать узел заранее — запрос к `href` без следования редиректу, взять `Location`.
- Для одного файла Яндекс стабильно отдаёт один и тот же узел; повторный `/download` реплику
  не меняет.

## 4. Превью

- Отдаёт `downloader.disk.yandex.ru/preview/...` напрямую (без узла хранилища) — живёт, даже
  когда оригинал недоступен.
- Размеры: XXXS…XXXL, DEFAULT, C. **Максимум 1280 px (XXXL)**; параметр `size=4000x4000` и т.п.
  возвращает те же 1280 px, если оригинала нет. Если сам оригинал меньше — превью его размера.
- **HTTP 434** на все размеры — превью для файла нет (в сетке Диска вместо миниатюры
  значок-заглушка). Сохранить нечего. **HTTP 422** на DEFAULT. **HTTP 500** — временная ошибка сервиса превью, иногда
  проходит при повторе.
- Для PNG-оригиналов превью приходит в PNG (сигнатура `\x89PNG`), для остальных — JPEG (`\xff\xd8`).

## 5. Коды и ошибки

| Симптом | Что значит |
|---|---|
| Узел принял TCP+TLS, но заголовков нет (ждали 40 с+) | Этот файл на узле недоступен. Узел при этом может отдавать другие файлы. → `NO_RESPONSE` |
| Поток обрывается на N байт; Range с N отдаёт 0 байт | Остаток файла сейчас не отдаётся → `PARTIAL`, начало сохраняется в `.part` |
| Поток обрывается, но Range с места обрыва даёт данные | Обычный сетевой обрыв → докачать |
| `206` на `Range: bytes=0-0`, но тело пустое — `http.client.IncompleteRead(0 bytes read, 1 more expected)` | Узел отдаёт заголовки, но не данные: файл временно недоступен → `NO_DATA`. Это **не** `OSError` — ловить `http.client.HTTPException` отдельно, иначе поток куска падает молча |
| `API 429 DiskResourceDownloadLimitExceededError` на `/download` | Лимит скачиваний **конкретного** публичного файла (часто `.exe` и популярное), не общий лимит API — повторить позже |
| `404 DiskNotFoundError` на корень ссылки | Ссылка не опубликована / перевыпущена / доступ ограничен. Веб-страница: `<title>Ничего не найдено — Яндекс Диск</title>` |
| Веб-страница ссылки отдаёт 403 | Антибот Яндекса на страницу, не на API |
| `API 429` | Лимит API (в нашем прогоне на 64 потоках не встречался ни разу) |
| `HTTP 500` от downloader | Временная ошибка сервиса — в список, потом автоповтор |

## 6. Замеры (через зарубежный VPN, 64 потока)

- Заголовки от живого узла: **0.38–0.52 с** (14 из 14 проб). Отсюда таймаут 5 с.
- Поток с живого узла: ~450 КБ/с на соединение; 194 МБ кусками ×8 — 12 с (~16 МБ/с).
- Попытки по неотвечающим файлам за 30 мин: успех с 1-й — 274, со 2-й — 6, с 3-й — 0, не скачано после 3 — 451.
- Доля временно недоступных в фотопапках в нашем прогоне: 65–70%; 99% из них — на узлах с суффиксом `klg`.
- Пропускная способность на мелких файлах (~1 МБ): ~600 файлов/мин при 2–4 МБ/с.

## 7. Быстрые проверки

```bash
# жива ли ссылка и что внутри
curl -s "https://cloud-api.yandex.net/v1/disk/public/resources?public_key=https://disk.yandex.ru/d/XXXX&limit=5" \
 | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("error") or (d["name"], d["type"], d.get("_embedded",{}).get("total")))'

# куда редиректит и отвечает ли узел (заголовки за 5 с?)
H=$(curl -s "https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key=<URL>&path=<путь>" | python3 -c 'import json,sys;print(json.load(sys.stdin)["href"])')
curl -s -o /dev/null -D - "$H" | grep -i '^location'
curl -s -L -m 10 -r 0-0 -o /dev/null -w 'code %{http_code} ttfb %{time_starttransfer}s\n' "$H"
```
