#!/usr/bin/env python3
"""Яндекс.Диск → NAS/диск: локальная качалка публичных ссылок с веб-интерфейсом.

Запуск — из терминальной панели Claude или Terminal.app (нужен доступ к сетевым томам):
  caffeinate -i python3 app.py          (куда качать — выбирается в интерфейсе)
Интерфейс: http://localhost:8765
"""
import collections
import errno
import hashlib
import http.client
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(HERE, "state.db"))
# Место назначения выбирается в интерфейсе на каждую ссылку (NAS, внешний диск, этот компьютер);
# NAS_ROOT — только подсказка по умолчанию.
NAS_ROOT = os.environ.get("NAS_ROOT")
DEFAULT_SUBDIR = "YandexBackup"
NETWORK_FS = {"smbfs", "afpfs", "nfs", "nfs4", "cifs", "smb3", "webdav", "fuse.sshfs", "sshfs", "9p"}
MOUNT_PREFIXES = ("/Volumes/", "/media/", "/run/media/", "/mnt/")
PORT = int(os.environ.get("PORT", "8765"))
API = "https://cloud-api.yandex.net/v1/disk/public/resources"
UA = {"User-Agent": "yadisk-backup/1.0"}
CHUNK = 256 * 1024
# Живые узлы отдают заголовки за 0.4–0.5 с (замер); битый файл не отвечает вовсе.
HEADERS_TIMEOUT = int(os.environ.get("HEADERS_TIMEOUT", "5"))
STREAM_TIMEOUT = 30         # пауза посреди потока — обрыв, следующая попытка докачает с места
MAX_ATTEMPTS = 3
MAX_WORKERS = 64
BIG = 200 * 1024 * 1024     # файлы крупнее качаем после мелочи
SEG_MIN = 32 * 1024 * 1024  # файлы крупнее качаем кусками в несколько соединений (Range)
SEG_COUNT = 8
SEG_SLOTS = threading.BoundedSemaphore(48)  # общий лимит соединений под куски на все файлы
# smbfs на Mac однажды намертво завис при ~100 параллельных записях + сетевом сбое — держим нагрузку умеренной
NAS_ERRNOS = {errno.EACCES, errno.EPERM, errno.EROFS, errno.ENOSPC, errno.EIO, errno.ESTALE,
              errno.ENOTCONN, errno.EDQUOT, errno.ENOENT}
PREVIEW_SIZES = ("XXXL", "XXL", "XL", "L")  # 1280 px максимум; меньшие — если крупное не закэшировано
PREVIEW_NONE, PREVIEW_SAVED, PREVIEW_NA, PREVIEW_BUSY = 0, 1, 2, 9

settings = {"workers": 4, "paused": False}
live = {}                   # file_id → прогресс активной загрузки
live_lock = threading.Lock()
counter = {"bytes": 0, "speed": 0.0}

DB = sqlite3.connect(DB_PATH, check_same_thread=False)
DB.row_factory = sqlite3.Row
DBL = threading.RLock()


def q(sql, *args):
    with DBL:
        return [dict(r) for r in DB.execute(sql, args).fetchall()]


def x(sql, *args):
    with DBL:
        cur = DB.execute(sql, args)
        DB.commit()
        return cur.lastrowid


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def human(n):
    n = float(n or 0)
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} ПБ"


def init_db():
    with DBL:
        DB.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS jobs(
                id INTEGER PRIMARY KEY, url TEXT UNIQUE, name TEXT, base TEXT,
                single INTEGER DEFAULT 0, walk_status TEXT DEFAULT 'pending',
                walk_dirs INTEGER DEFAULT 0, paused INTEGER DEFAULT 0, error TEXT, created REAL);
            CREATE TABLE IF NOT EXISTS files(
                id INTEGER PRIMARY KEY, job_id INTEGER, rel TEXT, path TEXT, size INTEGER, md5 TEXT,
                status TEXT DEFAULT 'queued', reason TEXT, attempts INTEGER DEFAULT 0, updated REAL,
                UNIQUE(job_id, rel));
            CREATE TABLE IF NOT EXISTS problems(
                id INTEGER PRIMARY KEY, job_id INTEGER, kind TEXT, rel TEXT, path TEXT,
                size INTEGER, reason TEXT, ts REAL, UNIQUE(job_id, kind, rel));
            CREATE INDEX IF NOT EXISTS files_status ON files(status);
            CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
            CREATE TABLE IF NOT EXISTS excludes(job_id INTEGER, path TEXT, PRIMARY KEY(job_id, path));
        """)
        row = DB.execute("SELECT v FROM kv WHERE k='workers'").fetchone()
        if row:
            settings["workers"] = int(row["v"])
        if "preview" not in {r[1] for r in DB.execute("PRAGMA table_info(files)")}:
            DB.execute("ALTER TABLE files ADD COLUMN preview INTEGER DEFAULT 0")
        if "priority" not in {r[1] for r in DB.execute("PRAGMA table_info(jobs)")}:
            DB.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER DEFAULT 0")
        DB.execute("UPDATE files SET status='queued' WHERE status='active'")
        DB.execute("UPDATE files SET preview=0 WHERE preview=?", (PREVIEW_BUSY,))
        DB.commit()


# ---------- Яндекс API ----------

class ApiError(Exception):
    pass


class NasError(Exception):
    """Запись на NAS не идёт — это не битый файл Яндекса, а повод встать на паузу."""


def is_nas_failure(e):
    return (isinstance(e, OSError) and not isinstance(e, urllib.error.URLError)
            and not isinstance(e, socket.timeout) and e.errno in NAS_ERRNOS)


def pause_for_nas(e):
    if not settings["paused"]:
        settings["paused"] = True
        log(f"Место назначения не принимает запись ({e!r}) — ПАУЗА. Подключи диск/шару и нажми «Продолжить»")


def api_get(endpoint, **params):
    url = endpoint + "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    for attempt in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")[:200]
            if e.code in (429, 500, 502, 503, 504) and attempt < 4:
                time.sleep(3 * 2 ** attempt)
                continue
            raise ApiError(f"API {e.code}: {body}")
        except OSError as e:
            if attempt < 4:
                time.sleep(3 * 2 ** attempt)
                continue
            raise ApiError(f"API недоступен: {e}")


BAD_CHARS = re.compile(r'[<>:"\\|?*\x00-\x1f]')


def safe_rel(rel):
    """Имена с символами, которые SMB не принимает, приводим к безопасным."""
    return "/".join(BAD_CHARS.sub("_", p).rstrip(" .") or "_" for p in rel.strip("/").split("/") if p)


def add_problem(job_id, kind, rel, path, size, reason):
    x("INSERT OR IGNORE INTO problems(job_id,kind,rel,path,size,reason,ts) VALUES(?,?,?,?,?,?,?)",
      job_id, kind, rel, path, size, reason, time.time())


def add_files(job_id, items, single=False):
    rows = []
    for it in items:
        rel = it["name"] if single else it["path"].lstrip("/")
        size = it.get("size", 0)
        rows.append((job_id, rel, None if single else it["path"], size, it.get("md5"), time.time()))
        if size == 0:
            add_problem(job_id, "EMPTY_SOURCE", rel, it.get("path"), 0, "На Диске файл размером 0 байт")
        if it.get("antivirus_status") == "infected":
            add_problem(job_id, "INFECTED", rel, it.get("path"), size, "Антивирус Яндекса пометил как заражённый")
    with DBL:
        DB.executemany("INSERT OR IGNORE INTO files(job_id,rel,path,size,md5,updated) VALUES(?,?,?,?,?,?)", rows)
        DB.commit()


def excluded_paths(job_id):
    return [r["path"] for r in q("SELECT path FROM excludes WHERE job_id=?", job_id)]


def under(path, prefix):
    return path == prefix or path.startswith(prefix + "/")


def walk(job_id, start_paths=None):
    """Обход папок в ширину: файлы корня попадают в очередь первыми."""
    url = q("SELECT url FROM jobs WHERE id=?", job_id)[0]["url"]
    x("UPDATE jobs SET walk_status='walking', error=NULL WHERE id=?", job_id)
    queue = collections.deque(start_paths or [None])
    try:
        while queue:
            path = queue.popleft()
            if path and any(under(path, e) for e in excluded_paths(job_id)):
                continue  # папку исключили — внутрь не ходим
            offset = 0
            while True:
                try:
                    meta = api_get(API, public_key=url, path=path, limit=200, offset=offset)
                except ApiError as e:
                    rel = (path or "/").lstrip("/") or "/"
                    add_problem(job_id, "LIST_FAIL", rel, path, 0, str(e))
                    log(f"папка не читается: {rel} ← {e}")
                    break
                if path is None and offset == 0:
                    name = meta["name"]
                    with DBL:  # проверка и запись имени атомарно: обходы ссылок идут параллельно
                        # две ссылки с одинаковым именем папки не должны писать в одно место
                        if q("SELECT 1 FROM jobs WHERE id != ? AND name = ?"
                             " AND base = (SELECT base FROM jobs WHERE id = ?)", job_id, name, job_id):
                            name = f"{name} ({job_id})"
                        # COALESCE: при повторном обходе имя (и папка на NAS) не меняется
                        x("UPDATE jobs SET name=COALESCE(name, ?), single=? WHERE id=?",
                          name, int(meta["type"] == "file"), job_id)
                if meta["type"] == "file":
                    add_files(job_id, [meta], single=True)
                    break
                emb = meta["_embedded"]
                items = emb["items"]
                queue.extend(it["path"] for it in items if it["type"] == "dir")
                add_files(job_id, [it for it in items if it["type"] == "file"])
                offset += len(items)
                if offset >= emb["total"] or not items:
                    break
            x("UPDATE jobs SET walk_dirs=walk_dirs+1 WHERE id=?", job_id)
        x("UPDATE jobs SET walk_status='done' WHERE id=?", job_id)
        log(f"обход ссылки #{job_id} завершён")
    except Exception as e:
        x("UPDATE jobs SET walk_status='failed', error=? WHERE id=?", repr(e), job_id)
        log(f"обход ссылки #{job_id} упал: {e!r}")


def start_walk(job_id, paths=None):
    threading.Thread(target=walk, args=(job_id, paths), daemon=True).start()


# ---------- Скачивание ----------

def claim():
    """Сначала превью для битых (мелкие и быстрые), потом оригиналы из очереди."""
    with DBL:
        row = DB.execute("""
            SELECT f.*, j.url, j.name, j.base, j.single FROM files f JOIN jobs j ON j.id = f.job_id
            WHERE f.status = 'failed' AND f.preview = ? AND j.paused = 0
            ORDER BY j.priority DESC, f.id LIMIT 1""", (PREVIEW_NONE,)).fetchone()
        if row:
            DB.execute("UPDATE files SET preview=? WHERE id=?", (PREVIEW_BUSY, row["id"]))
            DB.commit()
            return dict(row, mode="preview")
        row = DB.execute("""
            SELECT f.*, j.url, j.name, j.base, j.single FROM files f JOIN jobs j ON j.id = f.job_id
            WHERE f.status = 'queued' AND j.paused = 0
            ORDER BY j.priority DESC, (f.size > ?), f.job_id, f.id LIMIT 1""", (BIG,)).fetchone()
        if not row:
            return None
        DB.execute("UPDATE files SET status='active', updated=? WHERE id=?", (time.time(), row["id"]))
        DB.commit()
        return dict(row, mode="original")


def target_of(f):
    base = f["base"] if f["single"] else os.path.join(f["base"], safe_rel(f["name"] or "_"))
    return os.path.join(base, safe_rel(f["rel"]))


PREVIEW_FORMATS = {b"\xff\xd8": ".jpg", b"\x89PNG": ".png"}  # для PNG Яндекс отдаёт превью в PNG


def preview_of(f, ext=".jpg"):
    return target_of(f) + ".preview" + ext


def save_preview(f):
    """Оригинал не отдаётся (узел хранилища мёртв), но превью раздаёт отдельный живой сервис."""
    meta = api_get(API, public_key=f["url"], path=f["path"])
    if meta.get("media_type") != "image":
        return PREVIEW_NA, f"не фото ({meta.get('media_type')}) — превью бесполезно"
    urls = {s["name"]: s["url"] for s in meta.get("sizes", [])}
    data, size_name, last_err = None, None, "у файла нет превью"
    for size_name in (s for s in PREVIEW_SIZES if s in urls):
        try:
            with urllib.request.urlopen(urllib.request.Request(urls[size_name], headers=UA), timeout=60) as r:
                data = r.read()
            break
        except urllib.error.HTTPError as e:
            # 434 — превью так и не сгенерировали, а оригинал мёртв: взять не из чего
            last_err = f"превью тоже недоступно (HTTP {e.code})"
    if data is None:
        return PREVIEW_NA, last_err
    ext = next((e for magic, e in PREVIEW_FORMATS.items() if data.startswith(magic)), None)
    if len(data) < 1024 or not ext:
        return PREVIEW_NA, f"превью не картинка ({len(data)} Б)"
    target = preview_of(f, ext)
    os.makedirs(os.path.dirname(target), exist_ok=True)
    with open(target + ".part", "wb") as out:
        out.write(data)
    os.replace(target + ".part", target)
    return PREVIEW_SAVED, f"{size_name}, {human(len(data))}"


def is_timeout(e):
    return isinstance(e, socket.timeout) or isinstance(getattr(e, "reason", None), socket.timeout)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


NO_REDIRECT = urllib.request.build_opener(_NoRedirect)


def resolve(href):
    """Ссылка downloader.disk.yandex.ru редиректит на конкретный узел sXXX.storage.yandex.net."""
    try:
        with NO_REDIRECT.open(urllib.request.Request(href, headers=UA), timeout=30):
            return href
    except urllib.error.HTTPError as e:
        if e.code in (301, 302, 303, 307, 308) and e.headers.get("Location"):
            return e.headers["Location"]
        raise


def set_stream_timeout(r, seconds):
    """Заголовки ждём коротко, а пошедший поток — дольше: живой узел иногда притормаживает."""
    try:
        r.fp.raw._sock.settimeout(seconds)
    except AttributeError:
        pass


def fetch(f, part, entry):
    h = hashlib.md5()
    have = os.path.getsize(part) if os.path.exists(part) else 0
    if have:
        with open(part, "rb") as fh:
            for chunk in iter(lambda: fh.read(4 * CHUNK), b""):
                h.update(chunk)
    if have and have == entry["size"]:
        entry["done"] = have
        return have, h.hexdigest()
    href = resolve(api_get(API + "/download", public_key=f["url"], path=f["path"])["href"])
    entry["host"] = urllib.parse.urlparse(href).hostname
    headers = dict(UA)
    if have:
        headers["Range"] = f"bytes={have}-"
    entry["stage"] = "headers"
    with urllib.request.urlopen(urllib.request.Request(href, headers=headers), timeout=HEADERS_TIMEOUT) as r:
        entry["stage"] = "stream"
        set_stream_timeout(r, STREAM_TIMEOUT)
        if have and r.status != 206:
            have, h = 0, hashlib.md5()
        entry["done"] = have
        last_t, last_b = time.time(), have
        with open(part, "ab" if have else "wb") as out:
            while True:
                chunk = r.read(CHUNK)
                if not chunk:
                    break
                out.write(chunk)
                h.update(chunk)
                entry["done"] += len(chunk)
                with live_lock:
                    counter["bytes"] += len(chunk)
                now = time.time()
                if now - last_t >= 1:
                    inst = (entry["done"] - last_b) / (now - last_t)
                    entry["speed"] = inst if not entry["speed"] else 0.7 * entry["speed"] + 0.3 * inst
                    last_t, last_b = now, entry["done"]
    return os.path.getsize(part), h.hexdigest()


def _load_plan(part, size):
    """План кусков [начало, конец, скачано] — из .part.json, либо новый от уже скачанного начала."""
    try:
        with open(part + ".json") as fh:
            plan = json.load(fh)
        if plan.get("size") == size:
            return plan["segs"]
    except (OSError, ValueError):
        pass
    have = min(os.path.getsize(part), size) if os.path.exists(part) else 0
    step = max(CHUNK, -(-(size - have) // SEG_COUNT))
    return [[s, min(size, s + step) - 1, 0] for s in range(have, size, step)]


def _save_plan(part, size, segs, lock):
    with lock:
        data = json.dumps({"size": size, "segs": segs})
        with open(part + ".json.tmp", "w") as fh:
            fh.write(data)
        os.replace(part + ".json.tmp", part + ".json")


def _fetch_segment(f, part, seg, entry, state):
    """Качает bytes [seg.start + seg.done .. seg.end] в part по своему смещению; 2 попытки без прогресса — кусок мёртв."""
    seg_len = seg[1] - seg[0] + 1
    idle = 0
    while seg[2] < seg_len:
        before = seg[2]
        try:
            with SEG_SLOTS:
                href = resolve(api_get(API + "/download", public_key=f["url"], path=f["path"])["href"])
                req = urllib.request.Request(href, headers=dict(UA, Range=f"bytes={seg[0] + seg[2]}-{seg[1]}"))
                with urllib.request.urlopen(req, timeout=HEADERS_TIMEOUT) as r:
                    if r.status != 206:
                        raise OSError(f"узел ответил {r.status} вместо 206 на Range")
                    set_stream_timeout(r, STREAM_TIMEOUT)
                    with open(part, "r+b", buffering=0) as out:  # без буфера: учтённое в плане уже на диске
                        out.seek(seg[0] + seg[2])
                        while seg[2] < seg_len:
                            chunk = r.read(CHUNK)
                            if not chunk:
                                break
                            data = chunk[:seg_len - seg[2]]
                            view = memoryview(data)
                            while view:  # небуферизованная запись может принять не всё за раз
                                view = view[out.write(view):]
                            with state["lock"]:
                                seg[2] += len(data)
                                entry["done"] += len(data)
                            with live_lock:
                                counter["bytes"] += len(data)
        # HTTPError и таймауты — тоже OSError; IncompleteRead (обрыв посреди ответа) — HTTPException
        except (ApiError, OSError, http.client.HTTPException) as e:
            if is_nas_failure(e):
                state["nas_error"] = e
                return
            state["errors"].append(f"{e}")
        if seg[2] < seg_len:
            idle = idle + 1 if seg[2] == before else 0
            if idle >= 2:
                state["dead"].append(seg)
                return
            time.sleep(2)


def download_segmented(f, target, part, entry):
    """Крупный файл — кусками в SEG_COUNT соединений. None — узел не умеет Range, качаем обычным способом."""
    size = f["size"]
    href = resolve(api_get(API + "/download", public_key=f["url"], path=f["path"])["href"])
    entry["host"] = urllib.parse.urlparse(href).hostname
    entry["stage"] = "headers"
    with urllib.request.urlopen(urllib.request.Request(href, headers=dict(UA, Range="bytes=0-0")),
                                timeout=HEADERS_TIMEOUT) as r:  # битый файл отсеется здесь за 5 с
        try:
            r.read()
        except http.client.IncompleteRead:
            return "failed", "PARTIAL: узел отдал заголовки, но ни байта данных — файл у Яндекса потерян"
        if r.status != 206:
            return None
    entry["stage"] = "stream"
    segs = _load_plan(part, size)
    state = {"lock": threading.Lock(), "dead": [], "errors": []}
    _save_plan(part, size, segs, state["lock"])  # план — до первого байта: .part с дырами без плана не бывает
    if not os.path.exists(part):
        open(part, "wb").close()
    entry["done"] = (segs[0][0] if segs else size) + sum(s[2] for s in segs)
    entry["segments"] = len(segs)
    threads = [threading.Thread(target=_fetch_segment, args=(f, part, s, entry, state), daemon=True)
               for s in segs if s[2] < s[1] - s[0] + 1]
    for t in threads:
        t.start()
    last_t, last_b, last_save = time.time(), entry["done"], time.time()
    while any(t.is_alive() for t in threads):
        time.sleep(1)
        now = time.time()
        inst = (entry["done"] - last_b) / (now - last_t)
        entry["speed"] = inst if not entry["speed"] else 0.7 * entry["speed"] + 0.3 * inst
        last_t, last_b = now, entry["done"]
        if now - last_save >= 10:
            _save_plan(part, size, segs, state["lock"])
            last_save = now
    if state.get("nas_error"):
        raise NasError(state["nas_error"])
    _save_plan(part, size, segs, state["lock"])
    unfinished = [s for s in segs if s[2] < s[1] - s[0] + 1 and s not in state["dead"]]
    if unfinished and not state["dead"]:
        # поток куска упал непредвиденно — не трогаем .part, следующая попытка докачает по плану
        return "failed", f"ERROR: {len(unfinished)} кусков не докачано ({'; '.join(state['errors'][-2:])})"
    if state["dead"]:
        lost = sum(s[1] - s[0] + 1 - s[2] for s in state["dead"])
        return "failed", (f"PARTIAL: {human(lost)} из {human(size)} не отдаётся, остальное лежит в .part"
                          f" — «Повторить битые» докачает только недостающее")
    entry["stage"] = "verify"  # md5 по всему собранному файлу — гарантия, что куски легли правильно
    h = hashlib.md5()
    with open(part, "rb") as fh:
        for chunk in iter(lambda: fh.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    if os.path.getsize(part) != size or (f["md5"] and h.hexdigest() != f["md5"]):
        os.remove(part)
        os.remove(part + ".json")
        return "failed", "MD5_MISMATCH: собранный файл не совпал с Диском, будет скачан заново"
    os.replace(part, target)
    os.remove(part + ".json")
    for ext in PREVIEW_FORMATS.values():
        if os.path.exists(preview_of(f, ext)):
            os.remove(preview_of(f, ext))
    return "ok", None


def download(f):
    target = target_of(f)
    size = f["size"] or 0
    if os.path.exists(target) and os.path.getsize(target) == size:
        return "skip", None
    os.makedirs(os.path.dirname(target), exist_ok=True)
    part = target + ".part"
    entry = {"job_id": f["job_id"], "rel": f["rel"], "size": size, "done": 0, "speed": 0.0,
             "attempt": 0, "started": time.time(), "host": None}
    with live_lock:
        live[f["id"]] = entry
    reason = None
    try:
        if size >= SEG_MIN:
            entry["attempt"] = 1
            x("UPDATE files SET attempts=attempts+1 WHERE id=?", f["id"])
            try:
                res = download_segmented(f, target, part, entry)
            except ApiError as e:
                res = ("failed", str(e))
            except OSError as e:
                if is_nas_failure(e):
                    raise NasError(e)
                res = ("failed", f"NO_RESPONSE: узел {entry['host']} молчит по этому файлу > {HEADERS_TIMEOUT} с"
                       if is_timeout(e) and entry.get("stage") == "headers"
                       else f"Сеть ({entry['host'] or 'downloader'}): {e}")
            if res is not None:
                return res
            if os.path.exists(part + ".json"):  # узел перестал понимать Range — куски с дырами не докачать потоком
                os.remove(part)
                os.remove(part + ".json")
        for attempt in range(1, MAX_ATTEMPTS + 1):
            entry.update(attempt=attempt, speed=0.0, host=None, stage=None)
            x("UPDATE files SET attempts=attempts+1 WHERE id=?", f["id"])
            pause = 5 * attempt
            had = os.path.getsize(part) if os.path.exists(part) else 0
            try:
                got, md5 = fetch(f, part, entry)
            except ApiError as e:
                reason = str(e)
            except http.client.HTTPException as e:  # IncompleteRead: обрыв посреди ответа, .part докачается
                reason = f"Сеть ({entry['host'] or 'downloader'}): {e!r}"
            except urllib.error.HTTPError as e:
                reason = f"HTTP {e.code} от узла {entry['host'] or 'downloader'}"
                if e.code == 416 and os.path.exists(part):
                    os.remove(part)
            except OSError as e:
                if is_nas_failure(e):
                    raise NasError(e)
                if is_timeout(e) and entry["stage"] == "headers":
                    # узел жив (другие файлы отдаёт), но этот файл не отдаёт — умер его диск/реплика
                    reason = f"NO_RESPONSE: узел {entry['host']} молчит по этому файлу > {HEADERS_TIMEOUT} с"
                    # повтор почти не помогает (замер: 6 из 457 со 2-й попытки, 0 с 3-й) —
                    # сразу в реестр, остаток подберёт «Повторить битые»
                    break
                elif is_timeout(e) and entry["stage"] == "stream":
                    reason = f"STALL: поток с {entry['host']} замер > {STREAM_TIMEOUT} с"  # докачаем с места
                    pause = 2
                else:
                    reason = f"Сеть ({entry['host'] or 'downloader'}): {e}"
            else:
                if got < size and got <= had:
                    # докачка не дала ни байта: хвост файла у Яндекса потерян, повторять бесполезно
                    reason = f"PARTIAL: отдаётся только {human(got)} из {human(size)}, начало лежит в .part"
                    break
                if got != size:
                    reason = f"SIZE_MISMATCH: {got} вместо {size}"
                    if got > size:
                        os.remove(part)
                elif f["md5"] and md5 != f["md5"]:
                    reason = "MD5_MISMATCH: содержимое не совпало с Диском"
                    os.remove(part)
                else:
                    os.replace(part, target)
                    for ext in PREVIEW_FORMATS.values():  # оригинал ожил — превью больше не нужно
                        if os.path.exists(preview_of(f, ext)):
                            os.remove(preview_of(f, ext))
                    return "ok", None
            log(f"попытка {attempt}/{MAX_ATTEMPTS} не удалась: {f['rel']} ← {reason}")
            if attempt < MAX_ATTEMPTS:
                time.sleep(pause)
        return "failed", reason
    finally:
        with live_lock:
            live.pop(f["id"], None)


def worker(idx):
    while True:
        if settings["paused"] or idx >= settings["workers"]:
            time.sleep(1)
            continue
        f = claim()
        if not f:
            time.sleep(2)
            continue
        if f["mode"] == "preview":
            try:
                code, note = save_preview(f)
            except Exception as e:
                if is_nas_failure(e):
                    x("UPDATE files SET preview=? WHERE id=?", PREVIEW_NONE, f["id"])
                    pause_for_nas(e)
                    continue
                code, note = PREVIEW_NA, f"не скачалось: {e!r}"
            x("UPDATE files SET preview=?, reason=COALESCE(reason, '') || ? WHERE id=?",
              code, "" if code == PREVIEW_SAVED else f" · {note}", f["id"])
            log(f"{'превью' if code == PREVIEW_SAVED else 'без превью'}  {f['rel']}  ← {note}")
            continue
        try:
            status, reason = download(f)
        except NasError as e:
            x("UPDATE files SET status='queued', updated=? WHERE id=?", time.time(), f["id"])
            pause_for_nas(e.args[0])
            continue
        except Exception as e:
            if is_nas_failure(e):  # например, makedirs/exists на отвалившейся шаре
                x("UPDATE files SET status='queued', updated=? WHERE id=?", time.time(), f["id"])
                pause_for_nas(e)
                continue
            status, reason = "failed", f"ERROR: {e!r}"
        x("UPDATE files SET status=?, reason=?, updated=? WHERE id=?", status, reason, time.time(), f["id"])
        if status != "skip":
            log(f"{status:6} {human(f['size']):>10}  {f['rel']}" + (f"  ← {reason}" if reason else ""))


_started_workers = 0
_workers_lock = threading.Lock()


def ensure_workers(n):
    """Потоки только добавляем; лишние сами простаивают, пока idx >= settings['workers']."""
    global _started_workers
    with _workers_lock:
        while _started_workers < n:
            threading.Thread(target=worker, args=(_started_workers,), daemon=True).start()
            _started_workers += 1


def sampler():
    prev, prev_t = 0, time.time()
    while True:
        time.sleep(2)
        now = time.time()
        with live_lock:
            cur = counter["bytes"]
        inst = (cur - prev) / (now - prev_t)
        counter["speed"] = 0.6 * counter["speed"] + 0.4 * inst
        prev, prev_t = cur, now


# ---------- Реестр битых ----------

def broken_rows(limit=None):
    sql = """
        SELECT j.name AS job, j.url, 'FILE' AS kind, f.rel, f.size, f.reason, f.updated AS ts, f.job_id,
               f.preview = 1 AS preview
        FROM files f JOIN jobs j ON j.id = f.job_id WHERE f.status = 'failed'
        UNION ALL
        SELECT j.name, j.url, p.kind, p.rel, p.size, p.reason, p.ts, p.job_id, 0
        FROM problems p JOIN jobs j ON j.id = p.job_id
        ORDER BY ts DESC"""
    return q(sql + (f" LIMIT {int(limit)}" if limit else ""))


def broken_tsv():
    lines = ["время\tссылка\tпапка\tтип\tпуть\tразмер\tпричина\tпревью"]
    for r in broken_rows():
        cells = [time.strftime("%Y-%m-%d %H:%M", time.localtime(r["ts"] or 0)), r["url"], r["job"] or "",
                 r["kind"], r["rel"], str(r["size"] or 0), r["reason"] or "", "да" if r["preview"] else ""]
        lines.append("\t".join(c.replace("\t", " ").replace("\n", " ") for c in cells))
    return "\n".join(lines) + "\n"


def exporter():
    """Раз в 30 с кладёт реестр битых в каждое место назначения: <место>/_broken.tsv."""
    last = None
    while True:
        time.sleep(30)
        data = broken_tsv()
        if data == last:
            continue
        for out_dir in {r["base"] for r in q("SELECT DISTINCT base FROM jobs")}:
            try:
                if os.path.isdir(out_dir):
                    tmp = os.path.join(out_dir, "._broken.tsv.tmp")
                    with open(tmp, "w", encoding="utf-8") as fh:
                        fh.write(data)
                    os.replace(tmp, os.path.join(out_dir, "_broken.tsv"))
            except Exception as e:
                log(f"не смог выгрузить реестр в {out_dir}: {e!r}")
        last = data


# ---------- Места назначения ----------

_probes = {}  # путь → {"ts", "busy", "started", "result"}
_probes_lock = threading.Lock()


def _probe_worker(path):
    try:
        existing = path
        while not os.path.exists(existing):  # папки ещё нет — смотрим на ближайшего существующего родителя
            existing = os.path.dirname(existing.rstrip("/")) or "/"
        usage = shutil.disk_usage(existing)
        writable = os.access(existing, os.W_OK)
        res = {"ok": writable, "free": usage.free, "total": usage.total,
               "error": None if writable else "нет прав на запись"}
    except OSError as e:
        res = {"ok": False, "free": None, "total": None, "error": str(e)}
    with _probes_lock:
        _probes[path].update(result=res, ts=time.time(), busy=False)


def probe_dir(path, max_age=10, wait=1.5):
    """Свободно ли и пишется ли. Проверка — в отдельном потоке: отвалившаяся SMB-шара вешает stat
    намертво, и тогда зависает только этот поток, а мы честно отвечаем «не отвечает»."""
    with _probes_lock:
        p = _probes.setdefault(path, {"ts": 0, "busy": False, "started": 0, "result": None})
        if not p["busy"] and time.time() - p["ts"] > max_age:
            p.update(busy=True, started=time.time())
            threading.Thread(target=_probe_worker, args=(path,), daemon=True).start()
    end = time.time() + wait
    while time.time() < end:
        with _probes_lock:
            if not _probes[path]["busy"]:
                break
        time.sleep(0.05)
    with _probes_lock:
        p = dict(_probes[path])
    if p["busy"] and time.time() - p["started"] > 5:
        return {"ok": False, "free": None, "total": None, "error": "не отвечает — том отвалился?"}
    return p["result"] or {"ok": False, "free": None, "total": None, "error": "проверяю…"}


def mount_types():
    """Точка монтирования → тип ФС. Читаем таблицу ядра через `mount` — сами тома при этом не трогаем."""
    types = {}
    try:
        out = subprocess.run(["mount"], capture_output=True, text=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return types
    for line in out.splitlines():
        m = re.match(r"^.+? on (.+?) type (\S+)", line) or re.match(r"^.+? on (.+?) \(([^,)]+)", line)  # Linux | macOS
        if m:
            types[m[1]] = m[2]
    return types


def list_destinations():
    """Куда можно качать: сетевые тома, внешние диски, этот компьютер — со свободным местом."""
    dests = []
    for mount, fstype in mount_types().items():
        name = os.path.basename(mount.rstrip("/"))
        if not mount.startswith(MOUNT_PREFIXES) or name.startswith(("com.apple", ".timemachine")) or name == "Recovery":
            continue
        kind = "network" if fstype in NETWORK_FS else "external"
        dests.append({"path": mount, "name": name, "kind": kind, "fstype": fstype,
                      "suggested": os.path.join(mount, DEFAULT_SUBDIR), **probe_dir(mount)})
    home = os.path.expanduser("~")
    # не ~/Downloads и не ~/Documents: на macOS они под защитой приватности, как и сетевые тома
    dests.append({"path": home, "name": "домашняя папка", "kind": "local", "fstype": "",
                  "suggested": os.path.join(home, DEFAULT_SUBDIR), **probe_dir(home)})
    order = {"network": 0, "external": 1, "local": 2}
    return sorted(dests, key=lambda d: (order[d["kind"]], d["name"]))


def default_dest():
    row = q("SELECT v FROM kv WHERE k='last_dest'")
    if row:
        return row[0]["v"]
    if NAS_ROOT:
        return os.path.join(NAS_ROOT, DEFAULT_SUBDIR)
    return None  # интерфейс подставит первый найденный том или этот компьютер


# ---------- Состояние и действия ----------


def state():
    agg = q("SELECT job_id, status, COUNT(*) n, COALESCE(SUM(size), 0) b FROM files GROUP BY job_id, status")
    probs = {r["job_id"]: r["n"] for r in q("SELECT job_id, COUNT(*) n FROM problems GROUP BY job_id")}
    previews = {r["job_id"]: r["n"] for r in q(
        "SELECT job_id, COUNT(*) n FROM files WHERE status='failed' AND preview=? GROUP BY job_id", PREVIEW_SAVED)}
    jobs = q("SELECT * FROM jobs ORDER BY priority DESC, id")
    with live_lock:
        active = [dict(v, id=k) for k, v in live.items()]
    live_done = collections.Counter()
    for a in active:
        live_done[a["job_id"]] += a["done"]
    left = 0
    for j in jobs:
        s = {r["status"]: r for r in agg if r["job_id"] == j["id"]}
        n = lambda st: s.get(st, {}).get("n", 0)
        b = lambda st: s.get(st, {}).get("b", 0)
        counted = [r for st, r in s.items() if st != "excluded"]
        j.update(
            dest=os.path.join(j["base"], "" if j["single"] else (j["name"] or "…")),
            files_total=sum(r["n"] for r in counted), files_ok=n("ok") + n("skip"),
            files_excluded=n("excluded"), bytes_excluded=b("excluded"), dirs=top_dirs(j["id"]),
            files_failed=n("failed"), files_left=n("queued") + n("active"), problems=probs.get(j["id"], 0),
            files_preview=previews.get(j["id"], 0),
            bytes_total=sum(r["b"] for r in counted), bytes_done=b("ok") + b("skip") + live_done[j["id"]],
            bytes_failed=b("failed"),
            bytes_left=max(0, b("queued") + b("active") - live_done[j["id"]]),
        )
        left += j["bytes_left"]
    # места назначения ссылок, где ещё есть что качать: доступны ли и хватит ли места
    dests = {}
    for j in jobs:
        if j["files_left"]:
            d = dests.setdefault(j["base"], dict(probe_dir(j["base"]), path=j["base"], need=0))
            d["need"] += j["bytes_left"]
    for j in jobs:
        d = dests.get(j["base"]) or {}
        j.update(dest_ok=d.get("ok", True), dest_free=d.get("free"), dest_error=d.get("error"),
                 dest_short=bool(d.get("free") is not None and d["need"] > d["free"]))
    speed = counter["speed"]
    return {
        "jobs": jobs,
        "active": sorted(active, key=lambda a: a["started"]),
        "speed": speed,
        "eta": left / speed if speed > 1024 else None,
        "settings": settings,
        "dests": list(dests.values()),
        "broken": broken_rows(300),
        "broken_total": q("SELECT (SELECT COUNT(*) FROM files WHERE status='failed')"
                          " + (SELECT COUNT(*) FROM problems) n")[0]["n"],
    }


LINK = re.compile(r"(?:https?://)?(?:disk\.yandex\.[a-z]+|disk\.360\.yandex\.[a-z]+|yadi\.sk)/[^\s<>\"']+")


def add_jobs(text, dest=None, priority=False, subdir=None):
    """dest — абсолютный путь, куда класть (выбор пользователя); subdir — старый вариант API относительно NAS_ROOT."""
    if not dest:
        root = NAS_ROOT or os.path.expanduser("~")
        dest = os.path.join(root, safe_rel(subdir or "") or DEFAULT_SUBDIR)
    base = os.path.normpath(os.path.expanduser(dest.strip()))
    if not os.path.isabs(base):
        return {"added": [], "error": f"Нужен полный путь к папке, а не «{dest}»"}
    try:
        os.makedirs(base, exist_ok=True)
        probe = os.path.join(base, ".write_test")
        with open(probe, "w") as fh:
            fh.write("ok")
        os.remove(probe)
    except OSError as e:
        return {"added": [], "error": f"Не могу писать в {base}: {e.strerror or e}"}
    x("INSERT OR REPLACE INTO kv(k, v) VALUES('last_dest', ?)", base)
    added = []
    for raw in LINK.findall(text or ""):
        url = raw.rstrip(".,;)»")
        if not url.startswith("http"):
            url = "https://" + url
        existing = q("SELECT id FROM jobs WHERE url=?", url)
        if existing:
            if priority:
                x("UPDATE jobs SET priority=1 WHERE id=?", existing[0]["id"])
            added.append(existing[0]["id"])
            continue
        jid = x("INSERT INTO jobs(url, base, priority, created) VALUES(?,?,?,?)", url, base, int(priority), time.time())
        log(f"новая ссылка #{jid}{' ★' if priority else ''}: {url} → {base}")
        start_walk(jid)
        added.append(jid)
    return {"added": added}


def set_excluded(job_id, path, exclude):
    path = "/" + (path or "").strip("/")
    if path == "/":
        return {"ok": False}
    like = path.replace("%", r"\%").replace("_", r"\_") + "/%"
    if exclude:
        x("INSERT OR IGNORE INTO excludes(job_id, path) VALUES(?,?)", job_id, path)
        x("UPDATE files SET status='excluded' WHERE job_id=? AND status IN ('queued','failed')"
          " AND (path=? OR path LIKE ? ESCAPE '\\')", job_id, path, like)
        log(f"ссылка #{job_id}: папка {path} исключена")
    else:
        x("DELETE FROM excludes WHERE job_id=? AND path=?", job_id, path)
        x("UPDATE files SET status='queued', attempts=0, reason=NULL WHERE job_id=? AND status='excluded'"
          " AND (path=? OR path LIKE ? ESCAPE '\\')", job_id, path, like)
        start_walk(job_id, [path])  # дообойти то, что пропустили, пока папка была исключена
        log(f"ссылка #{job_id}: папка {path} возвращена в очередь")
    return {"ok": True}


def top_dirs(job_id):
    """Папки верхнего уровня ссылки: сколько файлов, сколько весят, сколько уже скачано."""
    rows = q("""
        SELECT substr(path, 1, instr(substr(path, 2), '/')) AS top, COUNT(*) AS files,
               COALESCE(SUM(size), 0) AS bytes,
               COALESCE(SUM(CASE WHEN status IN ('ok', 'skip') THEN size END), 0) AS done,
               SUM(status = 'failed') AS failed
        FROM files WHERE job_id = ? AND instr(substr(path, 2), '/') > 0
        GROUP BY top ORDER BY bytes DESC""", job_id)
    excl = set(excluded_paths(job_id))
    for r in rows:
        r["excluded"] = r["top"] in excl
    return rows


def job_action(job_id, action):
    if action in ("pause", "resume"):
        x("UPDATE jobs SET paused=? WHERE id=?", int(action == "pause"), job_id)
    elif action in ("prioritize", "unprioritize"):
        x("UPDATE jobs SET priority=? WHERE id=?", int(action == "prioritize"), job_id)
    elif action == "retry":
        # превью «не вышло» тоже перепробуем, если оригинал снова не дастся
        x("UPDATE files SET status='queued', attempts=0, reason=NULL,"
          " preview = CASE WHEN preview = ? THEN ? ELSE preview END WHERE job_id=? AND status='failed'",
          PREVIEW_NA, PREVIEW_NONE, job_id)
        paths = [r["path"] for r in q("SELECT path FROM problems WHERE job_id=? AND kind='LIST_FAIL'", job_id)]
        x("DELETE FROM problems WHERE job_id=? AND kind='LIST_FAIL'", job_id)
        if paths:
            start_walk(job_id, paths)
        log(f"ссылка #{job_id}: битые отправлены на повтор")
    return {"ok": True}


# ---------- HTTP ----------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def send(self, code, body, ctype="application/json; charset=utf-8"):
        data = body if isinstance(body, bytes) else body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def local_host(self):
        host = (self.headers.get("Host") or "").split(":")[0]
        return host in ("localhost", "127.0.0.1")

    def do_GET(self):
        if not self.local_host():
            return self.send(403, "{}")
        if self.path == "/":
            with open(os.path.join(HERE, "index.html"), "rb") as fh:
                self.send(200, fh.read(), "text/html; charset=utf-8")
        elif self.path == "/api/state":
            self.send(200, json.dumps(state(), ensure_ascii=False))
        elif self.path == "/api/destinations":
            self.send(200, json.dumps({"destinations": list_destinations(), "default": default_dest()},
                                      ensure_ascii=False))
        elif self.path == "/broken.tsv":
            self.send(200, broken_tsv(), "text/tab-separated-values; charset=utf-8")
        else:
            self.send(404, "{}")

    def do_POST(self):
        # JSON-only + проверка Host: чужая вкладка в браузере не сможет добавить ссылки
        if not self.local_host() or not (self.headers.get("Content-Type") or "").startswith("application/json"):
            return self.send(403, "{}")
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        m = re.fullmatch(r"/api/jobs/(\d+)/(pause|resume|retry|exclude|include|prioritize|unprioritize)", self.path)
        if self.path == "/api/jobs":
            res = add_jobs(body.get("urls"), body.get("dest"), bool(body.get("priority")), body.get("subdir"))
        elif m and m[2] in ("exclude", "include"):
            res = set_excluded(int(m[1]), body.get("path"), m[2] == "exclude")
        elif m:
            res = job_action(int(m[1]), m[2])
        elif self.path == "/api/settings":
            if "workers" in body:
                settings["workers"] = max(1, min(MAX_WORKERS, int(body["workers"])))
                x("INSERT OR REPLACE INTO kv(k, v) VALUES('workers', ?)", str(settings["workers"]))
                ensure_workers(settings["workers"])
            if "paused" in body:
                settings["paused"] = bool(body["paused"])
            res = settings
        else:
            return self.send(404, "{}")
        self.send(200, json.dumps(res, ensure_ascii=False))


def main():
    init_db()
    for j in q("SELECT id FROM jobs WHERE walk_status != 'done'"):
        x("DELETE FROM problems WHERE job_id=? AND kind='LIST_FAIL'", j["id"])
        x("UPDATE jobs SET walk_dirs=0 WHERE id=?", j["id"])
        start_walk(j["id"])
    ensure_workers(settings["workers"])
    threading.Thread(target=sampler, daemon=True).start()
    threading.Thread(target=exporter, daemon=True).start()
    places = ", ".join(f"{d['name']} ({'ok' if d['ok'] else d['error']})" for d in list_destinations())
    log(f"Интерфейс: http://localhost:{PORT}  ·  куда можно качать: {places}")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
