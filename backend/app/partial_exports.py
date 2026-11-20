"""Общий кэш промежуточных файлов владельца и администратора."""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import tempfile
import threading
import time
import uuid

from fastapi import HTTPException
from app import db
from app.config import (RESULTS_DIR, PARTIAL_EXPORT_CACHE_ENABLED, PARTIAL_EXPORT_CACHE_TTL_HOURS,
                        PARTIAL_EXPORT_WAIT_SEC)

logger = logging.getLogger(__name__)
MOSCOW = timezone(timedelta(hours=3))
ACTIVE = {'running','paused','queued'}
WAIT_MESSAGE = 'Файл ещё собирается, повторите через минуту'
VERSION_FIELDS = ('results_revision','total_rows','processed_rows','success_rows','failed_rows','groups_total','groups_processed')
RELEASE = "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end return 0"
RENEW = "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('expire',KEYS[1],ARGV[2]) end return 0"
PUBLISH = """if redis.call('get',KEYS[1])~=ARGV[1] or redis.call('get',KEYS[2])~=ARGV[2]
    or redis.call('get',KEYS[4])~=ARGV[5] then return {0,''} end
    local old=redis.call('get',KEYS[3])
    if old and tonumber(cjson.decode(old).revision)>tonumber(ARGV[3]) then return {0,''} end
    redis.call('set',KEYS[3],ARGV[4],'EX',ARGV[6]); return {1,old or ''}"""


def init_schema(conn):
    conn.execute('ALTER TABLE reports ADD COLUMN IF NOT EXISTS results_revision BIGINT NOT NULL DEFAULT 0')
    conn.execute("INSERT INTO schema_migrations(name,applied_at) VALUES ('partial_export_revision_v2_2_0',?) ON CONFLICT(name) DO NOTHING", (db._now_iso(),))


def version(row):
    # updated_at и status намеренно исключены: heartbeat/пауза не меняют данные среза.
    payload={key:int(row.get(key) or 0) for key in VERSION_FIELDS}
    return hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest()


def directory():
    return RESULTS_DIR / 'partial_cache'


def safe_id(report_id):
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,160}',report_id):
        raise HTTPException(404,'Отчёт не найден')
    return report_id


def key(report_id,fmt):
    return f'partial_export:{report_id}:{fmt}'


def metadata(report_id):
    row=db.get_report_any(report_id)
    if not row: raise HTTPException(404,'Отчёт не найден')
    if row['status'] not in ACTIVE:
        raise HTTPException(409,'Отчёт уже не активен. Скачайте итоговый результат')
    return row


def checked_path(value,report_id):
    path=Path(value).resolve()
    if path.parent!=directory().resolve() or not belongs(path,report_id):
        raise ValueError('Некорректный путь кэша')
    return path


def belongs(path,report_id):
    return bool(re.fullmatch(re.escape(safe_id(report_id))+r'_[0-9a-f]{64}(?:\.[0-9a-f]{32})?\.(xlsx|json)(\.tmp)?',path.name))


def unlink(path):
    try: path.unlink(missing_ok=True)
    except OSError:
        # Открытый файл на Windows будет убран следующей фоновой очисткой.
        logger.warning('Не удалось удалить файл промежуточного кэша: %s',path.name)


@dataclass
class Export:
    path: Path
    captured_at: str
    temporary: bool = False

    def fallback_name(self,report_id):
        at=datetime.fromisoformat(self.captured_at).astimezone(MOSCOW)
        return f'report_{report_id[:8]}_срез_{at:%d-%m_%H-%M}'


def slice_label(row,summary,at):
    stamp=datetime.fromisoformat(at).astimezone(MOSCOW).strftime('%d.%m.%Y %H:%M МСК')
    label=f'Срез на {stamp}, обработано {int(summary.get("processed_rows") or 0)} из {int(summary.get("total_rows") or 0)}'
    if str(row.get('group_by_column') or '').strip():
        label+=f'; групп {int(summary.get("groups_processed",row.get("groups_processed")) or 0)} из {int(summary.get("groups_total",row.get("groups_total")) or 0)}'
    return label


def build(row,fmt,path,at,conn):
    from app.services.excel_service import export_results_xlsx, export_raw_json
    from app import __version__
    rows=lambda:db.iter_report_rows(row['id'],batch_size=2000,conn=conn)
    if fmt=='xlsx':
        group_column=str(row.get('group_by_column') or '').strip() or None
        summary=db.get_report_summary_agg(row['id'],conn=conn,include_groups=bool(group_column))
        summary.update(partial=True)
        summary['Срез']=slice_label(row,summary,at)
        columns=row.get('non_analysis_columns_json') or []
        if isinstance(columns,str):columns=json.loads(columns)
        export_results_xlsx(path,rows,summary,prompt_example=row.get('prompt_template'),
            group_by_column=group_column,source_columns=columns)
    else:
        export_raw_json(path,rows_factory=rows,provider=str(row.get('provider') or ''),model=str(row.get('model') or ''),
            prompt_template=str(row.get('prompt_template') or ''),app_version=__version__)


@contextmanager
def snapshot(report_id):
    with db.get_conn() as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        row=conn.execute('SELECT * FROM reports WHERE id=? AND deleted_at IS NULL',(report_id,)).fetchone()
        if not row:raise HTTPException(404,'Отчёт не найден')
        if row['status'] not in ACTIVE:raise HTTPException(409,'Скачайте итоговый результат отчёта')
        yield dict(row),conn


def temporary_export(report_id,fmt):
    at=db._now_iso()
    with tempfile.NamedTemporaryFile(delete=False,suffix='.'+fmt,dir=RESULTS_DIR) as temp:path=Path(temp.name)
    try:
        with snapshot(report_id) as (row,conn):build(row,fmt,path,at,conn)
        return Export(path,at,True)
    except BaseException:
        unlink(path);raise


@contextmanager
def guard(client,report_id,fmt,wait=5):
    name=f'partial_export:guard:{report_id}:{fmt}';token=uuid.uuid4().hex
    deadline=time.monotonic()+wait
    while not client.set(name,token,nx=True,ex=30):
        if time.monotonic()>=deadline:raise HTTPException(503,WAIT_MESSAGE)
        time.sleep(.05)
    try:yield name,token
    finally:
        try:client.eval(RELEASE,1,name,token)
        except Exception:pass


def cached(client,report_id,fmt,v):
    value=client.get(key(report_id,fmt))
    if not value:return None
    try:
        entry=json.loads(value)
        path=checked_path(entry['path'],report_id)
        if entry['version']==v and path.is_file():return Export(path,entry['captured_at'])
    except (ValueError,KeyError,TypeError,OSError):pass
    return None


def get_export(report_id,fmt):
    safe_id(report_id)
    if fmt not in {'xlsx','json'}:raise ValueError('Неизвестный формат')
    if not PARTIAL_EXPORT_CACHE_ENABLED:return temporary_export(report_id,fmt)
    # Ошибки Redis дают прежнюю независимую временную сборку. Ошибки экспортера не скрываем.
    try:
        from redis.exceptions import RedisError
    except ImportError:
        class RedisError(Exception): pass
    try:
        client=db.get_redis_client()
        if client is None:return temporary_export(report_id,fmt)
        return _cached_export(client,report_id,fmt)
    except (RedisError,ConnectionError,TimeoutError):
        return temporary_export(report_id,fmt)


def _cached_export(client,report_id,fmt):
    deadline=time.monotonic()+PARTIAL_EXPORT_WAIT_SEC
    ttl=60  # Heartbeat удерживает живую сборку; после аварии не ждём весь WAIT_SEC.
    cache_ttl=max(1,int(PARTIAL_EXPORT_CACHE_TTL_HOURS*3600))
    epoch_key=f'partial_export:epoch:{report_id}:{fmt}'
    directory().mkdir(parents=True,exist_ok=True)
    while True:
        row=metadata(report_id);v=version(row)
        hit=cached(client,report_id,fmt,v)
        if hit:return hit
        if time.monotonic()>=deadline:raise HTTPException(503,WAIT_MESSAGE)
        lease=f'partial_export:lock:{report_id}:{fmt}:{v}';token=uuid.uuid4().hex
        if not client.set(lease,token,nx=True,ex=ttl):
            time.sleep(.1);continue
        stop=threading.Event()
        renew_until=time.monotonic()+max(60,PARTIAL_EXPORT_WAIT_SEC)
        def renew():
            while not stop.wait(max(1,min(10,ttl/3))):
                if time.monotonic()>=renew_until:return
                try:
                    if not client.eval(RENEW,1,lease,token,ttl):return
                except Exception:return
        heartbeat=threading.Thread(target=renew,daemon=True);heartbeat.start()
        temp=None;path=None
        try:
            client.set(epoch_key,uuid.uuid4().hex,nx=True,ex=max(cache_ttl,ttl*4))
            epoch=client.get(epoch_key)
            if isinstance(epoch,bytes):epoch=epoch.decode()
            if not epoch:continue
            # Generation защищает файл нового запуска от удаления запоздалой старой сборкой.
            file_hash=hashlib.sha256((v+epoch).encode()).hexdigest()
            path=directory()/f'{report_id}_{file_hash}.{fmt}'
            temp=directory()/f'{report_id}_{v}.{uuid.uuid4().hex}.{fmt}.tmp'
            at=db._now_iso()
            with snapshot(report_id) as (snap,conn):
                if version(snap)!=v:continue
                logger.info('Сборка промежуточной выгрузки: report_id=%s format=%s version=%s',report_id,fmt,v,
                    extra={'event_type':'partial_export.build','report_id':report_id})
                build(snap,fmt,temp,at,conn)
            with guard(client,report_id,fmt,wait=max(0,deadline-time.monotonic())) as (guard_key,guard_token):
                metadata(report_id)  # Не публикуем завершённый/удалённый отчёт.
                if client.get(epoch_key) not in (epoch,epoch.encode()) or client.get(lease) not in (token,token.encode()):continue
                os.replace(temp,path)
                entry=dict(version=v,revision=int(snap.get('results_revision') or 0),path=str(path),captured_at=at)
                result=client.eval(PUBLISH,4,lease,epoch_key,key(report_id,fmt),guard_key,
                    token,epoch,entry['revision'],json.dumps(entry),guard_token,cache_ttl)
                if not int(result[0]):
                    # Другой владелец мог уже опубликовать тот же путь; не удаляем его файл.
                    current=client.get(key(report_id,fmt))
                    if not current or json.loads(current).get('path')!=str(path):unlink(path)
                    path=None;continue
                if result[1]:
                    old=json.loads(result[1]);old_path=checked_path(old['path'],report_id)
                    if old_path!=path:unlink(old_path)
                # После восстановления Redis могли остаться непроиндексированные файлы.
                for obsolete in directory().glob(f'{report_id}_*.{fmt}'):
                    if obsolete!=path and belongs(obsolete,report_id):unlink(obsolete)
                return Export(path,at)
        finally:
            stop.set();heartbeat.join(timeout=1)
            if temp:unlink(temp)
            try:client.eval(RELEASE,1,lease,token)
            except Exception:pass
            # При потере Redis после rename файл может остаться сиротой. Его уберёт
            # GC; удалять итог вне guard нельзя — другой владелец мог занять этот путь.


def invalidate(report_id):
    safe_id(report_id)
    client=db.get_redis_client()
    for fmt in ('xlsx','json'):
        def remove():
            if client is not None:client.delete(key(report_id,fmt))
            for path in directory().glob(f'{report_id}_*.{fmt}'):
                if belongs(path,report_id):unlink(path)
            for path in directory().glob(f'{report_id}_*.{fmt}.tmp'):
                if belongs(path,report_id):unlink(path)
            # Смена поколения последняя под guard: ни одна старая сборка не опубликуется.
            if client is not None:
                client.set(f'partial_export:epoch:{report_id}:{fmt}',uuid.uuid4().hex,
                    ex=max(3600,int(PARTIAL_EXPORT_CACHE_TTL_HOURS*3600)))
        try:
            if client is None:remove()
            else:
                with guard(client,report_id,fmt):remove()
        except Exception:
            for path in directory().glob(f'{report_id}_*'):
                if belongs(path,report_id):unlink(path)
            logger.warning('Очистка промежуточного кэша отложена: report_id=%s',report_id)


def cleanup():
    if not directory().exists():return 0
    client=db.get_redis_client();now=time.time();removed=0
    for path in directory().iterdir():
        match=re.fullmatch(r'(.+)_([0-9a-f]{64})(?:\.([0-9a-f]{32}))?\.(xlsx|json)(\.tmp)?',path.name)
        if not match:continue
        rid,v,_,fmt,is_temp=match.groups()
        try:
            with guard(client,rid,fmt,wait=0) if client is not None else _no_guard():
                if not path.exists():continue
                age=now-path.stat().st_mtime
                expired=age>PARTIAL_EXPORT_CACHE_TTL_HOURS*3600
                orphan=False
                if age>600 and client is not None:
                    if is_temp:
                        orphan=not any(client.get(f'partial_export:lock:{rid}:{f}:{v}') for f in ('xlsx','json'))
                    else:
                        value=client.get(key(rid,fmt))
                        orphan=not value or json.loads(value).get('path')!=str(path)
                if expired or orphan:
                    if client is not None and not is_temp:
                        value=client.get(key(rid,fmt))
                        if value and json.loads(value).get('path')==str(path):client.delete(key(rid,fmt))
                    unlink(path);removed+=1
        except Exception:
            logger.debug('Промежуточный файл пропущен очисткой: %s',path.name)
    return removed


@contextmanager
def _no_guard():yield
