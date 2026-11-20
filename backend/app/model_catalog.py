"""Каталог моделей: транзакционное управление и короткий локальный кэш."""
import os
import asyncio
import re
import threading
import time
from fastapi import HTTPException
import httpx
from app import db
from app.config import TLS_VERIFY, get_provider_base_url, initial_model_catalog
from app.access import audit
from app.providers.openai_provider import OpenAIProvider, normalize_api_key

_cache = None
_expires = 0.0
_lock = threading.Lock()
CACHE_SECONDS = 10
MIGRATION = 'llm_models_v2_2_0'
EMPTY_CATALOG_MESSAGE = 'Модели ещё не настроены. Обратитесь к администратору'
# Единственное правило отбора каталога для первичной настройки чата.
NON_CHAT_PATTERNS = ('bge','embed','sdxl','image','flux','vl','perplexity','sonar','rerank',
                     'whisper','tts','speech','audio','asr')
BATCH_SIZE = 20


def chat_suitability(model):
    pattern=next((p for p in NON_CHAT_PATTERNS if p in model.lower()),None)
    return {'is_chat_candidate':pattern is None,
            'hidden_reason':None if pattern is None else 'Не подходит для чата: '+pattern}


async def add_batch(user, data):
    provider=data.get('provider')
    names=data.get('models')
    if provider not in {'openai','ollama'} or not isinstance(names,list) or not 1<=len(names)<=BATCH_SIZE:
        raise HTTPException(422,f'Выберите от 1 до {BATCH_SIZE} моделей одного провайдера')
    if any(not isinstance(n,str) or not n.strip() or len(n.strip())>200 for n in names):
        raise HTTPException(422,'Некорректные идентификаторы моделей')
    if len(set(n.strip() for n in names))!=len(names):raise HTTPException(422,'Модели не должны повторяться')
    results=[]
    for name in names:
        try:
            row=save(user,{'provider':provider,'model':name})
            results.append({'model':name,'created':True,'row':dict(row)})
        except HTTPException as exc:
            results.append({'model':name,'created':False,'error':str(exc.detail)})
    limit=asyncio.Semaphore(4)
    async def verify(item):
        if not item['created']:return
        async with limit:
            try:item['row']=await check(user,item['row']['id'])
            except Exception:item['error']='Модель добавлена, но проверка не завершилась. Повторите её из таблицы'
    await asyncio.gather(*(verify(item) for item in results))
    return {'results':results}


def init_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS llm_models (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        provider TEXT NOT NULL CHECK(provider IN ('openai','ollama')), model TEXT NOT NULL,
        display_name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '',
        is_enabled BOOLEAN NOT NULL DEFAULT TRUE, is_default BOOLEAN NOT NULL DEFAULT FALSE,
        sort_order INTEGER NOT NULL DEFAULT 0, last_check_at TEXT, last_check_ok BOOLEAN,
        last_check_message TEXT, last_check_latency_ms INTEGER,
        created_at TEXT NOT NULL, created_by BIGINT REFERENCES users(id),
        updated_at TEXT NOT NULL, updated_by BIGINT REFERENCES users(id), UNIQUE(provider,model))""")
    conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_llm_models_default ON llm_models(is_default) WHERE is_default AND is_enabled')
    if conn.execute('SELECT 1 FROM schema_migrations WHERE name=?', (MIGRATION,)).fetchone():
        return
    now = db._now_iso()
    if not conn.execute("SELECT 1 FROM llm_models LIMIT 1").fetchone():
        for order, (provider, model) in enumerate(initial_model_catalog()):
            conn.execute(
                """INSERT INTO llm_models (provider, model, display_name, is_default, sort_order, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (provider, model, model, order == 0, order, now, now),
            )
    conn.execute("INSERT INTO schema_migrations(name,applied_at) VALUES (?,?)", (MIGRATION, now))


def invalidate():
    global _cache, _expires
    with _lock:
        _cache, _expires = None, 0


def all_models(*, fresh=False):
    global _cache, _expires
    with _lock:
        if fresh or _cache is None or time.monotonic() >= _expires:
            with db.get_conn() as conn:
                _cache = [dict(r) for r in conn.execute('SELECT * FROM llm_models ORDER BY sort_order,id').fetchall()]
            _expires = time.monotonic() + CACHE_SECONDS
        return [dict(r) for r in _cache]


def public_models(provider):
    return [{k:r[k] for k in ('model','display_name','description','is_default','sort_order')}
            for r in all_models() if r['provider'] == provider and r['is_enabled']]


def require_enabled(provider, model, *, resume=False):
    enabled = [r for r in all_models(fresh=True) if r['is_enabled']]
    if not enabled and not resume: raise HTTPException(422, EMPTY_CATALOG_MESSAGE)
    if any(r['provider']==provider and r['model']==model for r in enabled):
        return
    safe_model = str(model)[:200]
    message = (f'Модель «{safe_model}» отключена. Создайте отчёт заново с другой моделью' if resume else
               f'Модель «{safe_model}» отключена администратором. Выберите другую')
    raise HTTPException(409 if resume else 422, message)


def validate_default(rows):
    enabled = [r for r in rows if r['is_enabled']]
    if enabled and sum(bool(r['is_default']) for r in enabled) != 1:
        raise HTTPException(409, 'Сначала выберите новую основную модель')
    if any(r['is_default'] and not r['is_enabled'] for r in rows):
        raise HTTPException(409, 'Основная модель должна быть включена')


def save(user, data, model_id=None):
    with db.get_conn() as conn:
        conn.execute('SELECT pg_advisory_xact_lock(2026111010)')
        rows = [dict(r) for r in conn.execute('SELECT * FROM llm_models ORDER BY id FOR UPDATE').fetchall()]
        before = next((r for r in rows if r['id']==model_id), None)
        if model_id is not None and before is None:
            raise HTTPException(404,'Модель не найдена')
        allowed = {'display_name','description','is_enabled','is_default','sort_order'}
        if before is None:
            allowed |= {'provider','model'}
        if set(data)-allowed:
            raise HTTPException(422,'Неизвестные поля модели')
        row = {**({'provider':'openai','model':'','display_name':'','description':'','is_enabled':True,
                   'is_default':not any(r['is_enabled'] for r in rows),'sort_order':len(rows)} if before is None else before), **data}
        for key, limit in [('provider',20),('model',200),('display_name',200),('description',500)]:
            if not isinstance(row[key],str) or len(row[key].strip())>limit:
                raise HTTPException(422,'Некорректное поле '+key)
            row[key] = row[key].strip()
        if row['provider'] not in {'openai','ollama'} or not row['model']:
            raise HTTPException(422,'Укажите провайдера и идентификатор')
        row['display_name'] = row['display_name'] or row['model']
        if any(type(row[k]) is not bool for k in ('is_enabled','is_default')) or type(row['sort_order']) is not int:
            raise HTTPException(422,'Некорректный статус или порядок')
        if any(r['provider']==row['provider'] and r['model']==row['model'] and r['id']!=model_id for r in rows):
            raise HTTPException(409,'Модель уже добавлена')
        others = [dict(r) for r in rows if r['id']!=model_id]
        if row['is_default']:
            for r in others: r['is_default'] = False
        validate_default([*others,row])
        if row['is_default']:
            for previous in rows:
                if previous['id'] != model_id and previous['is_default']:
                    cleared=conn.execute('''UPDATE llm_models SET is_default=FALSE,updated_at=?,updated_by=?
                        WHERE id=? RETURNING *''',(db._now_iso(),user['id'],previous['id'])).fetchone()
                    audit(conn,user['id'],'model.update','model',previous['id'],previous,cleared)
        columns = ['provider','model','display_name','description','is_enabled','is_default','sort_order']
        values = [row[k] for k in columns]
        if before:
            result = conn.execute('UPDATE llm_models SET '+','.join(k+'=?' for k in columns)+
                ',updated_at=?,updated_by=? WHERE id=? RETURNING *', [*values,db._now_iso(),user['id'],model_id]).fetchone()
        else:
            result = conn.execute('INSERT INTO llm_models('+','.join(columns)+',created_at,updated_at,created_by,updated_by) VALUES ('+
                ','.join('?' for _ in range(11))+') RETURNING *', [*values,db._now_iso(),db._now_iso(),user['id'],user['id']]).fetchone()
        audit(conn,user['id'],'model.update' if before else 'model.create','model',result['id'],before,result)
    invalidate()
    return result


def delete(user, model_id):
    with db.get_conn() as conn:
        conn.execute('SELECT pg_advisory_xact_lock(2026111010)')
        row = conn.execute('SELECT * FROM llm_models WHERE id=? FOR UPDATE',(model_id,)).fetchone()
        if not row: raise HTTPException(404,'Модель не найдена')
        if conn.execute('SELECT 1 FROM reports WHERE provider=? AND model=? LIMIT 1',(row['provider'],row['model'])).fetchone():
            raise HTTPException(409,'Модель использовалась в отчётах; отключите её')
        if row['is_default'] and conn.execute('SELECT 1 FROM llm_models WHERE is_enabled AND id<>? LIMIT 1',(model_id,)).fetchone():
            raise HTTPException(409,'Сначала выберите новую основную модель')
        conn.execute('DELETE FROM llm_models WHERE id=?',(model_id,))
        audit(conn,user['id'],'model.delete','model',model_id,row,None)
    invalidate()
    return {'deleted':True}


def sanitized(message):
    from app.logging_utils import _redact
    text = str(message)
    # Ошибка провайдера может повторить любое значение из конфигурации запроса.
    secrets = [os.getenv('OPENAI_API_KEY',''),os.getenv('OPENAI_BASE_URL',''),os.getenv('OLLAMA_BASE_URL','')]
    for name in ('OPENAI_EXTRA_HEADERS_JSON','OPENAI_EXTRA_BODY_JSON'):
        def values(item):
            if isinstance(item,dict):
                for v in item.values(): yield from values(v)
            elif isinstance(item,list):
                for v in item: yield from values(v)
            elif isinstance(item,str) and item: yield item
        secrets.extend(values(OpenAIProvider._json_object_env(name)))
    for secret in sorted(set(secrets),key=len,reverse=True):
        if secret: text = text.replace(secret,'[скрыто]')
    text=re.sub(r'https?://[^\s<>"\']+','[адрес скрыт]',text)
    return _redact(text)[:500]


def connection(provider):
    if provider not in {'openai','ollama'}: raise HTTPException(422,'Неизвестный провайдер')
    base = get_provider_base_url(provider).rstrip('/')
    if provider == 'openai':
        headers = {'Authorization':'Bearer '+normalize_api_key(os.getenv('OPENAI_API_KEY'))}
        headers.update({k:str(v) for k,v in OpenAIProvider._json_object_env('OPENAI_EXTRA_HEADERS_JSON').items()})
        return OpenAIProvider._v1_base(base), headers
    return base, {}


def provider_error(response):
    try:
        data=response.json(); error=data.get('error',{})
        message=error.get('message','') if isinstance(error,dict) else str(error)
    except (ValueError,AttributeError): message=''
    return sanitized(f'Провайдер ответил {response.status_code}: {message}')


async def check(user, model_id):
    row = next((r for r in all_models(fresh=True) if r['id']==model_id),None)
    if not row: raise HTTPException(404,'Модель не найдена')
    base,headers = connection(row['provider'])
    started=time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=30,verify=TLS_VERIFY) as client:
            if row['provider']=='openai':
                body=OpenAIProvider._json_object_env('OPENAI_EXTRA_BODY_JSON')
                for key in ('max_completion_tokens','max_output_tokens'):
                    if key in body: body[key]=16
                body.update(model=row['model'],messages=[{'role':'user','content':'Ответьте: OK'}],max_tokens=16,stream=False)
                response=await client.post(base+'/chat/completions',headers=headers,json=body)
            else:
                response=await client.post(base+'/api/generate',json={'model':row['model'],'prompt':'Ответьте: OK',
                    'stream':False,'options':{'num_predict':16}})
        ok=response.is_success
        message='Доступна' if ok else provider_error(response)
    except Exception:
        ok,message=False,'Провайдер недоступен или не ответил за 30 секунд'
    elapsed=round((time.monotonic()-started)*1000)
    with db.get_conn() as conn:
        current=conn.execute('SELECT * FROM llm_models WHERE id=? FOR UPDATE',(model_id,)).fetchone()
        if not current: raise HTTPException(404,'Модель удалена во время проверки')
        result=conn.execute('''UPDATE llm_models SET last_check_at=?,last_check_ok=?,last_check_message=?,
            last_check_latency_ms=?,updated_at=?,updated_by=? WHERE id=? RETURNING *''',
            (db._now_iso(),ok,message,elapsed,db._now_iso(),user['id'],model_id)).fetchone()
        audit(conn,user['id'],'model.check','model',model_id,current,result)
    invalidate()
    return result


async def catalog(provider):
    base,headers=connection(provider)
    try:
        async with httpx.AsyncClient(timeout=30,verify=TLS_VERIFY) as client:
            response=await client.get(base+('/models' if provider=='openai' else '/api/tags'),headers=headers)
        if not response.is_success: raise HTTPException(502,provider_error(response))
        data=response.json()
        names=sorted({str(r.get('id') if provider=='openai' else r.get('name')) for r in
                      data.get('data' if provider=='openai' else 'models',[]) if isinstance(r,dict)
                      and r.get('id' if provider=='openai' else 'name')})
    except HTTPException: raise
    except Exception: raise HTTPException(502,'Не удалось получить каталог провайдера')
    ours=[r for r in all_models(fresh=True) if r['provider']==provider]
    return {'models':[{'model':n,'already_added':any(r['model']==n for r in ours),**chat_suitability(n)} for n in names],
            'missing':missing_catalog_models(ours,names)}


def missing_catalog_models(ours,names):
    available=set(names)
    missing=[dict(row) for row in ours if row['model'] not in available]
    disabled=[row['id'] for row in missing if not row['is_enabled']]
    if disabled:
        # История учитывается целиком, включая мягко удалённые и нулевые отчёты.
        # Один запрос на набор, без отдельного обращения для каждой модели.
        with db.get_conn() as conn:
            usage=conn.execute('''SELECT m.id,COUNT(r.model)>0 AS is_used FROM llm_models m
                LEFT JOIN reports r ON r.provider=m.provider AND r.model=m.model
                WHERE m.id IN ('''+','.join('?' for _ in disabled)+') GROUP BY m.id',disabled).fetchall()
        used={row['id']:bool(row['is_used']) for row in usage}
        for row in missing:
            if not row['is_enabled']:row['is_used']=used.get(row['id'],True)
    return missing
