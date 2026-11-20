import httpx
import pytest
from fastapi import HTTPException
from unittest.mock import Mock

from app import db, model_catalog as models
from .test_models_profiles import catalog_db, empty_catalog_db


@pytest.mark.parametrize('identifier,pattern', [
    ('whisper-medium-local-preview','whisper'),
    ('whisper-turbo-local-preview','whisper'),
    ('TTS-1','tts'),
    ('speech-recognition','speech'),
    ('realtime-audio','audio'),
    ('asr-ru','asr'),
])
def test_speech_models_are_not_chat_candidates(identifier,pattern):
    assert models.chat_suitability(identifier)=={
        'is_chat_candidate':False,'hidden_reason':'Не подходит для чата: '+pattern}


@pytest.mark.parametrize('identifier',[
    'qwen3.8-27b','glm-4.6-357b','gpt-oss-120b','kimi-k2-instruct',
])
def test_chat_models_remain_visible(identifier):
    assert models.chat_suitability(identifier)=={'is_chat_candidate':True,'hidden_reason':None}


@pytest.mark.asyncio
async def test_disabled_missing_usage_includes_deleted_reports_and_matches_provider(catalog_db,monkeypatch):
    models.save({'id':1},{'is_enabled':False},2)
    unused=models.save({'id':1},{'provider':'openai','model':'test-off-unused','is_enabled':False})
    with catalog_db() as conn:
        conn.execute('ALTER TABLE reports ADD COLUMN deleted_at TEXT')
        conn.execute('INSERT INTO reports VALUES (?,?,?)',('openai','test-accurate','2026-11-11'))
        # Одноимённая модель другого провайдера не делает текущую использованной.
        conn.execute('INSERT INTO reports VALUES (?,?,?)',('ollama','test-off-unused',None))
    real=httpx.AsyncClient
    transport=httpx.MockTransport(lambda request:httpx.Response(200,json={'data':[{'id':'test-fast'}]}))
    monkeypatch.setattr(models.httpx,'AsyncClient',lambda **kwargs:real(transport=transport,**kwargs))
    monkeypatch.setattr(models,'get_provider_base_url',lambda *args:'https://provider.invalid/v1')
    result=await models.catalog('openai')
    missing={r['model']:r for r in result['missing']}
    assert missing['test-accurate']['is_used'] is True
    assert missing['test-off-unused']['is_used'] is False
    assert all(not r['is_enabled'] for r in missing.values())
    assert not any('is_used' in row for row in models.all_models())
    # Выдача метаданных не ослабляет окончательную проверку DELETE.
    with pytest.raises(HTTPException) as exc:models.delete({'id':1},2)
    assert exc.value.status_code==409
    assert models.delete({'id':1},unused['id'])=={'deleted':True}


def test_no_history_query_when_missing_models_are_enabled(monkeypatch):
    conn=Mock(side_effect=AssertionError('unexpected history lookup'))
    monkeypatch.setattr(db,'get_conn',conn)
    row=dict(id=1,model='test-missing',is_enabled=True)
    assert models.missing_catalog_models([row],[])==[row]
    assert models.missing_catalog_models([row],[row['model']])==[]
    conn.assert_not_called()


def test_disappeared_disabled_row_is_not_assumed_unused(catalog_db):
    row=dict(id=999,model='test-removed',is_enabled=False)
    assert models.missing_catalog_models([row],[])[0]['is_used'] is True
