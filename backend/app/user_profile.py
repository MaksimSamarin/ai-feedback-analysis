"""Профиль из проверенных SSO claims; контакты и токены не сохраняются."""
import logging
from app import db

PUBLIC_FIELDS = ('full_name','job_title','company')


def profile_claims(claims):
    def clean(key, limit):
        value = claims.get(key)
        return value.strip()[:limit] or None if isinstance(value,str) else None
    full = clean('fullname',300) or ' '.join(filter(None,(clean('family_name',100),clean('given_name',100),clean('middle_name',100)))) or clean('name',300)
    return {'full_name':full[:300] if full else None,'job_title':clean('title',300),'company':clean('company',300),
            'employee_id':clean('employeeID',128)}


def init_schema(conn):
    for name in (*PUBLIC_FIELDS,'employee_id','profile_synced_at'):
        conn.execute(f'ALTER TABLE users ADD COLUMN IF NOT EXISTS {name} TEXT')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_users_full_name_lower ON users(lower(full_name))')
    conn.execute('SAVEPOINT profile_trgm')
    try:
        # Расширение опционально; его отсутствие не прерывает миграцию.
        conn.execute("""DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_trgm') THEN
            CREATE INDEX IF NOT EXISTS idx_users_full_name_trgm ON users USING gin(full_name gin_trgm_ops);
            END IF; END $$""")
    except Exception:
        conn.execute('ROLLBACK TO SAVEPOINT profile_trgm')
        logging.getLogger(__name__).warning('Поиск ФИО работает без триграммного индекса')
    finally: conn.execute('RELEASE SAVEPOINT profile_trgm')
    conn.execute("INSERT INTO schema_migrations(name,applied_at) VALUES ('users_sso_profile_v2_2_0',?) ON CONFLICT(name) DO NOTHING",(db._now_iso(),))


def sync(user_id, profile):
    # Только белый список: phone_number и прочие claims сюда не попадают.
    fields = (*PUBLIC_FIELDS,'employee_id')
    values = [(str(profile.get(k) or '').strip()[:128 if k=='employee_id' else 300] or None) for k in fields]
    with db.get_conn() as conn:
        conn.execute('UPDATE users SET '+','.join(f'{k}=COALESCE(?,{k})' for k in fields)+
                     ',profile_synced_at=? WHERE id=?', [*values,db._now_iso(),user_id])


def columns(admin=False, alias='u', groups=None):
    fields = (*PUBLIC_FIELDS,*(('employee_id',) if admin else ()))
    if not admin and groups is not None:
        ids = ','.join(str(int(g)) for g in groups) or 'NULL'
        return ','.join(f'CASE WHEN EXISTS(SELECT 1 FROM access_group_members pm WHERE pm.user_id={alias}.id '
                        f'AND pm.group_id IN ({ids})) THEN {alias}.{k} END AS {k}' for k in fields)
    return ','.join(alias+'.'+k for k in fields)


def scoped_name(scope, alias='u'):
    if scope.admin: return alias+'.full_name'
    ids=','.join(str(int(g)) for g in scope.groups) or 'NULL'
    return (f'CASE WHEN EXISTS(SELECT 1 FROM access_group_members nm WHERE nm.user_id={alias}.id '
            f'AND nm.group_id IN ({ids})) THEN {alias}.full_name END')
