"""Человекочитаемые подписи журнала; значения форматируются по смыслу поля."""
import json

AUDIT_METADATA = ('id','created_at','updated_at','added_at','added_by','created_by','decided_at')


def business_state(snapshot):
    if isinstance(snapshot, str):
        snapshot = json.loads(snapshot)
    if isinstance(snapshot, dict):
        return {key: value for key, value in snapshot.items() if key not in AUDIT_METADATA}
    return snapshot


def has_changes(before, after):
    return business_state(before) != business_state(after)


def changes_predicate():
    keys = 'ARRAY[' + ','.join("'" + key + "'" for key in AUDIT_METADATA) + ']::text[]'
    def state(column):
        value = f'COALESCE({column},\'null\')::jsonb'
        return f"(CASE WHEN jsonb_typeof({value})='object' THEN {value}-{keys} ELSE {value} END)"
    return f'{state("a.before_json")} IS DISTINCT FROM {state("a.after_json")}'

STATUSES = {'pending':'Ожидает решения','approved':'Одобрена','rejected':'Отклонена','canceled':'Отменён',
            'queued':'В очереди','running':'В работе','paused':'На паузе','completed':'Готов','failed':'Ошибка','deleted':'Удалён'}
FIELDS = {'name':'Название','description':'Описание','token_limit':'Лимит группы','personal_token_limit':'Личный лимит',
          'display_name':'Название модели','model':'Идентификатор модели','provider':'Провайдер',
          'is_enabled':'Включена','is_default':'Основная','sort_order':'Порядок',
          'last_check_ok':'Доступна','last_check_message':'Результат проверки',
          'limit_period':'Период','is_active':'Активна','role':'Роль','role_in_group':'Роль в группе',
          'limit_mode':'Режим лимита','status':'Статус','decision_comment':'Комментарий','delete_reason':'Причина удаления'}
VALUES = {'admin':'Администратор','manager':'Руководитель','user':'Пользователь','member':'Пользователь',
          'group':'Групповой','personal':'Личный','month':'Месяц по Москве','none':'За всё время',
          'retention':'Хранение последних отчётов','paused_ttl':'Истёк срок паузы'}


def parse(value):
    if isinstance(value, dict): return value
    try: return json.loads(value or '{}')
    except (TypeError, ValueError): return {}


def value(field, item):
    if item is None or item == '':
        return 'без лимита' if field in {'token_limit','personal_token_limit'} else '—'
    if isinstance(item,bool): return 'Да' if item else 'Нет'
    if field == 'status': return STATUSES.get(str(item),str(item))
    if field in {'token_limit','personal_token_limit'}: return f'{int(item):,}'.replace(',',' ')
    return VALUES.get(str(item),str(item)) if field in {'role','role_in_group','limit_period','limit_mode','delete_reason'} else str(item)


def describe(row):
    before,after=parse(row.get('before_json')),parse(row.get('after_json'))
    if row['action']=='report.delete':after={**after,'status':'deleted'}
    changes=[{'field':field,'label':label,'before':value(field,before.get(field)),'after':value(field,after.get(field))}
             for field,label in FIELDS.items() if before.get(field)!=after.get(field)]
    who=row.get('target_full_name') or row.get('target_username') or 'пользователь'
    group=row.get('group_name') or before.get('name') or after.get('name')
    name=(after.get('display_name') or before.get('display_name')) if row['target_type']=='model' else row.get('report_name') if row['target_type']=='report' else group if row['target_type']=='group' else who
    names={'group':'Группа','report':'Отчёт','member':'Участник','request':'Заявка','user':'Пользователь','model':'Модель'}
    obj=f'{names.get(row["target_type"],"Объект")} «{name or "без названия"}»'
    if row['target_type'] in {'member','request'} and group:
        obj += f' · группа «{group}»'
    if row['action'] in {'member.add','member.save'} and not before:
        detail=f'Добавлен {who}'
    elif row['action']=='member.remove':detail=f'Исключён {who}'
    elif row['action']=='member.role':
        detail=f'Роль {who}: {value("role_in_group",before.get("role_in_group"))} → {value("role_in_group",after.get("role_in_group"))}'
    else:detail='; '.join(f'{c["label"]}: {c["before"]} → {c["after"]}' for c in changes) or 'Изменений значений нет'
    return {**row,'object_label':obj,'description':detail,'changes':changes}
