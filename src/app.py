import json
import os
import base64
import uuid
import urllib.request
from urllib.error import HTTPError
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from nacl.signing import VerifyKey
from nacl.exceptions import BadSignatureError


# ===== 起動確認用 =====
CODE_VERSION = "2026-01-07-2250-worker-v1"
print("BOOT CODE_VERSION =", CODE_VERSION)

# クライアント/リソースはグローバル化（高速化＆安定）
lambda_client = boto3.client("lambda")
ddb = boto3.resource("dynamodb")
scheduler = boto3.client("scheduler")

# =========
# Helpers
# =========

def _get_header(headers: dict, name: str):
    if not headers:
        return None
    return headers.get(name) or headers.get(name.lower()) or headers.get(name.upper())

def _resp(obj: dict, status_code: int = 200):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "isBase64Encoded": False,
        "body": json.dumps(obj, ensure_ascii=False),
    }

def _now_iso():
    return datetime.now(timezone.utc).isoformat()
JST = ZoneInfo("Asia/Tokyo")

def _parse_jst_state_at(s: str) -> datetime | None:
    """
    'YYYY-MM-DD HH:MM' を JST として datetime にする
    """
    if not s:
       return None
    s = s.strip()
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
        return dt.replace(tzinfo=JST)
    except ValueError:
        return None

def _scheduler_at_expr(dt: datetime) -> str:
    """
    EventBridge Scheduler の at() 用（秒まで）
    例: at(2026-01-19T21:00:00)
    """
    return f"at({dt.strftime('%Y-%m-%dT%H:%M:%S')})"

def _notice_remind_schedule_name(guild_id: str, notice_id: str) -> str:
    """
    Scheduler Name 制約:
      - 文字: [0-9a-zA-Z-_.]+ だけ
      - 長さ <= 64
    notice_id は "NTC#<uuid>" なので <uuid> 部分だけ使う
    """
    nid = notice_id.split("#", 1)[1] if "#" in notice_id else notice_id
    nid = nid[:32]  # uuid(32)想定。保険で切る
    return f"ntc-{guild_id}-{nid}-remind"

def _parse_body(event):
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    return body

def _verify_discord_request(headers: dict, raw_body: str):
    signature = _get_header(headers, "x-signature-ed25519")
    timestamp = _get_header(headers, "x-signature-timestamp")
    if not signature or not timestamp:
        return False, "missing signature headers"

    public_key_hex = os.environ.get("DISCORD_PUBLIC_KEY")
    if not public_key_hex:
        return False, "DISCORD_PUBLIC_KEY is not set"

    message = (timestamp + raw_body).encode("utf-8")
    try:
        vk = VerifyKey(bytes.fromhex(public_key_hex))
        vk.verify(message, bytes.fromhex(signature))
        return True, None
    except (BadSignatureError, ValueError):
        return False, "invalid request signature"

def _get_tables():
    events = ddb.Table(os.environ["DDB_EVENTS_TABLE"])
    members = ddb.Table(os.environ["DDB_EVENT_MEMBERS_TABLE"])
    notices = ddb.Table(os.environ["DDB_NOTICES_TABLE"])
    acks = ddb.Table(os.environ["DDB_NOTICE_ACKS_TABLE"])
    return events, members, notices, acks

def _split_custom_id(custom_id: str):
    if not custom_id or ":" not in custom_id:
        return custom_id, None
    k, v = custom_id.split(":", 1)
    return k, v

def _decorate_title(title: str) -> str:
    line = "━━━━━━━━━━━━━━"
    return f"{line}\n **{title}** \n{line}"



# =========
# Discord helpers
# =========

def defer_ephemeral():
    # まず即ACK（3秒制限回避）
    return {"type": 5, "data": {"flags": 64}}  # DEFERRED_CHANNEL_MESSAGE_WITH_SOURCE

DISCORD_UA = "DiscordBot (shishigamu-event-bot, 0.1)"  # 好きに命名OK（DiscordBot を含める）

def discord_followup(app_id, token, message):
    url = f"https://discord.com/api/v10/webhooks/{app_id}/{token}"

    body_obj = message if isinstance(message, dict) else {"content": str(message)}
    payload = json.dumps(body_obj, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": DISCORD_UA,  # ★追加
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        print("DISCORD_HTTPERROR(FOLLOWUP)", e.code, e.reason)
        print("DISCORD_HTTPERROR_BODY(FOLLOWUP)", err_body)
        raise

def discord_send_message_bot(channel_id: str, message: dict):
    bot_token = os.environ.get("DISCORD_BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("DISCORD_BOT_TOKEN is not set")

    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    data = json.dumps(message, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bot {bot_token}",
            "User-Agent": DISCORD_UA,
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        print("DISCORD_HTTPERROR(SEND_MESSAGE)", e.code, e.reason)
        print("DISCORD_HTTPERROR_BODY(SEND_MESSAGE)", err_body)
        raise

def discord_edit_message_bot(channel_id: str, message_id: str, message: dict):
    bot_token = os.environ.get("DISCORD_BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("DISCORD_BOT_TOKEN is not set")

    url = f"https://discord.com/api/v10/channels/{channel_id}/messages/{message_id}"
    data = json.dumps(message, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bot {bot_token}",
            "User-Agent": DISCORD_UA,
        },
        method="PATCH",
    )

    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        print("DISCORD_HTTPERROR(EDIT_MESSAGE)", e.code, e.reason)
        print("DISCORD_HTTPERROR_BODY(EDIT_MESSAGE)", err_body)
        raise

def invoke_worker_async(payload: dict, context):
    # 自分自身のARNで確実にinvoke（関数名ミス回避）
    fn_arn = context.invoked_function_arn
    job = {"job": "event_create_worker", "payload": payload}

    print("INVOKE_WORKER ->", fn_arn)

    lambda_client.invoke(
        FunctionName=fn_arn,
        InvocationType="Event",  # 非同期
        Payload=json.dumps(job, ensure_ascii=False).encode("utf-8"),
    )
#使ってない
def build_followup_event_message(title: str, event_id: str):
    return {
        "content": f"📣 **イベント参加募集**\n**{title}**\n\n参加する人は下のボタンを押してね！",
        "components": [
            {
                "type": 1,
                "components": [
                    {
                        "type": 2,
                        "style": 1,
                        "label": "参加する",
                        "custom_id": f"join_event:{event_id}",
                    }
                ],
            }
        ],
    }

def build_recruit_message(
    title: str,
    event_id: str,
    members: list[str],
    status: str = "OPEN",
    start_at: str | None = None,
    ):
    lines = "\n".join([f"- {name}" for name in members]) if members else "- まだいません"
    closed = (status != "OPEN")

    content = (
        f"📣 イベント参加募集\n"
        f"{_decorate_title(title)}\n"
    )

    if start_at:
        content += f"📅 **日時**: {start_at}\n"
    
    content += f"\n**参加者**\n{lines}\n\n"

    if closed:
        content += "🔒 **締切済み**\n"
    else:
        content += "参加/取消は下のボタンを押してね！"

    join_btn = {
        "type": 2,
        "style": 1,
        "label": "参加する",
        "custom_id": f"join_event:{event_id}",
        "disabled": closed,
    }
    leave_btn = {
        "type": 2,
        "style": 4,
        "label": "参加取消",
        "custom_id": f"leave_event:{event_id}",
        "disabled": False,
    }
    close_btn = {
        "type": 2,
        "style": 2,
        "label": "締切",
        "custom_id": f"close_event:{event_id}",
        "disabled": closed,
    }
    notice_open_btn = {
        "type": 2,
        "style":2,
        "label":"連絡を作成",
        "custom_id": f"notice_open:{event_id}",
        "disabled": False,
    }
    notice_list_btn = {
        "type": 2,
        "style":2,
        "label":"連絡一覧",
        "custom_id": f"notice_list:{event_id}",
        "disabled": False,
    }
    return {
        "content":content,
        "components": [
            {"type": 1, "components": [join_btn, leave_btn, close_btn]},
            {"type": 1, "components": [notice_open_btn, notice_list_btn]}
        ],
    }

def refresh_recruit_message(guild_id: str, event_id: str):
    events_table, members_table, _, _ = _get_tables()

    ev = events_table.get_item(
        Key={"guild_id": guild_id, "event_id": event_id},
        ConsistentRead=True,
    ).get("Item")

    if not ev:
        print("EVENT_NOT_FOUND:", guild_id, event_id)
        return

    recruit_channel_id = ev.get("recruit_channel_id") or ev.get("channel_id")
    recruit_message_id = ev.get("recruit_message_id") or ev.get("announce_message_id")
    title = ev.get("title") or "(no title)"

    if not recruit_channel_id or not recruit_message_id:
        print("RECRUIT_IDS_MISSING:", recruit_channel_id, recruit_message_id)
        return

    resp = members_table.query(
        KeyConditionExpression=Key("guild_id").eq(guild_id)
        & Key("member_key").begins_with(f"{event_id}#USER#")
    )
    items = resp.get("Items") or []
    items.sort(key=lambda x: x.get("joined_at") or "")
    member_names = [it.get("username") or it.get("user_id") for it in items]

    status = ev.get("status") or "OPEN"
    start_at = ev.get("event_start_at")
    if start_at:
        start_at = start_at.replace("T", " ")[:16]

    new_msg = build_recruit_message(title, event_id, member_names,start_at=start_at, status=status)
    discord_edit_message_bot(recruit_channel_id, recruit_message_id, new_msg)

def build_notice_message(guild_id: str, notice: dict, ack_count: int, member_count: int):
    title = notice.get("title") or "(no title)"
    body = notice.get("body") or ""
    status = notice.get("status") or "OPEN"

    content = (
        f"📣 **連絡**\n"
        f"**{title}**\n\n"
        f"{body}\n\n"
        f"✅ 確認済み: **{ack_count} / {member_count}**\n"
    )

    if status != "OPEN":
        content += "🔒 **確認受付は終了しました**\n"

    components = []
    if status == "OPEN":
        components = [
            {
                "type": 1,
                "components": [
                    {
                        "type": 2,
                        "style": 3,
                        "label": "確認しました",
                        "custom_id": f"notice_ack:{notice['notice_id']}",
                    }
                ],
            }
        ]

    return {"content": content, "components": components}

def _discord_message_link(guild_id: str, channel_id: str, message_id: str):
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"

# 連絡一覧をehemeralで表示したり表示/非表示ボタンを追加したり
def build_notice_list_ephemeral(guild_id: str, event_id: str, notices: list[dict]):
    visible = [n for n in notices if not n.get("is_hidden")]
    hidden = [n for n in notices if n.get("is_hidden")]

    lines = [f"📜 **連絡一覧**（このイベントのみ）"]
    if not notices:
        lines.append("（連絡はまだありません）")

    components = []

    def add_notice_row(n: dict):
        nid = n["notice_id"]
        title = n.get("title") or "(no title)"
        status = n.get("status") or "OPEN"
        is_hidden = bool(n.get("is_hidden"))

        row = {"type": 1, "components": []}

        ch = n.get("notice_channel_id") or n.get("channel_id")
        mid = n.get("notice_message_id") or n.get("message_id")
        if ch and mid:
            row["components"].append({
                "type": 2,
                "style": 5,
                "label": "開く",
                "url": _discord_message_link(guild_id, ch, mid),
            })

        if is_hidden:
            row["components"].append({
                "type": 2,
                "style": 2,
                "label": "再表示",
                "custom_id": f"notice_show:{nid}",
            })
        else:
            # OPENだけclose可能
            if status == "OPEN":
                row["components"].append({
                    "type": 2,
                    "style": 2,
                    "label": "close",
                    "custom_id": f"notice_close:{nid}",
                })
            row["components"].append({
                "type": 2,
                "style": 2,
                "label": "非表示",
                "custom_id": f"notice_hide:{nid}",
            })

        return row

    if visible:
        lines.append("\n**表示中**")
        for n in visible[:10]:
            lines.append(f"- {n.get('title') or '(no title)'} ({n.get('status') or 'OPEN'})")
            components.append(add_notice_row(n))

    if hidden:
        lines.append("\n**非表示中**")
        for n in hidden[:10]:
            lines.append(f"- {n.get('title') or '(no title)'} ({n.get('status') or 'OPEN'})")
            components.append(add_notice_row(n))

    return {
        "type": 4,
        "data": {
            "flags": 64,
            "content": "\n".join(lines),
            "components": components[:5]  # Discordはcomponents上限があるので最小は5行まで
        }
    }


# =========
# Slash command parsing
# =========

def _options_to_dict(options_list):
    if not options_list:
        return {}
    d = {}
    for opt in options_list:
        name = opt.get("name")
        if "value" in opt:
            d[name] = opt["value"]
        else:
            d[name] = opt.get("options")
    return d

def get_title_from_command(payload):
    data = payload.get("data") or {}
    options = data.get("options") or []
    for opt in options:
        if opt.get("name") == "create":
            subopts = opt.get("options") or []
            sub = _options_to_dict(subopts)
            title = sub.get("title")
            if isinstance(title, str) and title.strip():
                return title.strip()
    return None

#いったんスキャンする方で運用 いずれGSIで設計
def query_notices_by_event(guild_id: str, event_id: str, include_hidden: bool = True):
    _, _, notices_table, _ = _get_tables()

    resp = notices_table.query(
            IndexName="gsi_event",
            KeyConditionExpression=
                Key("guild_id").eq(guild_id)
                & Key("event_sk").begins_with(f"{event_id}#"),
        )
    items = resp.get("Items") or []

    if not include_hidden:
        items = [it for it in items if not it.get("is_hidden")]

    # 新しい順にしたいなら（event_skに created_at が入ってる前提）
    items.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return items

def get_open_notice(guild_id: str, event_id: str):
    items = query_notices_by_event(guild_id, event_id, include_hidden=True)
    for it in items:
        if (it.get("status") or "OPEN") == "OPEN" :
            return it
    return None

def count_event_members(guild_id: str, event_id: str):
    _, members_table, _, _ = _get_tables()
    resp = members_table.query(
        KeyConditionExpression=Key("guild_id").eq(guild_id)
        & Key("member_key").begins_with(f"{event_id}#USER#")
    )
    return len(resp.get("Items") or [])

def has_event_member(guild_id: str, event_id: str, user_id: str):
    _, members_table, _, _ = _get_tables()
    resp = members_table.get_item(
        Key={"guild_id": guild_id, "member_key": f"{event_id}#USER#{user_id}"},
        ConsistentRead=True,
    )
    return "Item" in resp

def get_notice_item(guild_id: str, notice_id: str):
    _, _, notices_table, _ = _get_tables()
    resp = notices_table.get_item(
        Key={"guild_id": guild_id, "notice_id": notice_id},
        ConsistentRead=True,
    )
    return resp.get("Item")

def get_notice_channel_id_from_command(payload):
    data = payload.get("data") or {}
    options = data.get("options") or []
    for opt in options:
        if opt.get("name") == "create":
            subopts = opt.get("options") or []
            sub = _options_to_dict(subopts)
            ch = sub.get("notice_channel")  # type=7(Channel) の value は channel_id
            if ch:
                return str(ch)
    return None

def get_create_options_from_command(payload):
    data = payload.get("data") or {}
    options = data.get("options") or []
    for opt in options:
        if opt.get("name") == "create":
            subopts = opt.get("options") or []
            sub = _options_to_dict(subopts)
            title = sub.get("title")
            notice_channel_id = sub.get("notice_channel")  # ★ここが channel_id になる
            start_at = sub.get("start_at")
            if isinstance(title, str):
                title = title.strip()
            if isinstance(start_at, str):
                start_at = start_at.strip()
            return title, notice_channel_id, start_at
    return None, None

def count_notice_acks(guild_id: str, notice_id: str) -> int:
    _, _, _, acks_table = _get_tables()
    resp = acks_table.query(
        KeyConditionExpression=Key("guild_id").eq(guild_id)
        & Key("ack_key").begins_with(f"{notice_id}#USER#")
    )
    return len(resp.get("Items") or [])

def get_join_user_ids(guild_id: str, event_id: str) -> set[str]:
    _, members_table, _, _ = _get_tables()
    prefix = f"{event_id}#USER#"
    res = members_table.query(
        KeyConditionExpression=
            Key("guild_id").eq(guild_id) &
            Key("member_key").begins_with(prefix)
    )
    return {
        item["member_key"][len(prefix):]
        for item in res.get("Items", [])
        if item.get("member_key", "").startswith(prefix)
    }    
def get_acked_user_ids(guild_id: str, notice_id: str) -> set[str]:
    """
    NoticeAcks から ack 済み（確認済み）ユーザーID集合を取得する
    想定:
      PK: guild_id
      SK: ack_key = "{notice_id}#USER#{user_id}"
    """
    _, _, _, acks_table = _get_tables()

    prefix = f"{notice_id}#USER#"

    res = acks_table.query(
        KeyConditionExpression=
            Key("guild_id").eq(guild_id) &
            Key("ack_key").begins_with(prefix)
    )
    items = res.get("Items", []) or []
    out = set()
    return {
        item["ack_key"][len(prefix):]
        for item in res.get("Items", [])
        if item.get("ack_key", "").startswith(prefix)
    }

def get_unacked_user_ids(guild_id: str, event_id: str, notice_id: str) -> list[str]:
    join_users = get_join_user_ids(guild_id, event_id)
    acked_users = get_acked_user_ids(guild_id, notice_id)

    print("JOIN_USERS =", join_users)
    print("ACKED_USERS =", acked_users)
    print("UNACKED =", join_users - acked_users)
    
    unacked_users = join_users - acked_users
    return sorted(unacked_users)

# =========
# Worker: Event create
# =========

def handle_event_create_deferred(payload):
    events_table, _, _, _ = _get_tables()

    app_id = payload.get("application_id")
    token = payload.get("token")

    guild_id = payload.get("guild_id")
    channel_id = (payload.get("channel") or {}).get("id")  # 募集メッセージ投稿先（コマンド打ったチャンネル）

    member = payload.get("member") or {}
    user = (member.get("user") or {})
    user_id = user.get("id")
    username = user.get("username")

    title, notice_channel_id, start_at_raw = get_create_options_from_command(payload)  # ★これ1本でOK

    print("CREATE title =", title)
    print("CREATE channel_id(recruit) =", channel_id)
    print("CREATE notice_channel_id =", notice_channel_id)
    print("CREATE start_at =", start_at_raw)
    if not title:
        discord_followup(app_id, token, {"content": "title が取得できなかった…（コマンド定義を確認してね）"})
        return
    if not notice_channel_id:
        discord_followup(app_id, token, {"content": "notice_channel を選択してね"})
        return
    if not channel_id:
        discord_followup(app_id, token, {"content": "channel_id が取得できなかった…"})
        return
    if not start_at_raw:
        discord_followup(app_id, token, {"content": "start_at が不正です"})
        return

    event_id = f"EVT#{uuid.uuid4().hex}"

    # ① start_at_raw（文字列）→ datetime（JST）
    start_at_dt = _parse_jst_state_at(start_at_raw)
    if not start_at_dt:
        discord_followup(app_id, token, {"content": "start_at の形式が不正です"})
        return

    # ② 保存用（ISO文字列）
    event_start_at = start_at_dt.isoformat()

    # ③ 前日リマインド
    remind_at_dt = start_at_dt - timedelta(days=1)
    

    # DynamoDB保存
    events_table.put_item(
        Item={
            "guild_id": guild_id,
            "event_id": event_id,
            "title": title,
            "created_by": user_id,
            "created_by_name": username,
            "created_at": _now_iso(),
            "status": "OPEN",
            # 募集投稿先
            "recruit_channel_id": channel_id,
            # 連絡投稿先（選択したチャンネル）
            "notice_channel_id": notice_channel_id,
            # イベント日時
            "event_start_at": event_start_at,
            # 1日前リマインド予定？？？
            "event_remind_at": remind_at_dt.isoformat(),
        }
    )

    # 募集メッセージ投稿
    msg = build_recruit_message(title, event_id, members=[], start_at=start_at_raw, status="OPEN")
    sent = discord_send_message_bot(channel_id, msg)
    message_id = sent.get("id")
    print("RECRUIT message_id:", message_id)

    # recruit_message_id を保存
    events_table.update_item(
        Key={"guild_id": guild_id, "event_id": event_id},
        UpdateExpression="SET recruit_message_id = :mid",
        ExpressionAttributeValues={":mid": message_id},
    )
    # 1日前リマインドを Scheduler に登録
    # Scheduler が Lambda を invoke するためのロールARN（環境変数で渡す）
    scheduler_role_arn = os.environ["SCHEDULER_ROLE_ARN"]
    if not scheduler_role_arn:
        print("SCHEDULER_ROLE_ARN is not set (skip schedule)")
        return
    schedule_name = f"evt-remind-{guild_id}-{event_id[-8:]}"
    target_lambda_arn = os.environ.get("TARGET_LAMBDA_ARN") 
    #↑ function ARNを入れるのが理想。未設定なら後述の注意参照。

    job_input = {
        "job": "event_remind",
        "guild_id": guild_id,
        "event_id": event_id,
    }
    print("TARGET_LAMBDA_ARN(env) =", os.environ.get("TARGET_LAMBDA_ARN"))
    print("target_lambda_arn(var) =", target_lambda_arn)
    try:
        scheduler.create_schedule(
            Name=schedule_name,
            ScheduleExpression=_scheduler_at_expr(remind_at_dt),
            ScheduleExpressionTimezone="Asia/Tokyo",
            FlexibleTimeWindow={"Mode":"OFF"},
            Target={
                "Arn":os.environ["TARGET_LAMBDA_ARN"],
                "RoleArn": scheduler_role_arn,
                "Input":json.dumps(job_input, ensure_ascii=False),
            },
        )
        events_table.update_item(
            Key={"guild_id": guild_id, "event_id": event_id},
            UpdateExpression="SET event_remind_schedule_name=:n",
            ExpressionAttributeValues={":n": schedule_name},
        )
        print("SCHEDULE_CREATED:", schedule_name, "at",remind_at_dt.isoformat())
    except Exception as e:
        import traceback
        print("SCHEDULE_CREATE_ERROR:", repr(e))
        print(traceback.format_exc())

def handle_event_remind(payload: dict):
    events_table,members_table, _, _ = _get_tables()
    guild_id = payload["guild_id"]
    event_id = payload["event_id"]
    
    ev = events_table.get_item(
        Key={"guild_id": guild_id, "event_id": event_id},
        ConsistentRead=True
    ).get("Item")
    if not ev:
        print("REMIND_EVENT_NOT_FOUND:", guild_id, event_id)
        return
    title = ev.get("title") or "(no title)"
    channel_id = ev.get("notice_channel_id") #リマインドはnoticeを設定したチャンネルに送られる
    if not channel_id:
        print("REMIND_CHANNEL_MISSING")
        return

    # join者一覧
    resp = members_table.query(
        KeyConditionExpression=Key("guild_id").eq(guild_id)
        & Key("member_key").begins_with(f"{event_id}#USER#")
    )
    items = resp.get("Items") or []
    user_ids = [it.get("user_id") for it in items if it.get("user_id")]
    if not user_ids:
        print("REMIND_NO_MEMBERS")
        return
    
    mentions = " ".join([f"<@{uid}>" for uid in user_ids])
    msg = {"content": f"🔔 明日です！ **{title}**\n{mentions}"}
    discord_send_message_bot(channel_id, msg)
    print("REMIND_SENT:", event_id, "count=", len(user_ids))

def upsert_notice_remind_schedule(*, guild_id: str, notice_id: str, event_id: str, notice_channel_id: str, remind_at_dt: datetime):
    name = _notice_remind_schedule_name(guild_id, notice_id)

    payload = {
        "kind": "notice_remind",
        "guild_id": guild_id,
        "event_id": event_id,
        "notice_id": notice_id,
        "notice_channel_id": notice_channel_id,
    }

    params = dict(
        Name=name,
        ScheduleExpression=_scheduler_at_expr(remind_at_dt),
        ScheduleExpressionTimezone="Asia/Tokyo",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": os.environ["TARGET_LAMBDA_ARN"],
            "RoleArn": os.environ["SCHEDULER_ROLE_ARN"],
            "Input": json.dumps(payload, ensure_ascii=False),
        },
    )

    try:
        scheduler.create_schedule(**params)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("ConflictException",):
            scheduler.update_schedule(**params)
        else:
            raise

    return name

def delete_notice_remind_schedule(guild_id: str, notice_id: str):
    name = _notice_remind_schedule_name(guild_id, notice_id)
    try:
        scheduler.delete_schedule(Name=name)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("ResourceNotFoundException",):
            return
        raise

def handle_notice_remind(event: dict):
    """
    Scheduler から呼ばれる:
    {
      "kind": "notice_remind",
      "guild_id": "...",
      "event_id": "...",
      "notice_id": "...",
      "notice_channel_id": "..."
    }
    """
    guild_id = event.get("guild_id")
    event_id = event.get("event_id")
    notice_id = event.get("notice_id")
    notice_channel_id = event.get("notice_channel_id")

    if not guild_id or not event_id or not notice_id or not notice_channel_id:
        print("[notice_remind] missing fields:", event)
        return {"ok": False, "reason": "missing fields"}

    # 念のため Notice が OPEN か確認（close 済みなら何もしない）
    events_table, members_table, notices_table, acks_table = _get_tables()
    notice_item = notices_table.get_item(
        Key={"guild_id": guild_id, "notice_id": notice_id},
        ConsistentRead=True
    ).get("Item")

    if not notice_item:
        print("[notice_remind] notice not found:", guild_id, notice_id)
        return {"ok": True, "reason": "notice not found"}

    if notice_item.get("status") != "OPEN":
        print("[notice_remind] notice not OPEN -> skip:", notice_item.get("status"))
        return {"ok": True, "reason": "notice not open"}

    unacked = get_unacked_user_ids(guild_id, event_id, notice_id)
    if not unacked:
        print("[notice_remind] no unacked -> skip")
        return {"ok": True, "reason": "no unacked"}

    mentions = " ".join([f"<@{uid}>" for uid in unacked])
    # 連絡メッセージへのリンク生成
    notice_link = _discord_message_link(
        guild_id,
        notice_channel_id,
        notice_item.get("notice_message_id"),
    )

    title = notice_item.get("title") or "連絡"

    # メッセージ内容（好みで調整OK）
    msg = {
        "content": (
            f"📣 **連絡確認リマインド**\n\n"
            f"**「{title}」** が未確認です。\n"
            f"こちらから確認してください👇\n"
            f"{notice_link}\n\n"
            f"未確認の方：\n{mentions}"
        )
    }

    sent = discord_send_message_bot(notice_channel_id, msg)
    print("[notice_remind] sent:", sent.get("id"))

    return {"ok": True, "unacked_count": len(unacked)}


# =========
# Lambda entry
# =========

def lambda_handler(event, context):
    kind = (event or {}).get("kind")

    if kind == "notice_remind":
        return handle_notice_remind(event)

    # ===== 非同期ワーカー =====
    if isinstance(event, dict) and event.get("job") in ("event_create_worker", "event_remind"): 
        print("WORKER_START")
        payload = event.get("payload") or event
        try:
            job = event.get("job")
            if job == "event_create_worker":
                handle_event_create_deferred(payload)
            elif job == "event_remind":
                handle_event_remind(payload)
            print("WORKER_DONE")
            return {"ok": True}
        except Exception as e:
            import traceback
            print("WORKER ERROR:", repr(e))
            print(traceback.format_exc())
            return {"ok": False}

    # ===== Discord Interaction =====
    headers = event.get("headers") or {}
    raw_body = _parse_body(event)

    ok, err = _verify_discord_request(headers, raw_body)
    if not ok:
        return _resp({"error": err}, 401)

    payload = json.loads(raw_body) if raw_body else {}
    itype = payload.get("type")
    print("ITYPE =", payload.get("type"))
    print("CUSTOM_ID =", ((payload.get("data") or {}).get("custom_id")))

    # ---- PING ----
    if itype == 1:
        return _resp({"type": 1}, 200)

    # ---- Slash command ----
    if itype == 2:
        data = payload.get("data") or {}
        name = data.get("name")

        if name == "ping":
            return _resp({"type": 4, "data": {"content": "pong"}}, 200)

        if name == "event":
            #ack = defer_ephemeral()
            try:
                invoke_worker_async(payload, context)
                return _resp(
                    {"type": 4, "data": {"flags": 64, "content": "✅ イベントを作成しました！"}},
                    200
                )
            except Exception as e:
                import traceback
                print("INVOKE_WORKER_ERROR:", repr(e))
                print(traceback.format_exc())
                return _resp(
                    {"type": 4, "data": {"flags": 64, "content": "{❌ 作成に失敗しました（ログ確認）"}},
                    200
                )
    # ---- Modal(記入フォーム) submit ----
    if itype == 5:
        data = payload.get("data") or {}
        custom_id = data.get("custom_id") or ""
        k, v = _split_custom_id(custom_id)

        guild_id = payload.get("guild_id")
        member = payload.get("member") or {}
        user = member.get("user") or {}
        user_id = user.get("id")
        username = user.get("username")

        events_table, members_table, notices_table, acks_table = _get_tables()

        if k != "notice_modal":
            return _resp({"type": 4, "data": {"flags": 64, "content": "Unknown modal"}}, 200)

        event_id = v

        ev = events_table.get_item(
            Key={"guild_id": guild_id, "event_id": event_id},
            ConsistentRead=True
        ).get("Item")
        if not ev:
            return _resp({"type": 4, "data": {"flags": 64, "content": "❌ イベントが見つかりません"}}, 200)

        if ev.get("created_by") and ev["created_by"] != user_id:
            return _resp({"type": 4, "data": {"flags": 64, "content": "⛔ 作成できるのはイベント作成者だけです"}}, 200)

        # OPEN notice は1つだけ
        open_notice = get_open_notice(guild_id, event_id)
        if open_notice:
            return _resp({"type": 4, "data": {"flags": 64, "content": "⚠️ OPEN中の連絡があります。closeしてから作成してください。"}}, 200)

        # modal values 抽出
        comps = data.get("components") or []
        values = {}
        for row in comps:
            for c in row.get("components") or []:
                values[c.get("custom_id")] = c.get("value")

        title = (values.get("title") or "").strip()
        body = (values.get("body") or "").strip()
        remind_at_str = (values.get("remind_at") or "").strip()

        if not title or not body:
            return _resp({"type": 4, "data": {"flags": 64, "content": "❌ タイトルと本文は必須です"}}, 200)

        remind_at_dt = _parse_jst_state_at(remind_at_str)
        if remind_at_str and not remind_at_dt:
            return _resp({
                "type": 4,
                "data": {
                    "flags": 64,
                    "content": "❌ remind_at は `YYYY-MM-DD HH:MM` (JST) で入力してね。例: 2026-01-18 21:00"
                }
            }, 200)

        notice_channel_id = ev.get("notice_channel_id")
        if not notice_channel_id:
            return _resp({"type": 4, "data": {"flags": 64, "content": "❌ notice_channel_id が未設定です"}}, 200)

        # ここから “1本道”
        created_at = _now_iso()
        notice_id = f"NTC#{uuid.uuid4().hex}"
        event_sk = f"{event_id}#{created_at}#{notice_id}"

        # (A) 先に notice_item を必ず作る
        notice_item = {
            "guild_id": guild_id,
            "notice_id": notice_id,
            "event_id": event_id,
            "event_sk": event_sk,              # GSI用
            "status": "OPEN",
            "is_hidden": False,
            "notice_channel_id": notice_channel_id,
            "notice_message_id": None,         # 後でupdate
            "title": title,
            "body": body,
            "created_by": user_id,
            "created_by_name": username,
            "created_at": created_at,
        }

        # (B) DDB 作成（まだmessage_id無し）
        notices_table.put_item(Item=notice_item)

        # (B2) remind_at があれば Scheduler 作成/更新
        if remind_at_dt:
            schedule_name = upsert_notice_remind_schedule(
                guild_id=guild_id,
                notice_id=notice_id,
                event_id=event_id,
                notice_channel_id=notice_channel_id,
                remind_at_dt=remind_at_dt,
            )
            notices_table.update_item(
                Key={"guild_id": guild_id, "notice_id": notice_id},
                UpdateExpression="SET remind_schedule_name=:sn, remind_at=:ra",
                ExpressionAttributeValues={
                    ":sn": schedule_name,
                    ":ra": remind_at_dt.isoformat(),
                },
            )

        # (C) 参加者数 → メッセージ生成 → Discord投稿（1回だけ）
        member_count = count_event_members(guild_id, event_id)
        msg = build_notice_message(guild_id, notice_item, ack_count=0, member_count=member_count)
        sent = discord_send_message_bot(notice_channel_id, msg)
        message_id = sent.get("id")

        # (D) message_id をDDBへ反映
        notices_table.update_item(
            Key={"guild_id": guild_id, "notice_id": notice_id},
            UpdateExpression="SET notice_message_id=:mid",
            ExpressionAttributeValues={":mid": message_id},
        )

        return _resp({"type": 4, "data": {"flags": 64, "content": "✅ 連絡を投稿しました！"}}, 200)

    # ---- Button / Component ----
    if itype == 3:
        
        data = payload.get("data") or {}
        custom_id = data.get("custom_id") or ""

        guild_id = payload.get("guild_id")
        member = payload.get("member") or {}
        user = member.get("user") or {}
        user_id = user.get("id")
        username = user.get("username")

        events_table, members_table, notices_table, acks_table = _get_tables()

        # notice
        k, v = _split_custom_id(custom_id)

    # ===== Notice: open -> modal =====
        if k == "notice_open":
            event_id = v

            ev = events_table.get_item(
                Key={"guild_id": guild_id, "event_id": event_id},
                ConsistentRead=True
            ).get("Item")
            if not ev:
                return _resp({"type": 4, "data": {"flags": 64, "content": "❌ イベントが見つかりません"}}, 200)

            # 作成者限定
            if ev.get("created_by") and ev["created_by"] != user_id:
                return _resp({"type": 4, "data": {"flags": 64, "content": "⛔ 連絡を作れるのはイベント作成者だけです"}}, 200)

            # OPEN notice は1つだけ
            open_notice = get_open_notice(guild_id, event_id)
            if open_notice:
                return _resp({"type": 4, "data": {"flags": 64, "content": "⚠️ OPEN中の連絡があります。closeしてから作成してください。"}}, 200)

            modal = {
                "type": 9,
                "data": {
                    "custom_id": f"notice_modal:{event_id}",
                    "title": "連絡を作成",
                    "components": [
                        {"type": 1, "components": [
                            {"type": 4, "custom_id": "title", "style": 1, "label": "タイトル", "required": True, "max_length": 100}
                        ]},
                        {"type": 1, "components": [
                            {"type": 4, "custom_id": "body", "style": 2, "label": "本文", "required": True, "max_length": 1000}
                        ]},
                        {"type": 1, "components": [
                            {"type": 4, "custom_id": "remind_at", "style": 1, "label": "リマインド時刻(JST)", "required": False, "max_length": 16, "placeholder": "例: 2026-01-18 21:00" }
                        ]}
                    ],
                },
            }

            return _resp(modal, 200)

        # ===== Notice: list (ephemeral) =====
        if k == "notice_list":
            event_id = v
            items = query_notices_by_event(guild_id, event_id, include_hidden=True)
            msg = build_notice_list_ephemeral(guild_id, event_id, items)
            return _resp(msg, 200)

        # ===== Notice: close/hide/show =====
        if k in ("notice_close", "notice_hide", "notice_show"):
            notice_id = v
            notice = get_notice_item(guild_id, notice_id)
            if not notice:
                return _resp({"type": 4, "data": {"flags": 64, "content": "❌ 連絡が見つかりません"}}, 200)

            event_id = notice.get("event_id")
            ev = events_table.get_item(Key={"guild_id": guild_id, "event_id": event_id}, ConsistentRead=True).get("Item")
            if not ev:
                return _resp({"type": 4, "data": {"flags": 64, "content": "❌ イベントが見つかりません"}}, 200)

            # 作成者限定（まずはイベント作成者のみで統一）
            if ev.get("created_by") and ev["created_by"] != user_id:
                return _resp({"type": 4, "data": {"flags": 64, "content": "⛔ 操作できるのはイベント作成者だけです"}}, 200)

            if k == "notice_close":
                # CLOSED にする
                notices_table.update_item(
                    Key={"guild_id": guild_id, "notice_id": notice_id},
                    UpdateExpression="SET #st=:c, closed_at=:t",
                    ExpressionAttributeNames={"#st": "status"},
                    ExpressionAttributeValues={":c": "CLOSED", ":t": _now_iso()},
                )
                delete_notice_remind_schedule(guild_id, notice_id)
                notice["status"] = "CLOSED"
                # NoticeメッセージからAckボタンを消す（再描画）
                ack_count = count_notice_acks(guild_id, notice_id)
                member_count = count_event_members(guild_id, event_id)
                new_msg = build_notice_message(guild_id, notice, ack_count, member_count)
                discord_edit_message_bot(notice["notice_channel_id"], notice["notice_message_id"], new_msg)

            elif k == "notice_hide":
                notices_table.update_item(
                    Key={"guild_id": guild_id, "notice_id": notice_id},
                    UpdateExpression="SET is_hidden=:t",
                    ExpressionAttributeValues={":t": True},
                )

            elif k == "notice_show":
                notices_table.update_item(
                    Key={"guild_id": guild_id, "notice_id": notice_id},
                    UpdateExpression="SET is_hidden=:f",
                    ExpressionAttributeValues={":f": False},
                )

            # 操作後は一覧を返す
            items = query_notices_by_event(guild_id, event_id, include_hidden=True)
            msg = build_notice_list_ephemeral(guild_id, event_id, items)
            return _resp(msg, 200)

        # ===== Notice: ack =====
        if k == "notice_ack":
            notice_id = v
            notice = get_notice_item(guild_id, notice_id)
            if not notice:
                return _resp({"type": 4, "data": {"flags": 64, "content": "❌ 連絡が見つかりません"}}, 200)

            event_id = notice.get("event_id")
            if (notice.get("status") or "OPEN") != "OPEN":
                return _resp({"type": 4, "data": {"flags": 64, "content": "🔒 この連絡は確認受付が終了しています"}}, 200)

            # 参加者限定
            if not has_event_member(guild_id, event_id, user_id):
                return _resp({"type": 4, "data": {"flags": 64, "content": "⛔ 確認できるのは参加者のみです"}}, 200)

            # 二重Ack防止（AcksテーブルのSK名は member_key に揃える想定）
            try:
                acks_table.put_item(
                    Item={
                        "guild_id": guild_id,
                        "ack_key": f"{notice_id}#USER#{user_id}",
                        "notice_id": notice_id,
                        "event_id": event_id,
                        "user_id": user_id,
                        "username": username,
                        "acked_at": _now_iso(),
                    },
                    ConditionExpression="attribute_not_exists(ack_key)",
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    return _resp({"type": 4, "data": {"flags": 64, "content": "⚠️ すでに確認済みです"}}, 200)
                raise

            ack_count = count_notice_acks(guild_id, notice_id)
            member_count = count_event_members(guild_id, event_id)
            new_msg = build_notice_message(guild_id, notice, ack_count, member_count)
            channel_id = notice.get("notice_channel_id") or notice.get("channel_id")
            message_id = notice.get("notice_message_id") or notice.get("message_id")

            if not channel_id or not message_id:
                print("NOTICE_KEYS:", list((notice or {}).keys()))
                return _resp({"type": 4, "data": {"flags": 64, "content": "❌ 投稿先/メッセージIDが見つかりません（ログ確認）"}}, 200)

            discord_edit_message_bot(channel_id, message_id, new_msg)

            return _resp({"type": 4, "data": {"flags": 64, "content": "✅ 確認しました！"}}, 200)

        # join_event
        if custom_id.startswith("join_event:"):
            event_id = custom_id.split(":", 1)[1]

            ev = events_table.get_item(Key={"guild_id": guild_id, "event_id": event_id}, ConsistentRead=True).get("Item")
            if not ev:
                return _resp({"type": 4, "data": {"flags": 64, "content": "❌ イベントが見つかりません"}}, 200)

            if (ev.get("status") or "OPEN") != "OPEN":
                return _resp({"type": 4, "data": {"flags": 64, "content": "🔒 このイベントは締切済みです"}}, 200)

            try:
                members_table.put_item(
                    Item={
                        "guild_id": guild_id,
                        "member_key": f"{event_id}#USER#{user_id}",
                        "event_id": event_id,
                        "user_id": user_id,
                        "username": username,
                        "joined_at": _now_iso(),
                    },
                    ConditionExpression="attribute_not_exists(member_key)",
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    return _resp({"type": 4, "data": {"flags": 64, "content": "⚠️ すでに参加しています！"}}, 200)
                raise

            # 募集メッセージ更新
            try:
                refresh_recruit_message(guild_id, event_id)
            except Exception as e:
                import traceback
                print("RECRUIT_REFRESH_ERROR(join):", repr(e))
                print(traceback.format_exc())

            return _resp({"type": 4, "data": {"flags": 64, "content": "✅ 参加を受け付けました！"}}, 200)

        # leave_event
        if custom_id.startswith("leave_event:"):
            event_id = custom_id.split(":", 1)[1]

            # 参加取り消し：該当アイテム削除（存在しなくてもOK）
            members_table.delete_item(
                Key={
                    "guild_id": guild_id,
                    "member_key": f"{event_id}#USER#{user_id}",
                }
            )

            # 募集メッセージ更新(取消)
            try:
                refresh_recruit_message(guild_id, event_id)
            except Exception as e:
                import traceback
                print("RECRUIT_REFRESH_ERROR(leave):", repr(e))
                print(traceback.format_exc())

            return _resp({"type": 4, "data": {"flags": 64, "content": "✅ 参加を取り消しました！"}}, 200)
        
        # close_event
        if custom_id.startswith("close_event:"):
            event_id = custom_id.split(":", 1)[1]

            events_table, _, _, _ = _get_tables()
            
            ev = events_table.get_item(
                Key={"guild_id": guild_id, "event_id": event_id},
                ConsistentRead=True,
            ).get("Item")

            if not ev:
                return _resp({"type": 4, "data": {"flags": 64, "content": "❌ イベントが見つかりません"}}, 200)
            
            # ★ 作成者限定
            created_by = ev.get("created_by")
            if created_by and created_by != user_id:
                return _resp({"type": 4, "data": {"flags": 64, "content": "⛔ 締切できるのはイベント作成者だけです"}}, 200)

            # 二重締め切りガード
            if (ev.get("status") or "OPEN") != "OPEN":
                return _resp({"type": 4, "data": {"flags": 64, "content": "🔒 このイベントは締切済みです"}}, 200)

            # 締切
            events_table.update_item(
                Key={"guild_id": guild_id, "event_id": event_id},
                UpdateExpression="SET #status = :closed",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":closed": "CLOSED"},
            )

            # 募集メッセージ更新(締切)
            try:
                refresh_recruit_message(guild_id, event_id)
            except Exception as e:
                import traceback
                print("RECRUIT_REFRESH_ERROR(close):", repr(e))
                print(traceback.format_exc())

            return _resp({"type": 4, "data": {"flags": 64, "content": "🔒 募集を締め切りました！"}}, 200)

        return _resp({"type": 4, "data": {"flags": 64, "content": "Unknown component"}}, 200)

    # ---- fallback ----
    return _resp({"type": 4, "data": {"content": "Unsupported interaction type"}}, 200)

