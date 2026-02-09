# Discord Event & Notice Bot (AWS Serverless)
イベント参加者への開催前通知と連絡未確認者への自動リマインドを自動化するDiscordサーバレスBotです。
EventBridge Scheduler と Lambda 非同期ワーカー設計で通知処理を実現しています。

## Demo / 操作イメージ
- `/event create` でイベント募集を投稿（参加/取消/締切ボタン付き）
- 「連絡を作成」→ Modal で連絡投稿（確認ボタン付き）
- 「連絡一覧」→ ephemeral で一覧表示（開く/close/非表示/再表示）
- 未確認者へリマインド（Scheduler → Lambda → Discord投稿）

---

flowchart LR
  Discord[Discord (Slash / Button / Modal)] -->|Interactions| APIGW[API Gateway]
  APIGW --> Lambda[Lambda (Handler + Worker)]

  Lambda --> DDB[(DynamoDB)]
  Lambda --> Scheduler[EventBridge Scheduler]

  Scheduler -->|invoke at(...)| Lambda
  Lambda -->|Bot REST API| DiscordAPI[Discord REST API]
  DiscordAPI --> Discord

---
## Features

### Event
- イベント募集投稿（参加者一覧を自動更新）
- イベント開催日時の指定
- 参加 / 参加取消
- 募集締切（締切後は参加不可）

### Notice
- 連絡投稿（確認ボタンでAck管理）
- 連絡一覧を ephemeral で表示（表示/非表示の切替）
- close（確認受付終了・ボタン削除）

### Reminder System
- イベント開催24時間前に参加者へ自動メンション通知
- 連絡未確認者への個別リマインド
- 指定時刻通知 / 前日通知の両対応
- EventBridge Scheduler → Lambda → Discord投稿
- 手動操作不要の自動運用

---

## Architecture
- Discord Interactions（署名検証: Ed25519）
- API Gateway → Lambda（Lambda Proxy）
- DynamoDB
  - Events / EventMembers / Notices / NoticeAcks
- EventBridge Scheduler
  - リマインド時刻に Lambda を invoke

詳細は `docs/architecture.md` を参照。

---

## Tech Stack
- Python
- AWS Lambda / API Gateway / DynamoDB / EventBridge Scheduler
- Discord API (Interactions + REST)
- PyNaCl（署名検証）

---

## Security
- Discord Interactions の署名検証（x-signature-ed25519 / x-signature-timestamp）
- Bot Token / Public Key 等は **環境変数で管理**（リポジトリには含めません）

---

## Setup (Local / Deployment)
### Environment Variables
`.env.example` を参考に環境変数を設定してください。

必須:
- `DISCORD_PUBLIC_KEY`
- `DISCORD_BOT_TOKEN`
- `DDB_EVENTS_TABLE`
- `DDB_EVENT_MEMBERS_TABLE`
- `DDB_NOTICES_TABLE`
- `DDB_NOTICE_ACKS_TABLE`
- `SCHEDULER_ROLE_ARN`
- `TARGET_LAMBDA_ARN`

### AWS Resources
- DynamoDB テーブル（上記4つ）
- EventBridge Scheduler が Lambda invoke するための IAM Role（`SCHEDULER_ROLE_ARN`）

---

## Design Notes (工夫点)
- Discord Interactions の **3秒制限**に対応するため、重い処理は **非同期ワーカー（同一LambdaをEvent invoke）**で実行
- DynamoDB put_item に `ConditionExpression` を使い、二重参加/二重Ackを防止
- Scheduler は create / update を使い分け、リマインド時刻の再設定に対応
