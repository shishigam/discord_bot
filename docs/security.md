# Security Design（セキュリティ設計）

このドキュメントでは、Discord Bot「Checky」におけるセキュリティ設計について説明します。

本システムは、Discord上のイベント管理および連絡確認を行うBotであり、以下の観点からセキュリティ対策を実施しています。

---

# 1. Discordリクエスト検証

本システムは、Discordから送信されたリクエストであることを検証しています。

Discord Interactionsでは以下のヘッダーが送信されます。

- `X-Signature-Ed25519`
- `X-Signature-Timestamp`

これらを利用して、Discordが公開している **Public Key** により署名検証を行います。

検証方法：message = timestamp + request_body

Ed25519で署名検証

署名検証に失敗した場合、リクエストは **HTTP 401** として拒否します。

この仕組みにより以下を防止します。

- 不正リクエスト
- リクエスト偽装
- 外部からの直接API実行

---

# 2. Bot Token の保護

Discord Bot Token はソースコードに直接記述せず、  
**AWS Lambdaの環境変数**として管理しています。

使用する環境変数：DISCORD_BOT_TOKEN


このトークンは以下のポリシーで管理します。

- GitHubなどのリポジトリに公開しない
- Lambda環境変数として安全に保管
- クライアント側には公開しない

---

# 3. AWS IAM 最小権限設計

Lambda実行ロールには **最小権限（Least Privilege）** を適用しています。

主な権限：
dynamodb:GetItem,
dynamodb:PutItem,
dynamodb:UpdateItem,
dynamodb:Query,
dynamodb:Scan,
scheduler:CreateSchedule,
scheduler:UpdateSchedule,
scheduler:DeleteSchedule



アクセス対象は **Checkyが使用するリソースのみに限定**しています。

対象リソース：

- DynamoDB テーブル
- EventBridge Scheduler

---

# 4. データ分離（マルチテナント設計）

本システムは複数のDiscordサーバーで使用されることを想定しています。

そのため、すべてのデータに `guild_id` を含めて管理しています。

例：
guild_id,
event_id,
notice_id

これにより以下を防止します。

- 他サーバーのデータ参照
- データ混在

---

# 5. 重複操作の防止

DynamoDBの **条件付き書き込み** を使用して  
重複操作を防止しています。

例：ConditionExpression="attribute_not_exists(ack_key)"

これにより以下を防ぎます。

- 確認ボタンの多重クリック
- 同一ユーザーの複数参加
- データの不整合

---

# 6. メッセージ編集制御

本システムは **Bot自身が投稿したメッセージのみ編集**します。

メッセージIDをDynamoDBに保存し、  
編集時に参照します。

例：recruit_message_id
notice_message_id


これにより、他ユーザーのメッセージを誤って編集することを防ぎます。

---

# 7. EventBridge Scheduler の安全な実行

リマインド機能には **EventBridge Scheduler** を使用しています。

SchedulerからLambdaを呼び出す際には  
専用IAMロールを使用します。

SCHEDULER_ROLE_ARN

このロールは以下の権限のみを持ちます。

lambda:InvokeFunction


これにより、他のAWSリソースへのアクセスを防止します。

---

# 8. Bot機能の利用制限

本システムでは、操作できるユーザーを制限しています。

例：

イベント作成者のみ実行可能

- 連絡作成
- 連絡Close
- 連絡非表示

イベント参加者のみ実行可能

- 確認ボタン

これにより、不正操作を防止します。

---

# 9. 今後のセキュリティ改善

今後、以下のセキュリティ強化を検討しています。

- APIレート制限
- 操作ログ（監査ログ）
- DynamoDBデータ暗号化
- 管理者ロールによる権限管理

---

# まとめ

本システムは以下のセキュリティ対策を実施しています。

- Discord署名検証（Ed25519）
- Bot Tokenの安全管理
- IAM最小権限
- サーバー単位のデータ分離
- DynamoDB条件付き書き込み
- EventBridge Schedulerの安全実行

これにより、安全にDiscordサーバーの連絡管理を行うことができます。
