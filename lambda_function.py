import json

def lambda_handler(event, context):
    body =  event.get("body") or "{}"
    if isinstance(body, str):
        payload = json.loads(body)
    else:
        payload = body
    
    #discordの疎通確認(PING)
    if payload.get("type") == 1:
        return {"statusCode": 200, "body" : json.dumps({"type": 1})}
    
    #スラッシュコマンドの例(/ping)
    data = payload.get("data", {})
    name = data.get("name")
    
    if name == "ping":
        return {
            "statusCode": 200,
            "body": json.dumps({
                "type": 4,
                "data": {"content": "pong"}
            })
        }
    return {
        "statusCode" : 200,
        "body" :json.dumps({
            "type": 4,
            "data": {"content": "無効なコマンドです"}
        })
    }