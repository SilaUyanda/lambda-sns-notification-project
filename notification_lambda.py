import os
import json
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from urllib.error import HTTPError, URLError

import boto3

BASE_URL = "https://api.football-data.org/v4"

COMPETITIONS = [
    "PL",
    "PD",
    "SA",
    "DED",
    "PPL",
    "BL1",
    "DSU",
    "TSU",
]

WATCH_TEAMS = {
    "Manchester City FC",
    "Liverpool FC",
    "Newcastle United FC",
    "FC Barcelona",
    "Real Madrid CF",
    "Atlético de Madrid",
    "FC Internazionale Milano",
    "SL Benfica",
    "PSV",
    "N.E.C.",
    "FC Midtjylland",
    "FC Bayern München",
    "Galatasaray SK",
}

sns = boto3.client("sns")

def http_get(path: str, params=None) -> dict:
    token = os.environ.get("FOOTBALL_DATA_TOKEN")
    if not token:
        raise RuntimeError("FOOTBALL_DATA_TOKEN not configured")

    query = ""
    if params:
        query = "?" + urllib.parse.urlencode(params)

    url = f"{BASE_URL}{path}{query}"

    request = urllib.request.Request(
        url,
        headers={
            "X-Auth-Token": token,
            "Accept": "application/json",
        },
        method="GET",
    )

    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))

def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)

def format_email(matches: list) -> str:
    if not matches:
        return "No matches found for today/tomorrow."

    lines = []
    for i, m in enumerate(matches, start=1):
        lines.append(
            f"{i}. {m['home']} vs {m['away']} | {m['competition']} | {m['kickoff_utc']}"
        )
    return "\n".join(lines)

def publish_email(subject: str, message: str):
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not topic_arn:
        raise RuntimeError("SNS_TOPIC_ARN not configured")

    sns.publish(
        TopicArn=topic_arn,
        Subject=subject[:100],  # SNS subject limit
        Message=message
    )

def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    end_of_tomorrow = (now + timedelta(days=1)).replace(
        hour=23, minute=59, second=59, microsecond=0
    )

    results = []

    for competition in COMPETITIONS:
        try:
            data = http_get(
                f"/competitions/{competition}/matches",
                {"status": "SCHEDULED"},
            )
        except (HTTPError, URLError):
            continue

        matches = data.get("matches", [])

        for match in matches:
            home = (match.get("homeTeam") or {}).get("name")
            away = (match.get("awayTeam") or {}).get("name")
            utc_date = match.get("utcDate")

            if not home or not away or not utc_date:
                continue

            kickoff = parse_utc(utc_date)

            if not (now <= kickoff <= end_of_tomorrow):
                continue

            if home not in WATCH_TEAMS and away not in WATCH_TEAMS:
                continue

            results.append({
                "competition": (match.get("competition") or {}).get("name"),
                "kickoff_utc": kickoff.strftime("%Y-%m-%d %H:%M UTC"),
                "home": home,
                "away": away,
            })

    results.sort(key=lambda x: x["kickoff_utc"])

    subject = f"Match Alerts (Today/Tomorrow): {len(results)} match(es)"
    message = format_email(results)

    # Send email every run (even if zero)
    publish_email(subject, message)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "count": len(results),
                "matches": results,
            },
            indent=2,
        ),
    }
