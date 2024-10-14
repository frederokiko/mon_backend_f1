from datetime import UTC, datetime , timedelta

expire =datetime.now(UTC)+timedelta(hours=2, minutes=30)

print(expire)
