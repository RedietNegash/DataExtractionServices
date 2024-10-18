timezone = pytz.timezone("UTC")
current_time = datetime.now(timezone)
last_24_hours = current_time - timedelta(hours=24)
