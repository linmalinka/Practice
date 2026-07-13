UNKNOWN_SLACK_ALERT_ICON = ":question:"


class AlertLevelCode:
    DISASTER = "DISASTER"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class AlertLevelCodeIconSlack:
    DISASTER = ":red_circle:"
    ERROR = ":large_orange_circle:"
    WARNING = ":large_yellow_circle:"
    INFO = ":large_blue_circle:"


def icon_by_alert_level(alert=None) -> str:
    if alert == AlertLevelCode.DISASTER:
        return AlertLevelCodeIconSlack.DISASTER
    elif alert == AlertLevelCode.ERROR:
        return AlertLevelCodeIconSlack.ERROR
    elif alert == AlertLevelCode.WARNING:
        return AlertLevelCodeIconSlack.WARNING
    elif alert == AlertLevelCode.INFO:
        return AlertLevelCodeIconSlack.INFO
    else:
        return UNKNOWN_SLACK_ALERT_ICON
