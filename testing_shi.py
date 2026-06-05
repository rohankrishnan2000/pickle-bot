import re

TIME_ID_BASE = 13
def time_to_id(time_str: str) -> int:
    """Convert a human time like ``"7:30AM"`` into YourCourts' slot id.

    The rest of the codebase uses this to sort slots chronologically and to
    reconstruct a ``startTimeId`` when the reservation form omits it.
    """
    m = re.fullmatch(r"(\d{1,2}):(\d{2})(AM|PM)", time_str.strip().upper())
    if not m:
        return -1

    hour = int(m.group(1))
    minute = int(m.group(2))
    meridiem = m.group(3)
    if hour < 1 or hour > 12 or minute not in (0, 30):
        return -1

    hour_24 = hour % 12
    if meridiem == "PM":
        hour_24 += 12

    minutes = hour_24 * 60 + minute
    first_slot = 7 * 60
    last_slot = 21 * 60 + 30
    if minutes < first_slot or minutes > last_slot:
        return -1

    return TIME_ID_BASE + (minutes - first_slot) // 30


x = time_to_id("12:00PM")