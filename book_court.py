from collections import defaultdict

try:
    import pickle_bot.yourcourts as yourcourts
except ModuleNotFoundError:
    import yourcourts

DEFAULT_DATE = "05/24/2026"


def display_slots(slots: list[dict]) -> list[dict]:
    by_time = defaultdict(list)
    for s in slots:
        by_time[s["time"]].append(s)

    sorted_times = sorted(by_time.keys(), key=lambda t: yourcourts.time_to_id(t))

    print(f"\n{'#':<5} {'Time':<10} {'Court':<10} {'Resource ID'}")
    print("-" * 42)

    numbered = []
    for time_str in sorted_times:
        for s in sorted(by_time[time_str], key=lambda x: x["court"]):
            numbered.append(s)
            print(f"{len(numbered):<5} {s['time']:<10} {s['court']:<10} {s['resource_id']}")

    return numbered


def main():
    session = yourcourts.make_session()

    print("Logging in...")
    if not yourcourts.login(session):
        print("Login failed — check credentials.")
        return
    print("Logged in.")

    date = input(f"Reservation date [{DEFAULT_DATE}]: ").strip() or DEFAULT_DATE

    print(f"\nFetching schedule for {date}...")
    slots = yourcourts.find_slots(session, date)
    if not slots:
        print("No available slots found.")
        return

    numbered = display_slots(slots)
    print(f"\n{len(numbered)} available slots.")

    while True:
        choice = input("\nEnter slot # to book (or 'q' to quit): ").strip()
        if choice.lower() == "q":
            print("Bye!")
            return
        try:
            idx = int(choice) - 1
            if not (0 <= idx < len(numbered)):
                print(f"Pick a number between 1 and {len(numbered)}.")
                continue
        except ValueError:
            print("Enter a number or 'q'.")
            continue

        selected = numbered[idx]
        confirm = input(
            f"Book {selected['court']} @ {selected['time']} on {date}? [y/N]: "
        ).strip().lower()
        if confirm == "y":
            ok, msg = yourcourts.book_slot(session, selected, date)
            print(msg)
        else:
            print("Cancelled.")


if __name__ == "__main__":
    main()
