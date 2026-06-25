# How requests to yourcourts.com are authenticated

This document explains *everything* you need to understand to send a working,
authenticated request to [yourcourts.com](https://www.yourcourts.com) the way
this bot does. It covers cookies, sessions, CSRF tokens, the per-slot
reservation token, and how `requests.Session` ties it all together.

It is written for someone who has never dealt with web auth before, so it
defines each term before using it. Everything here is grounded in the actual
code (`yourcourts.py`) and in captured network traffic (HAR files) from a real
login + booking.

---

## TL;DR

To book a court you need **three different credentials**, and they come from
**three different places**. Only one of them is tied to logging in.

| Credential | What it proves | Where you get it | Is it a cookie? |
|---|---|---|---|
| **Session cookie** | "I am logged-in member 279515" | Returned by the **login POST** | ✅ Yes |
| **Slot `token`** | "This specific open slot is real and reservable" | Parsed from the **schedule page** | ❌ No (URL query param) |
| **`SYNCHRONIZER_TOKEN`** | "This form submission is genuine, not forged" | Scraped from the **reservation form page** | ❌ No (CSRF token in form HTML) |

The session cookie is the part most people mean when they say "auth." The other
two are anti-abuse tokens the server hands you mid-flow.

---

## Part 1: The vocabulary

### HTTP is stateless

When your program sends an HTTP request (`GET`, `POST`, …) to a server, that
request is **self-contained**. The server does not remember anything about your
previous request. Each request arrives like a stranger walking up to a counter:
the server has no idea who you are unless *the request itself* carries proof.

This is the core problem all of the following concepts exist to solve: **how
does request #2 prove it's the same person who logged in on request #1?**

### Header

An HTTP request and response are both made of **headers** (metadata key/value
pairs) and an optional **body** (the payload). Examples of request headers:

```
User-Agent: Mozilla/5.0 ...      <- what kind of client I am
Referer: https://.../showLogin   <- the page I came from
Cookie: JSESSIONID=abc123...      <- my identity token (see below)
```

Headers are how almost all auth information travels.

### Cookie

A **cookie** is a small piece of text the server tells your client to store and
send back on every future request to that site. The mechanism is a pair of
headers:

1. The server replies with a `Set-Cookie` header:
   ```
   Set-Cookie: JSESSIONID=9F3A...; HttpOnly; Secure
   ```
2. Your client stores it, and on every later request to that site automatically
   adds:
   ```
   Cookie: JSESSIONID=9F3A...
   ```

That's it. A cookie is just "a value the server asked me to remember and repeat
back." It solves the stateless problem: the server gives you a cookie when you
log in, and from then on your requests carry it, so the server recognizes you.

Cookie attributes you'll see:
- **`HttpOnly`** — JavaScript in the browser can't read it (protects against
  theft via XSS). Irrelevant to a Python bot, but it's why you won't see the
  cookie in browser dev-tools scripting.
- **`Secure`** — only sent over HTTPS.
- **Expiry** — a cookie can be *session-scoped* (deleted when the browser
  closes) or have an explicit expiry date.

### Session

A **session** is the server-side counterpart to the cookie. When you log in, the
server creates a record in its own memory — "session `9F3A...` belongs to member
279515, logged in at 10:00" — and gives you a cookie containing only the session
**id** (`9F3A...`). Your cookie is just a claim ticket; the real data lives on
the server, keyed by that id.

This is why the cookie value looks like meaningless gibberish: it's not your data,
it's a lookup key into the server's session table.

YourCourts is a Java web application (more on how we know below), so its session
cookie is the classic Java name: **`JSESSIONID`**.

### `requests.Session` (the Python object)

Confusingly, the Python `requests` library has a class *also* called `Session`.
It is **not** the same thing as the server session above — it's a client-side
container. A `requests.Session` object:

- holds a **cookie jar** that automatically stores any `Set-Cookie` the server
  sends and re-sends it as a `Cookie` header on every subsequent request, and
- lets you set default headers (like `User-Agent`) once for all requests.

So the server-side *session* and the client-side `requests.Session` are two
halves of the same handshake: the server issues the cookie, and
`requests.Session` is what remembers and replays it for you. **You never have to
touch cookies by hand** — that's the entire point of using a `Session` object.

In this repo that object is created in `make_session()`:

```python
# yourcourts.py
def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 ... Chrome/148 ..."})
    return session
```

The `User-Agent` is set to look like a real Chrome browser so the site doesn't
treat the bot as a suspicious non-browser client. That's not auth — it's
camouflage.

### CSRF and the CSRF token

**CSRF** = Cross-Site Request Forgery. The attack it prevents: you're logged in
to yourcourts.com (so your browser holds a valid session cookie). You then visit
`evil.com`, which secretly makes your browser POST to
`yourcourts.com/reservation/bookcourtreservation`. Because cookies are sent
*automatically*, that forged request would carry your valid session cookie and
the booking would go through — even though *you* never clicked anything.

The defense is a **CSRF token**: a secret, unpredictable value the server embeds
**in the form HTML** when it hands you a legitimate form. To submit
successfully, you must echo that exact value back in the POST body. `evil.com`
can't read the token (it's on a different origin and can't see yourcourts' HTML),
so it can't forge a valid submission.

In this app the CSRF token is named **`SYNCHRONIZER_TOKEN`**. The name is a
dead giveaway that this is a **Grails / Spring** Java app — "synchronizer token"
is that framework's built-in CSRF pattern. A real captured value:

```
SYNCHRONIZER_TOKEN = ac5dc326-093d-4ddb-a75f-403f3760056c
```

Key properties of the CSRF token:
- It is **not a cookie.** It lives inside the page HTML and is sent in the POST
  **body**, not in a `Cookie` header.
- It is **single-use / per-form.** You must fetch the form page first to get a
  fresh one, then immediately use it. You can't reuse an old one.
- It is **tied to your session.** The server checks that the token you submit
  matches the one it issued to *your* session.

### The per-slot `token` (a different token entirely)

Separate from CSRF, each open reservation slot on the schedule page comes with
its own **`token`** baked into its link. Example from a real schedule page:

```
/reservation/newreservation/?reservableResourceId=7376&facilityId=1535
    &facility_id=&reservationDate=05/24/2026&token=AC2F251ADFMzk2MTZZQ09URT0__
```

This `token` is the server's way of saying "yes, this exact court+date+time
combination was genuinely open when I rendered the page." You can't just guess a
`reservableResourceId` and book it — you have to follow a link the server
itself generated, carrying the matching `token`. It is, again, **not a cookie**;
it's a URL query parameter.

In code, `find_slots()` pulls it straight out of the link:

```python
# yourcourts.py — find_slots()
qs = parse_qs(urlparse(href).query)
slots.append({
    "court": court, "time": slot_time,
    "resource_id": qs.get("reservableResourceId", [""])[0],
    "token": qs.get("token", [""])[0],   # <- the per-slot token
    "href": href,
})
```

---

## Part 2: The actual flow, request by request

Here is the full sequence the bot performs, with which credential each step
produces or consumes. Cross-reference: `yourcourts.py`.

### Step 0 — Build the client

```python
session = make_session()
```
Creates the `requests.Session` with its (empty) cookie jar and the browser-like
`User-Agent`. No network yet.

### Step 1 — Log in (this is where the cookie is born)

```python
# yourcourts.py — login()
session.get(f"{BASE_URL}/login")                    # warm-up GET
resp = session.post(
    f"{BASE_URL}/security/login/form-login",
    data={"username": email, "password": password, "rememberMe": "on"},
    allow_redirects=True,
)
return "showLogin" not in resp.url
```

What happens on the wire (from the captured login HAR):

```
POST /security/login/form-login   ->  302 redirect to /member/index
   body: username=...&password=...&rememberMe=on
GET  /member/index                ->  200  (you're now on your member home page)
```

- The server validates the credentials and replies **302** (a redirect). On that
  response it sets the authenticated **session cookie** via `Set-Cookie`.
  `requests.Session` stores it automatically.
- The redirect target tells you the outcome: success → `/member/index`, failure
  → a URL containing `showLogin`. That's exactly what the `return "showLogin"
  not in resp.url` check is testing.
- **Credentials** (`YC_EMAIL` / `YC_PASSWORD`) come from environment variables /
  `.env`; they are used *only* here, to obtain the cookie.

From this point on, every request through `session` automatically carries the
session cookie, so the server knows who you are. No step below re-sends your
password.

### Step 2 — Find open slots (produces the per-slot `token`)

```python
# yourcourts.py — find_slots()
resp = session.get(f"{BASE_URL}/facility/viewschedule",
                   params={"reservationDate": date, "facility_id": ""})
if "showLogin" in resp.url:
    raise SessionExpired("Redirected to login while fetching schedule")
```

- Carries the session cookie automatically (that's how this member-only page
  loads at all).
- If the cookie has expired, the server redirects you to the login page; the code
  detects the `showLogin` URL and raises `SessionExpired`.
- Parses every open slot's link to extract `reservableResourceId` and the
  per-slot **`token`** (see above).

### Step 3 — Open the reservation form (produces the `SYNCHRONIZER_TOKEN`)

```python
# yourcourts.py — book_slot()
form_url = f"{BASE_URL}{slot['href']}"   # the /reservation/newreservation/?...&token=... link
form_resp = session.get(form_url)
csrf = extract_csrf(form_resp.text)              # SYNCHRONIZER_TOKEN
owner_id = extract_field(form_resp.text, "ownerId")
start_time_id = extract_field(form_resp.text, "startTimeId")
```

- GET-ing the form page (using the slot's `token`) returns HTML containing the
  fresh **`SYNCHRONIZER_TOKEN`** (CSRF) plus hidden fields like `ownerId` (your
  member id, e.g. `279515`) and `startTimeId`.
- `extract_csrf()` / `extract_field()` are just regexes that pull those values
  out of the raw HTML.

### Step 4 — Submit the booking (consumes all three credentials)

```python
# yourcourts.py — book_slot()
resp = session.post(
    f"{BASE_URL}/reservation/bookcourtreservation",
    data={
        "SYNCHRONIZER_TOKEN": csrf,                        # CSRF token (Step 3)
        "SYNCHRONIZER_URI": "/reservation/newreservation/",
        "reservationDate": date,
        "startTimeId": start_time_id,
        "facilityId": FACILITY_ID,                         # "1535"
        "ownerId": owner_id,                               # "279515"
        "reservableResourceId": slot["resource_id"],       # from the slot (Step 2)
        "reservationTypeId": RESERVATION_TYPE_ID,          # "1200"
        "duration": str(duration_min),
        # ... plus several empty/fixed fields the form requires
    },
    headers={"Origin": BASE_URL, "Referer": form_url},
    allow_redirects=False,
)
if resp.status_code == 302:
    return True, "Booked ..."
```

This single POST relies on **all three** credentials at once:
1. the **session cookie** (auto-attached by `requests.Session`) → proves you're
   logged in,
2. the **`SYNCHRONIZER_TOKEN`** in the body → proves the request isn't forged,
3. the slot identity (`reservableResourceId`, `startTimeId`) that came from the
   tokenized slot link → proves the slot is real.

The `Origin` and `Referer` headers are set to look like the request genuinely
came from the form page — another anti-CSRF signal the server may check.

**Success is a `302`.** On a successful booking the server redirects you (to the
schedule view); `allow_redirects=False` stops `requests` from following it so the
code can inspect the raw `302`. A non-302 means failure, and the code scrapes any
`alert` message out of the response HTML to report why.

---

## Part 3: Why you can't see the cookie in the HAR files

If you open the captured `.har` files looking for the cookie, **you won't find
it** — there are zero `Cookie` and zero `Set-Cookie` headers across every
request. This is **not** because cookies aren't used. It's because the HAR files
were exported with **sanitization on**, which strips `Cookie`, `Set-Cookie`, and
`Authorization` headers for privacy.

So how do we *know* it's still cookie-based? By elimination. Consider this real
authenticated request from the HAR:

```
GET /findAMatch/findPendingInvitationsCountForMember/279515   ->   200
request headers present: accept, referer, user-agent, x-requested-with, sec-* ...
                         (no Authorization, no token in the URL, no API key)
```

That endpoint returns data for **member 279515 specifically**, and it succeeds.
Yet the request carries:
- no `Authorization` header,
- no bearer token or API key in the query string,
- no credential of any kind that's visible.

A stateless HTTP request **cannot** be recognized as "logged-in member 279515"
unless something identifies the session. Every visible candidate is absent — so
the identifier must be the one header the export removed: the **session cookie**.
The booking POST tells the same story: no auth header, yet it books for
`ownerId=279515`.

**Takeaway:** "I don't see a cookie in the HAR" means "the export hid it," not
"there is no cookie." The cookie is real; you just can't see its value in a
sanitized capture.

---

## Part 4: Session expiry and refresh

A session cookie doesn't live forever — the server can expire it. The code
handles this two ways:

1. **Detection.** Any time a request gets redirected to a `showLogin` URL, the
   session is dead. `find_slots()` raises `SessionExpired` (a custom subclass of
   `requests.RequestException`) so callers can react.
2. **Proactive refresh.** The long-running headless snipe re-logs in
   periodically so the cookie never goes stale mid-poll:

   ```python
   # snipe_headless.py
   if attempt % 40 == 0:
       print("Refreshing session...")
       yourcourts.login(session)     # re-issues a fresh session cookie into the same jar
   ```

   Re-calling `login()` on the *same* `session` object just overwrites the old
   cookie in the jar with a fresh one — no other code has to change.

---

## Part 5: Glossary (quick reference)

- **Stateless** — each HTTP request stands alone; the server remembers nothing
  between requests on its own.
- **Header** — a key/value metadata line on a request or response.
- **Cookie** — a value the server tells the client to store (`Set-Cookie`) and
  resend on every later request (`Cookie`). Used here to carry the session id.
- **`Set-Cookie` / `Cookie`** — the response header that issues a cookie / the
  request header that returns it.
- **Session (server-side)** — the server's stored record of your logged-in state,
  looked up by the id inside your cookie. Here the cookie is `JSESSIONID`.
- **`requests.Session` (client-side)** — the Python object with a cookie jar that
  auto-stores and auto-resends cookies. Created by `make_session()`. Distinct
  from the server session despite the shared name.
- **CSRF (Cross-Site Request Forgery)** — an attack where another site triggers
  authenticated requests using your auto-sent cookie.
- **CSRF token / `SYNCHRONIZER_TOKEN`** — an unpredictable per-form value
  embedded in the form HTML that you must echo back in the POST body to prove the
  submission is genuine. Not a cookie. Signals a Grails/Spring Java app.
- **Per-slot `token`** — a query-string value attached to each open slot's link
  proving that slot was genuinely available. Not a cookie. Extracted by
  `find_slots()`.
- **302** — an HTTP redirect status. Here it's the **success** signal for both
  login and booking.
- **`ownerId` (279515)** — your member id, pulled from the form; identifies whose
  reservation this is.
- **`facilityId` (1535) / `reservationTypeId` (1200)** — fixed identifiers for
  this particular facility and reservation type.
- **HAR** — "HTTP Archive," a JSON capture of browser network traffic. The ones
  in this analysis were sanitized, so cookie headers are stripped.

---

## One-paragraph summary

YourCourts is a stateless Java (Grails/Spring) web app, so every request must
carry its own proof of identity. You obtain that proof once by POSTing your email
and password to `/security/login/form-login`; the server replies `302` and sets a
**session cookie** (`JSESSIONID`), which `requests.Session` stores and then
attaches automatically to every later request — that's "being logged in." To
actually book, you additionally need a fresh **`SYNCHRONIZER_TOKEN`** (a CSRF
token scraped from the reservation form page right before you submit) and the
**per-slot `token`** (extracted from the open-slot link on the schedule page).
The booking POST succeeds (`302`) only when all three are present and valid. None
of these are visible in the provided HAR files because the export stripped
cookie/auth headers — but the fact that member-scoped endpoints return `200` with
no other credential is itself the proof that a session cookie is doing the work.
