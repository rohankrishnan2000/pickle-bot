#!/usr/bin/env python3
"""Minimal smoke test for the schedule parser against the live site."""

import sys
import os

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

import yourcourts

session = yourcourts.make_session()
if not yourcourts.login(session):
    print("Login failed")
    sys.exit(1)

slots = yourcourts.find_slots(session, "06/05/2026")
print(f"Found {len(slots)} slots:\n")
for s in slots:
    print(s)
