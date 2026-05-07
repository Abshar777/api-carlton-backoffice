"""
One-time script: tag 3,922 clients as "EDU CARLTON" in Carlton DB.
Emails not found in DB are SKIPPED (no ghost clients created).

Run:  python3 tag_edu_carlton_clients.py
"""

import asyncio
import re
from datetime import datetime, timezone

import openpyxl
from motor.motor_asyncio import AsyncIOMotorClient

# ── Config ────────────────────────────────────────────────────────────────────
MONGO_URL  = "mongodb://delta:123@31.97.237.248:27017"
DB_NAME    = "carlton_ac_db"
EXCEL      = "/Users/mhdabshar/Downloads/master manage carlton.xlsx"
TAG_NAME   = "EDU CARLTON"
BATCH_SIZE = 100


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_emails(path: str) -> list[str]:
    wb = openpyxl.load_workbook(path)
    ws = wb.active
    emails = []
    for row in ws.iter_rows(values_only=True):
        val = row[0]
        if val and isinstance(val, str) and "@" in val:
            emails.append(val.strip().lower())
    return list(dict.fromkeys(emails))


async def chunked_tx_update(db, client_ids: list[str], tag_name: str) -> int:
    total = 0
    for i in range(0, len(client_ids), BATCH_SIZE):
        chunk = client_ids[i : i + BATCH_SIZE]
        result = await db.transactions.update_many(
            {"client_id": {"$in": chunk}},
            {"$addToSet": {"client_tags": tag_name}}
        )
        total += result.modified_count
        print(f"  Batch {i // BATCH_SIZE + 1}: {len(chunk)} clients → {result.modified_count} transactions tagged")
    return total


async def main():
    mongo_client = AsyncIOMotorClient(MONGO_URL)
    db = mongo_client[DB_NAME]
    now = datetime.now(timezone.utc).isoformat()

    # ── Step 1: resolve tag ───────────────────────────────────────────────────
    tag_doc = await db.client_tags.find_one(
        {"name": {"$regex": f"^{re.escape(TAG_NAME)}$", "$options": "i"}}, {"_id": 0}
    )
    if tag_doc:
        tag_id   = tag_doc["tag_id"]
        tag_name = tag_doc["name"]
        print(f"✅ Found existing tag: '{tag_name}'  (id={tag_id})")
    else:
        print(f"❌ Tag '{TAG_NAME}' not found in Carlton DB. Aborting.")
        mongo_client.close()
        return

    # ── Step 2: load emails ───────────────────────────────────────────────────
    emails = load_emails(EXCEL)
    print(f"\n📋 Emails in Excel: {len(emails):,}")

    # ── Step 3: find existing clients (two-pass, case-insensitive) ────────────
    matched_docs = await db.clients.find(
        {"email": {"$in": emails}},
        {"_id": 0, "client_id": 1, "email": 1, "tags": 1}
    ).to_list(None)

    matched_lower = {c["email"].lower() for c in matched_docs}
    remaining = [e for e in emails if e not in matched_lower]
    if remaining:
        for i in range(0, len(remaining), 200):
            chunk = remaining[i : i + 200]
            extra = await db.clients.find(
                {"email": {"$regex": "|".join([f"^{re.escape(e)}$" for e in chunk]),
                           "$options": "i"}},
                {"_id": 0, "client_id": 1, "email": 1, "tags": 1}
            ).to_list(None)
            matched_docs.extend(extra)

    # Deduplicate by client_id
    seen_ids: set = set()
    existing_clients: list = []
    for c in matched_docs:
        if c["client_id"] not in seen_ids:
            seen_ids.add(c["client_id"])
            existing_clients.append(c)

    existing_emails_lower = {c["email"].lower() for c in existing_clients}
    missing_emails        = [e for e in emails if e not in existing_emails_lower]
    already_tagged        = sum(1 for c in existing_clients if tag_id in (c.get("tags") or []))

    print(f"✅ Existing clients matched:      {len(existing_clients):,}")
    print(f"🏷️  Already have '{tag_name}' tag: {already_tagged:,}")
    print(f"🔄 Will be newly tagged:           {len(existing_clients) - already_tagged:,}")
    print(f"⏭️  Emails not in DB (skipped):    {len(missing_emails)}")

    if missing_emails:
        print(f"\n📧 Skipped email(s):")
        for e in missing_emails:
            print(f"   • {e}")

    # ── Step 4: confirm ───────────────────────────────────────────────────────
    print()
    print("Plan:")
    print(f"  • Add '{tag_name}' tag to {len(existing_clients):,} existing clients")
    print(f"  • Backfill transactions in batches of {BATCH_SIZE}")
    print(f"  • No new records created — missing emails are skipped")
    print()
    ans = input("Proceed? (yes/no): ").strip().lower()
    if ans != "yes":
        print("Aborted.")
        mongo_client.close()
        return

    # ── Step 5: tag existing clients ──────────────────────────────────────────
    existing_ids = [c["client_id"] for c in existing_clients]
    clients_result = await db.clients.update_many(
        {"client_id": {"$in": existing_ids}},
        {"$addToSet": {"tags": tag_id}, "$set": {"updated_at": now}}
    )
    print(f"\n✅ Clients tagged: {clients_result.modified_count:,}")

    # ── Step 6: backfill transactions ─────────────────────────────────────────
    print(f"\n🔄 Updating transactions for {len(existing_ids):,} clients in batches of {BATCH_SIZE}...")
    total_tx = await chunked_tx_update(db, existing_ids, tag_name)

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("═" * 55)
    print("DONE")
    print(f"  Tag:                      {tag_name} ({tag_id})")
    print(f"  Emails in file:           {len(emails):,}")
    print(f"  Clients tagged:           {clients_result.modified_count:,}")
    print(f"  Skipped (not in DB):      {len(missing_emails)}")
    print(f"  Ghost clients created:    0")
    print(f"  Transactions updated:     {total_tx:,}")
    print("═" * 55)

    if missing_emails:
        print(f"\n⚠️  Skipped email(s) (not found in Carlton DB):")
        for e in missing_emails:
            print(f"   • {e}")

    mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())
