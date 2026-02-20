import sys
from datetime import date

from app import (
    init_db,
    get_db,
    get_invoice_for_run,
    is_due,
    run_for_recipient,
    get_grouped_customer_ids,
    load_invoice_df,
)


def main():
    init_db()
    try:
        invoice_file_id, invoice_path = get_invoice_for_run()
        invoice_df = load_invoice_df(invoice_path)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM recipients ORDER BY group_name ASC")
    recipients = cur.fetchall()
    conn.close()

    today = date.today()
    grouped_customer_ids = get_grouped_customer_ids()
    sent = 0
    skipped = 0
    failed = 0
    for recipient in recipients:
        if recipient["recipient_type"] == "single" and recipient["id"] in grouped_customer_ids:
            continue
        if is_due(recipient, today):
            try:
                status, _ = run_for_recipient(
                    recipient,
                    invoice_path,
                    invoice_file_id,
                    "scheduled",
                    preloaded_df=invoice_df,
                )
                if status == "sent":
                    sent += 1
                    print(f"Sent: {recipient['group_name']}")
                elif status == "skipped":
                    skipped += 1
                    print(f"Skipped: {recipient['group_name']}")
                else:
                    failed += 1
                    print(f"Failed: {recipient['group_name']}")
            except Exception as exc:
                failed += 1
                print(f"Failed: {recipient['group_name']} - {exc}")

    print(f"Done. Sent {sent}, skipped {skipped}, failed {failed}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
