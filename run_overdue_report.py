import sys

from app import init_db, run_overdue_report


def main():
    init_db()
    status, error = run_overdue_report()
    if status == "success":
        print("Overdue report generated.")
        return 0
    print(f"Overdue report failed: {error}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
