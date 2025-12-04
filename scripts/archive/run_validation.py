"""Run data validation suite."""
import sys
import logging
from src.data.validation import run_validation_suite, print_report

logging.basicConfig(level=logging.INFO)

def main():
    results = run_validation_suite()
    print_report(results)
    
    # Exit with error if any FAIL
    if any(r.status == "FAIL" for r in results):
        sys.exit(1)

if __name__ == "__main__":
    main()
