import subprocess
import sys


def run_module(module: str) -> None:
    result = subprocess.run([sys.executable, '-m', module, '--help'], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert 'usage' in result.stdout.lower()


def test_forward_test_help():
    run_module('src.models.forward_test')


def test_moneyline_dataset_help():
    run_module('src.features.moneyline_dataset')
