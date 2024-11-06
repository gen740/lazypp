import select
import subprocess
import sys


def run_sh(
    command: list[str],
    env: dict[str, str] | None = None,
):
    # Popen を使って sys.stdout に出力する例
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
    ) as process:
        # 標準出力と標準エラーのリアルタイム処理
        while process.poll() is None:
            readable, _, _ = select.select([process.stdout, process.stderr], [], [])
            for stream in readable:
                line = stream.readline()
                if line:
                    output = sys.stdout if stream is process.stdout else sys.stderr
                    output.write(line)
                    output.flush()
