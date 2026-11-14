from __future__ import annotations

import asyncio
import os

from taiyiagent.worker.cli import run_worker


async def main() -> None:
    config_path = os.environ.get("TAIYIFLOW_WORKER_CONFIG", "worker.yml")
    await run_worker(config_path)


if __name__ == "__main__":
    asyncio.run(main())
