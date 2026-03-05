from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]


class DockerPackagingTests(unittest.TestCase):
    def test_dockerfile_declares_python_311_entrypoint_and_runtime_volume(self) -> None:
        dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("FROM python:3.11-slim", dockerfile)
        self.assertIn('ENTRYPOINT ["qtbot"]', dockerfile)
        self.assertIn('VOLUME ["/app/runtime"]', dockerfile)

    def test_compose_service_has_env_file_runtime_mount_and_budget_command(self) -> None:
        compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn("qtbot:", compose)
        self.assertIn("env_file:", compose)
        self.assertIn("- .env", compose)
        self.assertIn("- ./runtime:/app/runtime", compose)
        self.assertIn('command: ["start", "--budget", "${QTBOT_START_BUDGET_CAD:-1000}"]', compose)

    def test_env_example_includes_compose_start_budget_default(self) -> None:
        env_example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
        self.assertIn("QTBOT_START_BUDGET_CAD=1000", env_example)


if __name__ == "__main__":
    unittest.main()
